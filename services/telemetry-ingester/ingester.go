package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type EventScorePair struct {
	Event BotMetricsEvent
	Score float64
}

type Ingester struct {
	db                     *pgxpool.Pool
	redis                  *redis.Client
	maxAcceptableLatencyUS float64 // ceiling for the weighted p50/p90/p99 score
	maxAcceptableTPS       float64
	lifecycleCtx           context.Context

	mu          sync.Mutex
	eventBuffer []EventScorePair
	flushTicker *time.Ticker
	closeCh     chan struct{}
	doneCh      chan struct{}
}

func NewIngester(lifecycleCtx context.Context, dsn, redisAddr, redisPass string, maxLatencyUS, maxTPS float64) (*Ingester, error) {
	rdb, err := connectRedis(lifecycleCtx, redisAddr, redisPass)
	if err != nil {
		return nil, err
	}

	db, err := connectPostgres(lifecycleCtx, dsn)
	if err != nil {
		_ = rdb.Close()
		return nil, err
	}

	ingester := &Ingester{
		db:                     db,
		redis:                  rdb,
		maxAcceptableLatencyUS: maxLatencyUS,
		maxAcceptableTPS:       maxTPS,
		lifecycleCtx:           lifecycleCtx,
		eventBuffer:            make([]EventScorePair, 0, 1000),
		flushTicker:            time.NewTicker(500 * time.Millisecond),
		closeCh:                make(chan struct{}),
		doneCh:                 make(chan struct{}),
	}

	if ingester.maxAcceptableLatencyUS <= 0 {
		return nil, fmt.Errorf("MAX_LATENCY_US must be > 0, got %v", ingester.maxAcceptableLatencyUS)
	}

	go ingester.flushLoop()

	return ingester, nil
}

func connectRedis(ctx context.Context, addr, password string) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})

	backoff := time.Second
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return rdb, nil
		}
		if ctx.Err() != nil {
			_ = rdb.Close()
			return nil, ctx.Err()
		}
		slog.Warn("redis not ready, retrying", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			_ = rdb.Close()
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func connectPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	backoff := time.Second
	for {
		db, err := pgxpool.New(ctx, dsn)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			slog.Warn("postgres pool create failed, retrying", "err", err, "backoff", backoff)
		} else {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = db.Ping(pingCtx)
			cancel()
			if err == nil {
				return db, nil
			}
			db.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			slog.Warn("postgres not ready, retrying", "err", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (i *Ingester) flushLoop() {
	for {
		select {
		case <-i.flushTicker.C:
			flushCtx, flushCancel := context.WithTimeout(i.lifecycleCtx, 10*time.Second)
			i.flush(flushCtx)
			flushCancel()
		case <-i.closeCh:
			i.flushTicker.Stop()
			flushCtx, flushCancel := context.WithTimeout(i.lifecycleCtx, 10*time.Second)
			i.flush(flushCtx)
			flushCancel()
			close(i.doneCh)
			return
		}
	}
}

func (i *Ingester) flush(ctx context.Context) {
	i.mu.Lock()
	if len(i.eventBuffer) == 0 {
		i.mu.Unlock()
		return
	}
	bufferToFlush := make([]EventScorePair, len(i.eventBuffer))
	copy(bufferToFlush, i.eventBuffer)
	i.eventBuffer = i.eventBuffer[:0]
	i.mu.Unlock()

	backoff := time.Second
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				slog.Error("flush cancelled during retry", "error", ctx.Err())
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
		if err := i.writeBatch(ctx, bufferToFlush); err != nil {
			slog.Error("flush batch error", "error", err, "attempt", attempt+1)
			continue
		}
		return
	}
	slog.Error("flush batch failed after 3 attempts", "dropped_events", len(bufferToFlush))
}

func (i *Ingester) writeBatch(ctx context.Context, pairs []EventScorePair) error {
	batch := &pgx.Batch{}

	for _, p := range pairs {
		batch.Queue(`
			INSERT INTO telemetry_events
				(time, submission_id, bot_id, event_type, latency_us, order_id)
			VALUES
				($1, $2, $3, $4, $5, NULL)
		`, time.Unix(0, p.Event.EmittedAt), p.Event.SubmissionID, p.Event.TestRunID, "bot.metrics", p.Event.AckP90US)

		batch.Queue(`
			INSERT INTO submission_scores
				(submission_id, team_name, p50_us, p90_us, p99_us, tps, correctness, composite, scored_at)
			VALUES
				($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (submission_id) DO UPDATE SET
				p50_us      = EXCLUDED.p50_us,
				p90_us      = EXCLUDED.p90_us,
				p99_us      = EXCLUDED.p99_us,
				tps         = EXCLUDED.tps,
				correctness = EXCLUDED.correctness,
				composite   = EXCLUDED.composite,
				scored_at   = EXCLUDED.scored_at
		`, p.Event.SubmissionID, p.Event.TeamName, p.Event.AckP50US, p.Event.AckP90US, p.Event.AckP99US, p.Event.TPS,
			p.Event.CorrectnessScore, p.Score, time.Now())
	}

	br := i.db.SendBatch(ctx, batch)
	defer func() {
		if err := br.Close(); err != nil {
			slog.Error("batch result close error", "error", err)
		}
	}()

	var errs []error
	for j := 0; j < len(pairs)*2; j++ {
		_, err := br.Exec()
		if err != nil {
			errs = append(errs, fmt.Errorf("batch exec error at index %d: %w", j, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d batch exec errors: %v", len(errs), errs[0])
	}

	return nil
}

func (i *Ingester) Handle(ctx context.Context, event BotMetricsEvent) {
	score := i.computeScore(event)

	i.mu.Lock()
	i.eventBuffer = append(i.eventBuffer, EventScorePair{Event: event, Score: score})
	i.mu.Unlock()

	if err := i.updateLeaderboard(ctx, event, score); err != nil {
		slog.Error("update leaderboard", "error", err)
	}

	slog.Info("ingested",
		"submission", event.SubmissionID,
		"score", score,
		"tps", event.TPS,
		"ack_p90", event.AckP90US,
	)
}

func (i *Ingester) computeScore(event BotMetricsEvent) float64 {
	// Latency score: weighted combination of p50/p90/p99 acknowledgment latencies.
	// p99 carries the most weight (0.5) because tail latency is the primary
	// discriminator between submissions under load.
	weightedLatencyUS := float64(event.AckP50US)*0.2 +
		float64(event.AckP90US)*0.3 +
		float64(event.AckP99US)*0.5
	latencyScore := 1.0 - (weightedLatencyUS / i.maxAcceptableLatencyUS)
	latencyScore = math.Max(0, math.Min(1, latencyScore))

	// Throughput score: sustained TPS relative to the platform ceiling.
	throughputScore := event.TPS / i.maxAcceptableTPS
	throughputScore = math.Max(0, math.Min(1, throughputScore))

	// Correctness score: validated orderbook integrity from GET /orderbook.
	// Sent by the bot-runner after the quiet-period snapshot.
	correctnessScore := math.Max(0, math.Min(1, event.CorrectnessScore))

	// Composite: Speed 35% | Throughput 35% | Correctness 30%
	return (latencyScore * 0.35) + (throughputScore * 0.35) + (correctnessScore * 0.30)
}

// leaderboardEntry is the enriched payload written to both the pub/sub channel
// and the leaderboard_details hash. It carries enough data for the frontend to
// render both the summary row and the expanded metrics panel without hitting
// TimescaleDB.
type leaderboardEntry struct {
	SubmissionID string  `json:"submission_id"`
	TeamName     string  `json:"team_name"`
	Score        float64 `json:"score"`
	TPS          float64 `json:"tps"`
	AckP50US     int64   `json:"ack_p50_us"`
	AckP90US     int64   `json:"ack_p90_us"`
	AckP99US     int64   `json:"ack_p99_us"`
	OrdersSent   int64   `json:"orders_sent"`
	RejectsRecv  int64   `json:"rejects_recv"`
	Correctness  float64 `json:"correctness"`
	DurationMS   int64   `json:"duration_ms"`
	Timestamp    string  `json:"timestamp"`
}

var updateLeaderboardScript = redis.NewScript(`
	redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
	redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
	return redis.call('PUBLISH', KEYS[3], ARGV[3])
`)

func (i *Ingester) updateLeaderboard(ctx context.Context, event BotMetricsEvent, score float64) error {
	correctness := math.Max(0, math.Min(1, event.CorrectnessScore))

	entry := leaderboardEntry{
		SubmissionID: event.SubmissionID,
		TeamName:     event.TeamName,
		Score:        score,
		TPS:          event.TPS,
		AckP50US:     event.AckP50US,
		AckP90US:     event.AckP90US,
		AckP99US:     event.AckP99US,
		OrdersSent:   event.OrdersSent,
		RejectsRecv:  event.RejectsRecv,
		Correctness:  correctness,
		DurationMS:   event.DurationMS,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal leaderboard entry: %w", err)
	}

	// score*1000 avoids Redis float collation issues in sorted sets
	return updateLeaderboardScript.Run(ctx, i.redis,
		[]string{"leaderboard", "leaderboard_details", "leaderboard_updates"},
		score*1000, event.SubmissionID, payload,
	).Err()
}

func (i *Ingester) Close() {
	close(i.closeCh)
	<-i.doneCh
	i.db.Close()
	if err := i.redis.Close(); err != nil {
		slog.Error("redis close error", "error", err)
	}
}
