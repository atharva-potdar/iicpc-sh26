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
	db                   *pgxpool.Pool
	redis                *redis.Client
	maxAcceptableLatencyUS float64 // ceiling for the weighted p50/p90/p99 score
	maxAcceptableTPS     float64
	lifecycleCtx          context.Context

	mu          sync.Mutex
	eventBuffer []EventScorePair
	flushTicker *time.Ticker
	closeCh     chan struct{}
}

func NewIngester(lifecycleCtx context.Context, dsn, redisAddr, redisPass string, maxLatencyUS, maxTPS float64) (*Ingester, error) {
	ctx := context.Background()

	// Connect Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
	})

	pingCtx, pingCancel := context.WithTimeout(lifecycleCtx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	pingCancel()

	// Connect PostgreSQL
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	ingester := &Ingester{
		db:                   db,
		redis:                rdb,
		maxAcceptableLatencyUS: maxLatencyUS,
		maxAcceptableTPS:     maxTPS,
		lifecycleCtx:          lifecycleCtx,
		eventBuffer:          make([]EventScorePair, 0, 1000),
		flushTicker:          time.NewTicker(500 * time.Millisecond),
		closeCh:              make(chan struct{}),
	}

	go ingester.flushLoop()

	return ingester, nil
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

	if err := i.writeBatch(ctx, bufferToFlush); err != nil {
		slog.Error("flush batch error", "error", err)
	}
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

func (i *Ingester) updateLeaderboard(ctx context.Context, event BotMetricsEvent, score float64) error {
	member := fmt.Sprintf("%s:%s", event.SubmissionID, event.TeamName)
	err := i.redis.ZAdd(ctx, "leaderboard", redis.Z{
		Score:  score * 1000,
		Member: member,
	}).Err()
	if err != nil {
		return err
	}

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

	// Store in hash for HTTP initial-load; key is submission_id so we can HGET
	// individual entries after a ZREVRANGE scan.
	if err := i.redis.HSet(ctx, "leaderboard_details", event.SubmissionID, payload).Err(); err != nil {
		return fmt.Errorf("hset leaderboard_details: %w", err)
	}

	return i.redis.Publish(ctx, "leaderboard_updates", payload).Err()
}

func (i *Ingester) Close() {
	close(i.closeCh)
	time.Sleep(100 * time.Millisecond) // wait for flushLoop to complete final flush
	i.db.Close()
	if err := i.redis.Close(); err != nil {
		slog.Error("redis close error", "error", err)
	}
}
