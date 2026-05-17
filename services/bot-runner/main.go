package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	endpoint := envStr("TARGET_ENDPOINT", "ws://localhost:8080/stream")
	numBots := envInt("NUM_BOTS", 10)
	durationSec := envInt("DURATION_SECONDS", 30)
	teamName := envStr("TEAM_NAME", "unknown")
	submissionID := envStr("TEST_RUN_ID", "")
	testRunID := submissionID
	rawBrokers := envStr("REDPANDA_BROKERS", "")
	topic := envStr("KAFKA_TOPIC", "bot.metrics")
	var brokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}

	duration := time.Duration(durationSec) * time.Second

	var client *kgo.Client
	if len(brokers) > 0 && brokers[0] != "" {
		var err error
		client, err = kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			return fmt.Errorf("failed to create kafka client: %w", err)
		}
		defer client.Close()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Phase 1: Correctness (synchronous, 1 bot, ~5-10s) ──────────────
	slog.Info("phase 1: correctness validation", "endpoint", endpoint)
	cb := NewCorrectnessBot(endpoint)
	cbCtx, cbCancel := context.WithTimeout(ctx, 30*time.Second)
	defer cbCancel()
	correctnessResult := cb.Run(cbCtx)
	slog.Info("correctness complete",
		"score", correctnessResult.Score,
		"passed", correctnessResult.Passed,
		"total", correctnessResult.TotalAssertions,
	)

	// ── Phase 2: Load test (async, N bots, duration) ────────────────────
	slog.Info("phase 2: load test",
		"bots", numBots,
		"duration", duration,
		"endpoint", endpoint,
	)

	testCtx, testCancel := context.WithTimeout(ctx, duration+30*time.Second)
	defer testCancel()

	bots := make([]*Bot, numBots)
	for i := range bots {
		seqs := sequences(i)
		seq := seqs[i%len(seqs)]
		bots[i] = NewBot(i, endpoint, seq)
	}

	readyCh := make(chan struct{}, numBots)
	var wg sync.WaitGroup
	for _, bot := range bots {
		wg.Add(1)
		go func(b *Bot) {
			defer wg.Done()
			b.Run(testCtx, duration, readyCh)
		}(bot)
	}

	slog.Info("waiting for bots to warm up (quorum or timeout)")
	warmupTimeout := time.After(15 * time.Second)
	quorum := int(float64(numBots) * 0.8) // 80% quorum
	readyCount := 0

warmupLoop:
	for i := 0; i < numBots; i++ {
		select {
		case <-readyCh:
			readyCount++
			if readyCount >= quorum {
				slog.Info("quorum reached", "ready", readyCount)
				break warmupLoop
			}
		case <-warmupTimeout:
			slog.Warn("warmup timeout reached", "ready", readyCount)
			break warmupLoop
		case <-testCtx.Done():
			break warmupLoop
		}
	}
	slog.Info("starting measurement", "readyCount", readyCount)
	start := time.Now()

	wg.Wait()

	if testCtx.Err() != nil {
		return testCtx.Err()
	}
	elapsed := time.Since(start)

	metrics := make([]*BotMetrics, numBots)
	for i, b := range bots {
		metrics[i] = b.metrics
	}
	agg := merge(metrics)
	report(agg, elapsed)

	slog.Info("attempting to publish metrics", "submission", submissionID)
	if submissionID != "" {
		if err := publishMetrics(client, topic, agg, elapsed, teamName, submissionID, testRunID, correctnessResult.Score); err != nil {
			slog.Error("failed to publish metrics", "err", err)
		} else {
			slog.Info("metrics published to Redpanda")
		}
	}

	if client != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer flushCancel()
		if err := client.Flush(flushCtx); err != nil {
			slog.Error("kafka flush error", "err", err)
		}
	}
	slog.Info("Closing bot runner")
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
