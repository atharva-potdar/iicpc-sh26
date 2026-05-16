package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
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

func main() {
	endpoint := envStr("TARGET_ENDPOINT", "ws://localhost:8080/stream")
	numBots := envInt("NUM_BOTS", 10)
	durationSec := envInt("DURATION_SECONDS", 30)
	teamName := envStr("TEAM_NAME", "unknown")
	submissionID := envStr("TEST_RUN_ID", "")
	testRunID := submissionID
	rawBrokers := envStr("REDPANDA_BROKERS", "")
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
			slog.Error("failed to create kafka client", "error", err)
			os.Exit(1)
		}
		defer client.Close()
	}

	// ── Phase 1: Correctness (synchronous, 1 bot, ~5-10s) ──────────────
	slog.Info("phase 1: correctness validation", "endpoint", endpoint)
	cb := NewCorrectnessBot(endpoint)
	correctnessResult := cb.Run(context.Background())
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

	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()

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
			b.Run(ctx, duration, readyCh)
		}(bot)
	}

	slog.Info("waiting for all bots to warm up")
	for i := 0; i < numBots; i++ {
		select {
		case <-readyCh:
		case <-ctx.Done():
			return
		}
	}
	slog.Info("all bots warmed up, starting measurement")
	start := time.Now()

	wg.Wait()
	elapsed := time.Since(start)

	metrics := make([]*BotMetrics, numBots)
	for i, b := range bots {
		metrics[i] = b.metrics
	}
	agg := merge(metrics)
	report(agg, elapsed)

	slog.Info("attempting to publish metrics", "submission", submissionID)
	if submissionID != "" {
		if err := publishMetrics(client, agg, elapsed, teamName, submissionID, testRunID, correctnessResult.Score); err != nil {
			slog.Error("failed to publish metrics", "error", err)
		} else {
			slog.Info("metrics published to Redpanda")
		}
	}

	if client != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := client.Flush(flushCtx); err != nil {
			slog.Error("kafka flush error", "error", err)
		}
		flushCancel()
	}
	slog.Info("Closing bot runner")
}
