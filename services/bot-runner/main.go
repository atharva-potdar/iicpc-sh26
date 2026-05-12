package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	brokers := strings.Split(envStr("REDPANDA_BROKERS", ""), ",")

	duration := time.Duration(durationSec) * time.Second

	log.Printf("starting %d bots | duration=%s | target=%s | submission=%s",
		numBots, duration, endpoint, submissionID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bots := make([]*Bot, numBots)
	for i := range bots {
		seqs := sequences(i)
		seq := seqs[i%len(seqs)]
		bots[i] = NewBot(i, endpoint, seq)
	}

	// Wait for all bots to complete warmup before starting the clock
	readyCh := make(chan struct{}, numBots)
	var wg sync.WaitGroup
	for _, bot := range bots {
		wg.Add(1)
		go func(b *Bot) {
			defer wg.Done()
			b.Run(ctx, duration, readyCh)
		}(bot)
	}

	log.Printf("waiting for all bots to warm up...")
	for i := 0; i < numBots; i++ {
		select {
		case <-readyCh:
		case <-ctx.Done():
			return
		}
	}
	log.Printf("all bots warmed up, starting measurement")
	start := time.Now()

	wg.Wait()
	elapsed := time.Since(start)

	metrics := make([]*BotMetrics, numBots)
	for i, b := range bots {
		metrics[i] = b.metrics
	}
	agg := merge(metrics)
	report(agg, elapsed)

	log.Printf("attempting to publish metrics: submission=%s brokers=%v", submissionID, brokers)
	if submissionID != "" {
		if err := publishMetrics(brokers, agg, elapsed, teamName, submissionID, testRunID); err != nil {
			log.Printf("failed to publish metrics: %v", err)
		} else {
			log.Printf("metrics published to Redpanda")
		}
	}
	log.Printf("Closing bot runner")
}
