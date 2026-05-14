package main

import (
	"context"
	"log"
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
	brokers := strings.Split(envStr("REDPANDA_BROKERS", ""), ",")

	duration := time.Duration(durationSec) * time.Second

	var client *kgo.Client
	if len(brokers) > 0 && brokers[0] != "" {
		var err error
		client, err = kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			log.Fatalf("failed to create kafka client: %v", err)
		}
		defer client.Close()
	}

	log.Printf("starting %d bots | duration=%s | target=%s | submission=%s",
		numBots, duration, endpoint, submissionID)

	// ctx is cancelled after correctness validation to cleanly close bot connections.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// quietChs receive a signal when each bot's write loop finishes.
	// Connections are held open until cancel() is called.
	quietChs := make([]chan struct{}, numBots)
	for i := range quietChs {
		quietChs[i] = make(chan struct{}, 1)
	}

	bots := make([]*Bot, numBots)
	for i := range bots {
		seqs := sequences(i)
		seq := seqs[i%len(seqs)]
		bots[i] = NewBot(i, endpoint, seq)
	}

	// Wait for all bots to connect before starting the clock.
	readyCh := make(chan struct{}, numBots)
	var wg sync.WaitGroup
	for i, bot := range bots {
		wg.Add(1)
		go func(b *Bot, q chan struct{}) {
			defer wg.Done()
			b.Run(ctx, duration, readyCh, q)
		}(bot, quietChs[i])
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

	// Wait for all bots to finish writing. Connections remain open.
	for _, q := range quietChs {
		select {
		case <-q:
		case <-ctx.Done():
			return
		}
	}
	elapsed := time.Since(start)
	log.Printf("all bots done writing (elapsed=%s), running correctness validation", elapsed.Round(time.Millisecond))

	// Correctness validation: query GET /orderbook while connections are live
	// so resting orders are still present in the contestant's book.
	httpBase := strings.Replace(endpoint, "ws://", "http://", 1)
	httpBase = strings.TrimSuffix(httpBase, "/stream")
	expected := ComputeExpected(numBots)
	valResult, valErr := ValidateOrderbook(httpBase+"/orderbook", expected)
	if valErr != nil {
		log.Printf("correctness validation error: %v", valErr)
		valResult = ValidationResult{CorrectnessScore: 0.0}
	}
	log.Printf("correctness: score=%.4f bids(exp=%d got=%d) asks(exp=%d got=%d)",
		valResult.CorrectnessScore,
		valResult.ExpectedBids, valResult.ActualBids,
		valResult.ExpectedAsks, valResult.ActualAsks)

	// Cancel context: bots unblock from <-ctx.Done() and close connections.
	cancel()
	wg.Wait()

	metrics := make([]*BotMetrics, numBots)
	for i, b := range bots {
		metrics[i] = b.metrics
	}
	agg := merge(metrics)
	report(agg, elapsed)

	log.Printf("attempting to publish metrics: submission=%s brokers=%v", submissionID, brokers)
	if submissionID != "" {
		if err := publishMetrics(client, agg, elapsed, teamName, submissionID, testRunID, valResult.CorrectnessScore); err != nil {
			log.Printf("failed to publish metrics: %v", err)
		} else {
			log.Printf("metrics published to Redpanda")
		}
	}

	if client != nil {
		client.Flush(context.Background())
	}
	log.Printf("Closing bot runner")
}

