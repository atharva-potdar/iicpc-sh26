package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func main() {
	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	redpandaBrokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	sandboxTimeout := envInt("SANDBOX_TIMEOUT_SECONDS", 60)
	maxLogBytes := envInt("MAX_LOG_BYTES", 4096)
	healthInterval := envDuration("HEALTH_CHECK_INTERVAL", 2*time.Second)
	healthRetries := envInt("HEALTH_CHECK_RETRIES", 15)

	publisher, err := NewPublisher(redpandaBrokers)
	if err != nil {
		log.Fatalf("init publisher: %v", err)
	}
	defer publisher.Close()

	orchestrator, err := NewOrchestrator(seaweedfsEndpoint, sandboxTimeout, maxLogBytes, healthInterval, healthRetries)
	if err != nil {
		log.Fatalf("init orchestrator: %v", err)
	}

	consumer, err := NewConsumer(redpandaBrokers, orchestrator, publisher)
	if err != nil {
		log.Fatalf("init consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("sandbox-orchestrator starting")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer: %v", err)
	}
	log.Println("sandbox-orchestrator stopped")
}
