package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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

func main() {
	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	redpandaBrokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	buildTimeout := envInt("BUILD_TIMEOUT_SECONDS", 120)
	maxLogBytes := envInt("MAX_LOG_BYTES", 4096)

	publisher, err := NewPublisher(redpandaBrokers)
	if err != nil {
		log.Fatalf("init publisher: %v", err)
	}
	defer publisher.Close()

	builder, err := NewBuilder(seaweedfsEndpoint, buildTimeout, maxLogBytes)
	if err != nil {
		log.Fatalf("init builder: %v", err)
	}

	consumer, err := NewConsumer(redpandaBrokers, builder, publisher)
	if err != nil {
		log.Fatalf("init consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("build-service starting")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer: %v", err)
	}
	log.Println("build-service stopped")
}
