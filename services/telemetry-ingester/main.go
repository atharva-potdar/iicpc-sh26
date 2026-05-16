package main

import (
	"context"
	"fmt"
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

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func run() error {
	brokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	dsn := envStr("TIMESCALEDB_DSN", "postgres://postgres:obarena@timescaledb.platform.svc.cluster.local:5432/obarena")
	redisAddr := envStr("REDIS_ADDR", "redis.platform.svc.cluster.local:6379")
	redisPass := envStr("REDIS_PASSWORD", "")

	maxLatencyUS := envFloat("MAX_LATENCY_US", 50000.0)
	maxTPS := envFloat("MAX_TPS", 1000.0)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ingester, err := NewIngester(ctx, dsn, redisAddr, redisPass, maxLatencyUS, maxTPS)
	if err != nil {
		return fmt.Errorf("init ingester: %v", err)
	}
	defer ingester.Close()

	consumer, err := NewConsumer(brokers)
	if err != nil {
		return fmt.Errorf("init consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("telemetry-ingester started")

	consumer.Run(ctx, ingester.Handle)

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
