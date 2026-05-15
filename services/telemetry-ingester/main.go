package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
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

func main() {
	brokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	dsn := envStr("TIMESCALEDB_DSN", "postgres://postgres:iicpc@timescaledb.platform.svc.cluster.local:5432/iicpc")
	redisAddr := envStr("REDIS_ADDR", "redis.platform.svc.cluster.local:6379")
	redisPass := envStr("REDIS_PASSWORD", "")

	ingester, err := NewIngester(dsn, redisAddr, redisPass, maxLatencyUS, maxTPS)
	if err != nil {
		log.Fatalf("init ingester: %v", err)
	}
	defer ingester.Close()

	consumer, err := NewConsumer(brokers)
	if err != nil {
		log.Fatalf("init consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("telemetry-ingester started")

	ctx := context.Background()
	consumer.Run(ctx, ingester.Handle)
}
