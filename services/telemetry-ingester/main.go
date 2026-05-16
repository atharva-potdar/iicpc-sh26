package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func run() error {
	rawBrokers := envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092")
	var brokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
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

	slog.Info("telemetry-ingester started")

	// Health check endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			slog.Error("healthz write error", "error", err)
		}
	})
	healthSrv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("healthz server", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("healthz shutdown error", "error", err)
		}
	}()

	consumer.Run(ctx, ingester.Handle)

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
