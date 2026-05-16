package main

import (
	"context"
	"fmt"
	"log/slog"
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

func run() error {
	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	rawBrokers := envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092")
	var redpandaBrokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			redpandaBrokers = append(redpandaBrokers, trimmed)
		}
	}
	topic := envStr("KAFKA_TOPIC", "submission.lifecycle")

	cfg := SandboxConfig{
		Timeout:        time.Duration(envInt("SANDBOX_TIMEOUT_SECONDS", 60)) * time.Second,
		MaxLogBytes:    envInt("MAX_LOG_BYTES", 4096),
		CpuRequest:     envStr("SANDBOX_CPU_REQUEST", "2"),
		CpuLimit:       envStr("SANDBOX_CPU_LIMIT", "2"),
		MemoryRequest:  envStr("SANDBOX_MEMORY_REQUEST", "512Mi"),
		MemoryLimit:    envStr("SANDBOX_MEMORY_LIMIT", "512Mi"),
		SeccompProfile: envStr("SANDBOX_SECCOMP_PROFILE_PATH", "sandbox-seccomp.json"),
		RunAsUser:      int64(envInt("SANDBOX_RUN_AS_USER", 65534)),
		NodeSelectorK:  envStr("SANDBOX_NODE_SELECTOR_KEY", "workload"),
		NodeSelectorV:  envStr("SANDBOX_NODE_SELECTOR_VALUE", "sandbox"),
		TolerationK:    envStr("SANDBOX_TOLERATION_KEY", "workload"),
		TolerationV:    envStr("SANDBOX_TOLERATION_VALUE", "sandbox"),
	}

	publisher, err := NewPublisher(redpandaBrokers, topic)
	if err != nil {
		return fmt.Errorf("init publisher: %w", err)
	}
	defer publisher.Close()

	orchestrator, err := NewOrchestrator(seaweedfsEndpoint, cfg)
	if err != nil {
		return fmt.Errorf("init orchestrator: %w", err)
	}
	defer orchestrator.Close()

	consumer, err := NewConsumer(redpandaBrokers, topic, orchestrator, publisher)
	if err != nil {
		return fmt.Errorf("init consumer: %w", err)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("sandbox-orchestrator starting")
	if err := consumer.Run(ctx); err != nil {
		if ctx.Err() == nil {
			return fmt.Errorf("consumer: %w", err)
		}
	}
	slog.Info("sandbox-orchestrator stopped")

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
