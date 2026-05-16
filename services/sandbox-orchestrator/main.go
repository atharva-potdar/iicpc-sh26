package main

import (
	"context"
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

func main() {
	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	redpandaBrokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	
	cfg := SandboxConfig{
		Timeout:       time.Duration(envInt("SANDBOX_TIMEOUT_SECONDS", 60)) * time.Second,
		MaxLogBytes:   envInt("MAX_LOG_BYTES", 4096),
		CpuRequest:    envStr("SANDBOX_CPU_REQUEST", "2"),
		CpuLimit:      envStr("SANDBOX_CPU_LIMIT", "2"),
		MemoryRequest: envStr("SANDBOX_MEMORY_REQUEST", "512Mi"),
		MemoryLimit:   envStr("SANDBOX_MEMORY_LIMIT", "512Mi"),
		SeccompProfile: envStr("SANDBOX_SECCOMP_PROFILE_PATH", "sandbox-seccomp.json"),
		RunAsUser:     int64(envInt("SANDBOX_RUN_AS_USER", 65534)),
		NodeSelectorK: envStr("SANDBOX_NODE_SELECTOR_KEY", "workload"),
		NodeSelectorV: envStr("SANDBOX_NODE_SELECTOR_VALUE", "sandbox"),
		TolerationK:   envStr("SANDBOX_TOLERATION_KEY", "workload"),
		TolerationV:   envStr("SANDBOX_TOLERATION_VALUE", "sandbox"),
	}

	publisher, err := NewPublisher(redpandaBrokers)
	if err != nil {
		slog.Error("init publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	orchestrator, err := NewOrchestrator(seaweedfsEndpoint, cfg)
	if err != nil {
		slog.Error("init orchestrator", "error", err)
		os.Exit(1)
	}
	defer orchestrator.Close()

	consumer, err := NewConsumer(redpandaBrokers, orchestrator, publisher)
	if err != nil {
		slog.Error("init consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("sandbox-orchestrator starting")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("consumer", "error", err)
		os.Exit(1)
	}
	slog.Info("sandbox-orchestrator stopped")
}
