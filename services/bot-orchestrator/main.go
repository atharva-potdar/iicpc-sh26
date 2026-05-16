package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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

type Server struct {
	ctx          context.Context
	orchestrator *Orchestrator
	mu           sync.Mutex
	isRunning    bool
}

func (s *Server) runHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		http.Error(w, "Test already in progress", http.StatusConflict)
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	var event SandboxReadyEvent
	if r.Body != nil {
		defer func() {
			if err := r.Body.Close(); err != nil {
				slog.Error("runHandler body close error", "error", err)
			}
		}()
		_ = json.NewDecoder(r.Body).Decode(&event)
	}

	// Default fallback if no body provided
	if event.SubmissionID == "" {
		event.SubmissionID = "manual-run"
	}
	if event.PodIP == "" {
		event.PodIP = "submission-api.platform.svc.cluster.local"
	}
	if event.WSPort == 0 {
		event.WSPort = 8080
	}
	if event.TeamName == "" {
		event.TeamName = "manual-team"
	}

	go func() {
		defer func() {
			s.mu.Lock()
			s.isRunning = false
			s.mu.Unlock()
		}()
		s.orchestrator.Handle(s.ctx, event)
	}()

	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte(`{"status": "started"}`)); err != nil {
		slog.Error("runHandler write error", "error", err)
	}
}

func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	running := s.isRunning
	s.mu.Unlock()

	status := "idle"
	if running {
		status = "running"
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": status}); err != nil {
		slog.Error("statusHandler encode error", "error", err)
	}
}

func main() {
	rawBrokers := envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092")
	var brokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	numBots := envInt("NUM_BOTS", 50)
	durationSec := envInt("DURATION_SECONDS", 60)
	jobTimeoutSec := envInt("JOB_TIMEOUT_SECONDS", 120)
	warmupSec := envInt("WARMUP_SECONDS", 15)
	sandboxNamespace := envStr("SANDBOX_NAMESPACE", "sandboxes")
	botRunnerImage := envStr("BOT_RUNNER_IMAGE", "bot-runner:dev")

	publisher, err := NewPublisher(brokers)
	if err != nil {
		slog.Error("init publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	cfg := Config{
		NumBots:          numBots,
		DurationSeconds:  durationSec,
		JobTimeoutSec:    jobTimeoutSec,
		WarmupSeconds:    warmupSec,
		RedpandaBrokers:  strings.Join(brokers, ","),
		BotRunnerImage:   botRunnerImage,
		SandboxNamespace: sandboxNamespace,
	}

	orchestrator, err := NewOrchestrator(publisher, cfg)
	if err != nil {
		slog.Error("init orchestrator", "error", err)
		os.Exit(1)
	}

	consumer, err := NewConsumer(brokers)
	if err != nil {
		slog.Error("init consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	slog.Info("bot-orchestrator started",
		"numBots", numBots,
		"duration", durationSec,
		"jobTimeout", jobTimeoutSec,
		"warmup", warmupSec,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	srv := &Server{ctx: ctx, orchestrator: orchestrator}
	http.HandleFunc("/run", srv.runHandler)
	http.HandleFunc("/status", srv.statusHandler)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			slog.Error("healthz write error", "error", err)
		}
	})

	httpServer := &http.Server{Addr: ":8080"}

	// Run HTTP server
	go func() {
		slog.Info("HTTP server listening on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	// Use consumer as well, forwarding to the same orchestrator (guarded by state if needed)
	go func() {
		consumer.Run(ctx, func(c context.Context, e SandboxReadyEvent) {
			srv.mu.Lock()
			if srv.isRunning {
				srv.mu.Unlock()
				slog.Info("ignoring sandbox.ready, test already running", "submission", e.SubmissionID)
				return
			}
			srv.isRunning = true
			srv.mu.Unlock()

			defer func() {
				srv.mu.Lock()
				srv.isRunning = false
				srv.mu.Unlock()
			}()

			orchestrator.Handle(c, e)
		})
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server shutdown error", "error", err)
	}
}
