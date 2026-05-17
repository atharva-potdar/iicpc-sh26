package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	activeTests  map[string]bool
}

func (s *Server) runHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event SandboxReadyEvent
	if r.Body != nil {
		defer func() {
			if err := r.Body.Close(); err != nil {
				slog.Error("runHandler body close error", "err", err)
			}
		}()
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			slog.Error("runHandler decode error", "err", err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
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

	s.mu.Lock()
	if s.activeTests[event.SubmissionID] {
		s.mu.Unlock()
		http.Error(w, "Test already in progress for this submission", http.StatusConflict)
		return
	}
	s.activeTests[event.SubmissionID] = true
	s.mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in runHandler goroutine", "error", r)
			}
			s.mu.Lock()
			delete(s.activeTests, event.SubmissionID)
			s.mu.Unlock()
		}()
		s.orchestrator.Handle(s.ctx, event)
	}()

	w.WriteHeader(http.StatusAccepted)
	if _, err := w.Write([]byte(`{"status": "started"}`)); err != nil {
		slog.Error("runHandler write error", "err", err)
	}
}

func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	running := len(s.activeTests) > 0
	s.mu.Unlock()

	status := "idle"
	if running {
		status = "running"
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": status}); err != nil {
		slog.Error("statusHandler encode error", "err", err)
	}
}

func run() error {
	rawBrokers := envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092")
	var brokers []string
	for _, b := range strings.Split(rawBrokers, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	topic := envStr("KAFKA_TOPIC", "submission.lifecycle")
	numBots := envInt("NUM_BOTS", 50)
	durationSec := envInt("DURATION_SECONDS", 60)
	jobTimeoutSec := envInt("JOB_TIMEOUT_SECONDS", 120)
	warmupSec := envInt("WARMUP_SECONDS", 15)
	sandboxNamespace := envStr("SANDBOX_NAMESPACE", "sandboxes")
	botRunnerImage := envStr("BOT_RUNNER_IMAGE", "bot-runner:dev")

	publisher, err := NewPublisher(brokers, topic)
	if err != nil {
		return fmt.Errorf("init publisher: %w", err)
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
		return fmt.Errorf("init orchestrator: %w", err)
	}

	consumer, err := NewConsumer(brokers, topic)
	if err != nil {
		return fmt.Errorf("init consumer: %w", err)
	}
	defer consumer.Close()

	slog.Info("bot-orchestrator starting",
		"numBots", numBots,
		"duration", durationSec,
		"jobTimeout", jobTimeoutSec,
		"warmup", warmupSec,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &Server{ctx: ctx, orchestrator: orchestrator, activeTests: make(map[string]bool)}
	mux := http.NewServeMux()
	mux.HandleFunc("/run", srv.runHandler)
	mux.HandleFunc("/status", srv.statusHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			slog.Error("healthz write error", "err", err)
		}
	})

	httpServer := &http.Server{Addr: ":8080", Handler: mux}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("HTTP server listening on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Run(ctx, func(c context.Context, e SandboxReadyEvent) {
			srv.mu.Lock()
			if srv.activeTests[e.SubmissionID] {
				srv.mu.Unlock()
				slog.Info("ignoring sandbox.ready, test already running", "submission", e.SubmissionID)
				return
			}
			srv.activeTests[e.SubmissionID] = true
			srv.mu.Unlock()

			defer func() {
				srv.mu.Lock()
				delete(srv.activeTests, e.SubmissionID)
				srv.mu.Unlock()
			}()

			orchestrator.Handle(c, e)
		})
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}

	wg.Wait()

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
