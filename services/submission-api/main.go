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

	"github.com/google/uuid"
)

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

type contextKey string

const requestIDKey contextKey = "request_id"

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func loggerFor(r *http.Request) *slog.Logger {
	reqID, _ := r.Context().Value(requestIDKey).(string)
	return slog.With("request_id", reqID)
}

func run() error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	seaweedfsEndpoint := envStr("SEAWEEDFS_ENDPOINT", "http://seaweedfs.platform.svc.cluster.local:8333")
	redpandaBrokers := strings.Split(envStr("REDPANDA_BROKERS", "redpanda.platform.svc.cluster.local:9092"), ",")
	port := envStr("PORT", "8080")
	maxUploadMB := envInt64("MAX_UPLOAD_SIZE_MB", 128)

	storage, err := NewStorage(seaweedfsEndpoint)
	if err != nil {
		return fmt.Errorf("init storage: %v", err)
	}

	publisher, err := NewPublisher(redpandaBrokers)
	if err != nil {
		return fmt.Errorf("init publisher: %v", err)
	}
	defer publisher.Close()

	h := NewHandler(storage, publisher, maxUploadMB)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			loggerFor(r).Error("healthz write error", "error", err)
		}
	})
	mux.HandleFunc("POST /submissions", h.handleSubmit)

	handler := requestIDMiddleware(mux)

	logger.Info("submission-api listening", "port", port)

	srv := &http.Server{Addr: ":" + port, Handler: handler}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown error", "error", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
