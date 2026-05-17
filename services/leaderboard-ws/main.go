package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// leaderboardEntry mirrors the enriched payload written by telemetry-ingester.
type leaderboardEntry struct {
	SubmissionID string  `json:"submission_id"`
	TeamName     string  `json:"team_name"`
	Score        float64 `json:"score"`
	TPS          float64 `json:"tps"`
	AckP50US     int64   `json:"ack_p50_us"`
	AckP90US     int64   `json:"ack_p90_us"`
	AckP99US     int64   `json:"ack_p99_us"`
	OrdersSent   int64   `json:"orders_sent"`
	RejectsRecv  int64   `json:"rejects_recv"`
	Correctness  float64 `json:"correctness"`
	DurationMS   int64   `json:"duration_ms"`
	Timestamp    string  `json:"timestamp"`
	Rank         int     `json:"rank"`
}

func run() error {
	redisAddr := envStr("REDIS_ADDR", "redis.platform.svc.cluster.local:6379")
	redisPass := envStr("REDIS_PASSWORD", "")
	port := envStr("PORT", "8090")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
	})
	defer func() {
		if err := rdb.Close(); err != nil {
			slog.Error("redis close error", "err", err)
		}
	}()

	hub := newHub()
	go hub.run()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go subscribeRedis(ctx, rdb, hub)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			slog.Error("healthz write error", "err", err)
		}
	})

	mux.HandleFunc("GET /api/leaderboard", leaderboardHandler(rdb))
	mux.HandleFunc("GET /ws", wsHandler(rdb, hub))

	// Serve embedded static frontend. fs.Sub strips the "static" prefix so
	// that index.html is served at "/" rather than "/static/index.html".
	frontend, err := fs.Sub(staticFS, "static")
	if err != nil {
		return fmt.Errorf("fs.Sub: %v", err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(frontend)))

	slog.Info("leaderboard-ws listening", "port", port)

	srv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	close(hub.quitCh)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %v", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// leaderboardHandler reads the full ranked list from Redis and returns it as
// a JSON array ordered by rank (descending score). Each entry is fetched from
// the leaderboard_details hash so we can return full metrics.
func leaderboardHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// ZREVRANGE returns members in descending score order (best first).
		members, err := rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, -1).Result()
		if err != nil {
			slog.Error("zrevrange", "err", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}

		entries := make([]leaderboardEntry, 0, len(members))
		for rank, z := range members {
			memberStr, ok := z.Member.(string)
			if !ok {
				continue
			}
			// Member format: "submission_id:team_name"
			submissionID := strings.SplitN(memberStr, ":", 2)[0]

			blob, err := rdb.HGet(ctx, "leaderboard_details", submissionID).Bytes()
			if err != nil {
				// Entry in sorted set but details missing — emit a minimal record.
				parts := strings.SplitN(memberStr, ":", 2)
				teamName := ""
				if len(parts) == 2 {
					teamName = parts[1]
				}
				entries = append(entries, leaderboardEntry{
					SubmissionID: submissionID,
					TeamName:     teamName,
					Score:        z.Score / 1000,
					Rank:         rank + 1,
				})
				continue
			}

			var entry leaderboardEntry
			if err := json.Unmarshal(blob, &entry); err != nil {
				slog.Error("unmarshal leaderboard_details", "submission", submissionID, "err", err)
				continue
			}
			entry.Rank = rank + 1
			entries = append(entries, entry)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err := json.NewEncoder(w).Encode(entries); err != nil {
			slog.Error("encode leaderboard response", "err", err)
		}
	}
}

// subscribeRedis blocks and fans every message from the leaderboard_updates
// pub/sub channel out to all connected WebSocket clients via the hub.
func subscribeRedis(ctx context.Context, rdb *redis.Client, hub *Hub) {
	sub := rdb.Subscribe(ctx, "leaderboard_updates")
	defer func() {
		if err := sub.Close(); err != nil {
			slog.Error("redis subscription close error", "err", err)
		}
	}()

	ch := sub.Channel()
	for msg := range ch {
		hub.broadcast([]byte(msg.Payload))
	}
}
