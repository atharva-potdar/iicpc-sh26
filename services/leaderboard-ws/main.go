package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"

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

func main() {
	redisAddr := envStr("REDIS_ADDR", "redis.platform.svc.cluster.local:6379")
	port := envStr("PORT", "8090")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	hub := newHub()
	go hub.run()
	go subscribeRedis(rdb, hub)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("GET /api/leaderboard", leaderboardHandler(rdb))
	mux.HandleFunc("GET /ws", wsHandler(rdb, hub))

	// Serve embedded static frontend. fs.Sub strips the "static" prefix so
	// that index.html is served at "/" rather than "/static/index.html".
	frontend, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("fs.Sub: %v", err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(frontend)))

	log.Printf("leaderboard-ws listening :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
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
			log.Printf("zrevrange: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}

		entries := make([]leaderboardEntry, 0, len(members))
		for rank, z := range members {
			// Member format: "submission_id:team_name"
			submissionID := strings.SplitN(z.Member.(string), ":", 2)[0]

			blob, err := rdb.HGet(ctx, "leaderboard_details", submissionID).Bytes()
			if err != nil {
				// Entry in sorted set but details missing — emit a minimal record.
				parts := strings.SplitN(z.Member.(string), ":", 2)
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
				log.Printf("unmarshal leaderboard_details[%s]: %v", submissionID, err)
				continue
			}
			entry.Rank = rank + 1
			entries = append(entries, entry)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err := json.NewEncoder(w).Encode(entries); err != nil {
			log.Printf("encode leaderboard response: %v", err)
		}
	}
}

// subscribeRedis blocks and fans every message from the leaderboard_updates
// pub/sub channel out to all connected WebSocket clients via the hub.
func subscribeRedis(rdb *redis.Client, hub *Hub) {
	ctx := context.Background()
	sub := rdb.Subscribe(ctx, "leaderboard_updates")
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		hub.broadcast([]byte(msg.Payload))
	}
}
