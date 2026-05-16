package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
)

// client represents a single connected browser WebSocket session.
type client struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub maintains the set of active clients and fans out broadcast messages.
type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
	regCh   chan *client
	unregCh chan *client
	msgCh   chan []byte
	quitCh  chan struct{}
}

func newHub() *Hub {
	return &Hub{
		clients: make(map[*client]struct{}),
		regCh:   make(chan *client, 64),
		unregCh: make(chan *client, 64),
		msgCh:   make(chan []byte, 256),
		quitCh:  make(chan struct{}),
	}
}

// run is the hub's central event loop. Must be called in its own goroutine.
func (h *Hub) run() {
	for {
		select {
		case <-h.quitCh:
			return
		case c := <-h.regCh:
			h.mu.Lock()
			h.clients[c] = struct{}{}
			h.mu.Unlock()

		case c := <-h.unregCh:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()

		case msg := <-h.msgCh:
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					// Slow client — drop the message rather than blocking.
				}
			}
			h.mu.RUnlock()
		}
	}
}

// broadcast enqueues a message for fan-out to all connected clients.
func (h *Hub) broadcast(msg []byte) {
	h.msgCh <- msg
}

// wsHandler upgrades the HTTP connection to WebSocket, sends a full leaderboard
// snapshot on connect, then streams live updates from the hub.
func wsHandler(rdb *redis.Client, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			slog.Error("ws accept", "error", err)
			return
		}

		c := &client{
			conn: conn,
			send: make(chan []byte, 256),
		}

		hub.regCh <- c

		ctx, cancel := context.WithCancel(r.Context())
		defer func() {
			cancel()
			hub.unregCh <- c
			if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				slog.Error("ws close error", "error", err)
			}
		}()

		// Send a full snapshot on connect so the client doesn't have to make a
		// separate HTTP request before receiving live updates.
		if snapshot, err := buildSnapshot(r.Context(), rdb); err == nil {
			select {
			case c.send <- snapshot:
			default:
			}
		}

		// Writer goroutine: drains c.send and writes to the WebSocket.
		go func() {
			for msg := range c.send {
				if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
					cancel()
					return
				}
			}
		}()

		// Reader: keep the connection alive and detect client disconnect.
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}
}

// buildSnapshot returns a JSON array of all current leaderboard entries
// suitable for sending as the first message on a new WebSocket connection.
func buildSnapshot(ctx context.Context, rdb *redis.Client) ([]byte, error) {
	members, err := rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]leaderboardEntry, 0, len(members))
	for rank, z := range members {
		submissionID := strings.SplitN(z.Member.(string), ":", 2)[0]

		blob, err := rdb.HGet(ctx, "leaderboard_details", submissionID).Bytes()
		if err != nil {
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
			slog.Error("unmarshal snapshot entry", "submission", submissionID, "error", err)
			continue
		}
		entry.Rank = rank + 1
		entries = append(entries, entry)
	}

	// Wrap in a snapshot envelope so the client can distinguish an initial
	// snapshot from a single-entry live update.
	type snapshotMsg struct {
		Type    string           `json:"type"`
		Entries []leaderboardEntry `json:"entries"`
	}
	return json.Marshal(snapshotMsg{Type: "snapshot", Entries: entries})
}
