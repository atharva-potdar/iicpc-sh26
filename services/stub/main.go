package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

var orderIDRegex = regexp.MustCompile(`^[A-Za-z0-9/-]+$`)


var lastTimestamp atomic.Int64

func now() int64 {
	ts := time.Now().UnixNano()
	for {
		old := lastTimestamp.Load()
		if ts <= old {
			ts = old + 1
		}
		if lastTimestamp.CompareAndSwap(old, ts) {
			return ts
		}
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ── Message types ──────────────────────────────────────────────────────────

type IncomingMessage struct {
	Type      string  `json:"type"`
	OrderID   string  `json:"order_id"`
	Side      string  `json:"side"`
	OrderType string  `json:"order_type"`
	Price     float64 `json:"price"`
	Quantity  int64   `json:"quantity"`
}

type AckMessage struct {
	Type      string `json:"type"`
	OrderID   string `json:"order_id"`
	Timestamp int64  `json:"timestamp"`
}

type FillMessage struct {
	Type      string  `json:"type"`
	OrderID   string  `json:"order_id"`
	FilledQty int64   `json:"filled_qty"`
	FillPrice float64 `json:"fill_price"`
	Remaining int64   `json:"remaining"`
	Timestamp int64   `json:"timestamp"`
}

type CancelAckMessage struct {
	Type      string `json:"type"`
	OrderID   string `json:"order_id"`
	Timestamp int64  `json:"timestamp"`
}

type RejectMessage struct {
	Type      string `json:"type"`
	OrderID   string `json:"order_id"`
	Reason    string `json:"reason"`
	Timestamp int64  `json:"timestamp"`
}

// ── Orderbook ──────────────────────────────────────────────────────────────

type Order struct {
	OrderID   string
	Side      string
	OrderType string
	Price     float64
	Quantity  int64
	Remaining int64
	EnteredAt int64
	SessionID string
	index     int // heap index for removal
}

// bidHeap is a max-heap by price, then min-heap by time (earlier first)
type bidHeap []*Order

func (h bidHeap) Len() int { return len(h) }
func (h bidHeap) Less(i, j int) bool {
	if h[i].Price != h[j].Price {
		return h[i].Price > h[j].Price // higher price = better bid
	}
	return h[i].EnteredAt < h[j].EnteredAt // earlier time = first in queue
}

func (h bidHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *bidHeap) Push(x any) {
	n := len(*h)
	order := x.(*Order)
	order.index = n
	*h = append(*h, order)
}

func (h *bidHeap) Pop() any {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.index = -1
	*h = old[:n-1]
	return order
}

// askHeap is a min-heap by price, then min-heap by time (earlier first)
type askHeap []*Order

func (h askHeap) Len() int { return len(h) }
func (h askHeap) Less(i, j int) bool {
	if h[i].Price != h[j].Price {
		return h[i].Price < h[j].Price // lower price = better ask
	}
	return h[i].EnteredAt < h[j].EnteredAt // earlier time = first in queue
}

func (h askHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *askHeap) Push(x any) {
	n := len(*h)
	order := x.(*Order)
	order.index = n
	*h = append(*h, order)
}

func (h *askHeap) Pop() any {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.index = -1
	*h = old[:n-1]
	return order
}

type Orderbook struct {
	mu     sync.Mutex
	bids   bidHeap
	asks   askHeap
	orders map[string]*Order
}

func NewOrderbook() *Orderbook {
	ob := &Orderbook{
		orders: make(map[string]*Order),
	}
	heap.Init(&ob.bids)
	heap.Init(&ob.asks)
	return ob
}

// match attempts to fill order against the opposite side.
// Returns fill messages for the incoming order.
// Must be called with ob.mu held.
func (ob *Orderbook) match(order *Order) []FillMessage {
	var fills []FillMessage

	if order.Side == "buy" {
		for order.Remaining > 0 && len(ob.asks) > 0 {
			best := ob.asks[0]
			if order.OrderType == "limit" && order.Price < best.Price {
				break
			}
			qty := min64(order.Remaining, best.Remaining)
			order.Remaining -= qty
			best.Remaining -= qty
			fills = append(fills, FillMessage{
				Type:      "fill",
				OrderID:   order.OrderID,
				FilledQty: qty,
				FillPrice: best.Price,
				Remaining: order.Remaining,
				Timestamp: now(),
			})
			if best.Remaining == 0 {
				delete(ob.orders, best.OrderID)
				heap.Pop(&ob.asks)
			} else {
				heap.Fix(&ob.asks, 0)
			}
		}
	} else {
		for order.Remaining > 0 && len(ob.bids) > 0 {
			best := ob.bids[0]
			if order.OrderType == "limit" && order.Price > best.Price {
				break
			}
			qty := min64(order.Remaining, best.Remaining)
			order.Remaining -= qty
			best.Remaining -= qty
			fills = append(fills, FillMessage{
				Type:      "fill",
				OrderID:   order.OrderID,
				FilledQty: qty,
				FillPrice: best.Price,
				Remaining: order.Remaining,
				Timestamp: now(),
			})
			if best.Remaining == 0 {
				delete(ob.orders, best.OrderID)
				heap.Pop(&ob.bids)
			} else {
				heap.Fix(&ob.bids, 0)
			}
		}
	}

	return fills
}

// rest adds a partially or unfilled limit order to the book.
// Must be called with ob.mu held.
func (ob *Orderbook) rest(order *Order) {
	ob.orders[order.OrderID] = order
	if order.Side == "buy" {
		heap.Push(&ob.bids, order)
	} else {
		heap.Push(&ob.asks, order)
	}
}

func (ob *Orderbook) remove(orderID string) {
	order, exists := ob.orders[orderID]
	if !exists {
		return
	}
	delete(ob.orders, orderID)

	if order.Side == "buy" {
		if order.index >= 0 && order.index < len(ob.bids) {
			heap.Remove(&ob.bids, order.index)
		}
	} else if order.Side == "sell" {
		if order.index >= 0 && order.index < len(ob.asks) {
			heap.Remove(&ob.asks, order.index)
		}
	}
}

func (ob *Orderbook) cancelSession(sessionID string) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var toRemove []*Order
	for _, o := range ob.orders {
		if o.SessionID == sessionID {
			toRemove = append(toRemove, o)
		}
	}

	for _, o := range toRemove {
		ob.remove(o.OrderID)
	}
}

// ── Session ────────────────────────────────────────────────────────────────

type Session struct {
	id      string
	conn    *websocket.Conn
	ob      *Orderbook
	send    chan any
	ctx     context.Context
	latency time.Duration
	wg      sync.WaitGroup
}

func (s *Session) writeLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-s.send:
			if !ok {
				return
			}
			if err := wsjson.Write(s.ctx, s.conn, msg); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("write error", "session", s.id, "error", err)
				}
				return
			}
		case <-ticker.C:
			if err := s.conn.Ping(s.ctx); err != nil {
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) handleOrder(msg IncomingMessage) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}

	// Validate fields
	if len(msg.OrderID) == 0 || len(msg.OrderID) > 64 || !orderIDRegex.MatchString(msg.OrderID) ||
		(msg.Side != "buy" && msg.Side != "sell") ||
		(msg.OrderType != "limit" && msg.OrderType != "market") {
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "invalid_order", Timestamp: now(),
		}
		return
	}
	if msg.Quantity <= 0 {
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "invalid_quantity", Timestamp: now(),
		}
		return
	}
	if msg.OrderType == "limit" && msg.Price <= 0 {
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "invalid_price", Timestamp: now(),
		}
		return
	}

	s.ob.mu.Lock()

	if _, exists := s.ob.orders[msg.OrderID]; exists {
		s.ob.mu.Unlock()
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "duplicate_order_id", Timestamp: now(),
		}
		return
	}

	order := &Order{
		OrderID:   msg.OrderID,
		Side:      msg.Side,
		OrderType: msg.OrderType,
		Price:     msg.Price,
		Quantity:  msg.Quantity,
		Remaining: msg.Quantity,
		EnteredAt: now(),
		SessionID: s.id,
		index:     -1,
	}

	// Messages to send outside the lock
	var toSend []any

	// Ack immediately, before matching.
	toSend = append(toSend, AckMessage{Type: "ack", OrderID: msg.OrderID, Timestamp: order.EnteredAt})

	fills := s.ob.match(order)
	for i := range fills {
		toSend = append(toSend, fills[i])
	}

	if order.OrderType == "market" && order.Remaining > 0 {
		// Market order could not fully fill — reject remaining
		toSend = append(toSend, RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "no_liquidity", Timestamp: now(),
		})
	} else if order.Remaining > 0 {
		// Limit order rests on the book
		s.ob.rest(order)
	}

	s.ob.mu.Unlock()

	for _, m := range toSend {
		s.send <- m
	}
}

func (s *Session) handleCancel(msg IncomingMessage) {
	s.ob.mu.Lock()
	order, exists := s.ob.orders[msg.OrderID]
	if !exists {
		s.ob.mu.Unlock()
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "unknown_order", Timestamp: now(),
		}
		return
	}
	if order.SessionID != s.id {
		s.ob.mu.Unlock()
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "unauthorized", Timestamp: now(),
		}
		return
	}
	s.ob.remove(msg.OrderID)
	s.ob.mu.Unlock()

	s.send <- CancelAckMessage{Type: "cancel_ack", OrderID: msg.OrderID, Timestamp: now()}
}

// ── HTTP handlers ──────────────────────────────────────────────────────────

type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity int64   `json:"quantity"`
}

type SnapshotResponse struct {
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
	Timestamp int64        `json:"timestamp"`
}

func orderbookHandler(ob *Orderbook) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ob.mu.Lock()
		bidMap := make(map[float64]int64)
		askMap := make(map[float64]int64)
		for _, o := range ob.bids {
			bidMap[o.Price] += o.Remaining
		}
		for _, o := range ob.asks {
			askMap[o.Price] += o.Remaining
		}
		ob.mu.Unlock()

		bids := make([]PriceLevel, 0, len(bidMap))
		asks := make([]PriceLevel, 0, len(askMap))
		for p, q := range bidMap {
			bids = append(bids, PriceLevel{p, q})
		}
		for p, q := range askMap {
			asks = append(asks, PriceLevel{p, q})
		}
		sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
		sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(SnapshotResponse{
			Bids: bids, Asks: asks, Timestamp: now(),
		}); err != nil {
			slog.Error("orderbook encode error", "err", err)
		}
	}
}

func streamHandler(latency time.Duration, ob *Orderbook) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			slog.Error("accept", "err", err)
			return
		}

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		s := &Session{
			id:      r.RemoteAddr + "/" + strconv.FormatInt(now(), 10),
			conn:    conn,
			ob:      ob,
			send:    make(chan any, 256),
			ctx:     ctx,
			latency: latency,
		}

		s.wg.Add(1)
		go s.writeLoop()
		defer func() {
			s.ob.cancelSession(s.id)
			close(s.send)
			s.wg.Wait()
			if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				slog.Error("ws close error", "err", err)
			}
		}()

		for {
			var msg IncomingMessage
			if err := wsjson.Read(ctx, conn, &msg); err != nil {
				if !errors.Is(err, context.Canceled) && websocket.CloseStatus(err) != websocket.StatusNormalClosure {
					slog.Error("read error", "session", s.id, "error", err)
				}
				return
			}
			switch msg.Type {
			case "order":
				s.handleOrder(msg)
			case "cancel":
				s.handleCancel(msg)
			default:
				s.send <- RejectMessage{
					Type: "reject", OrderID: msg.OrderID,
					Reason: "unknown_type", Timestamp: now(),
				}
			}
		}
	}
}

// ── Main ───────────────────────────────────────────────────────────────────

func main() {
	var latency time.Duration
	if v := os.Getenv("STUB_LATENCY_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			latency = time.Duration(ms) * time.Millisecond
		}
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ob := NewOrderbook()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			slog.Error("healthz write error", "err", err)
		}
	})
	mux.HandleFunc("GET /stream", streamHandler(latency, ob))
	mux.HandleFunc("GET /orderbook", orderbookHandler(ob))

	slog.Info("stub listening", "port", port, "latency", latency)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "err", err)
	}
}
