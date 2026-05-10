package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func now() int64 { return time.Now().UnixNano() }

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
}

type Orderbook struct {
	mu     sync.Mutex
	bids   []*Order // descending price, ascending time
	asks   []*Order // ascending price, ascending time
	orders map[string]*Order
}

func NewOrderbook() *Orderbook {
	return &Orderbook{orders: make(map[string]*Order)}
}

// match attempts to fill order against the opposite side.
// Returns fill messages for the incoming order.
// Must be called with ob.mu held.
func (ob *Orderbook) match(order *Order) []FillMessage {
	var fills []FillMessage

	if order.Side == "buy" {
		for len(ob.asks) > 0 && order.Remaining > 0 {
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
				ob.asks = ob.asks[1:]
			}
		}
	} else {
		for len(ob.bids) > 0 && order.Remaining > 0 {
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
				ob.bids = ob.bids[1:]
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
		ob.bids = append(ob.bids, order)
		sort.Slice(ob.bids, func(i, j int) bool {
			if ob.bids[i].Price != ob.bids[j].Price {
				return ob.bids[i].Price > ob.bids[j].Price
			}
			return ob.bids[i].EnteredAt < ob.bids[j].EnteredAt
		})
	} else {
		ob.asks = append(ob.asks, order)
		sort.Slice(ob.asks, func(i, j int) bool {
			if ob.asks[i].Price != ob.asks[j].Price {
				return ob.asks[i].Price < ob.asks[j].Price
			}
			return ob.asks[i].EnteredAt < ob.asks[j].EnteredAt
		})
	}
}

func (ob *Orderbook) remove(orderID, side string) {
	delete(ob.orders, orderID)
	if side == "buy" {
		ob.bids = removeFromSlice(ob.bids, orderID)
	} else {
		ob.asks = removeFromSlice(ob.asks, orderID)
	}
}

func (ob *Orderbook) cancelSession(sessionID string) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	for id, o := range ob.orders {
		if o.SessionID == sessionID {
			delete(ob.orders, id)
			if o.Side == "buy" {
				ob.bids = removeFromSlice(ob.bids, id)
			} else {
				ob.asks = removeFromSlice(ob.asks, id)
			}
		}
	}
}

func removeFromSlice(orders []*Order, id string) []*Order {
	for i, o := range orders {
		if o.OrderID == id {
			return append(orders[:i], orders[i+1:]...)
		}
	}
	return orders
}

// ── Session ────────────────────────────────────────────────────────────────

type Session struct {
	id      string
	conn    *websocket.Conn
	ob      *Orderbook
	send    chan any
	ctx     context.Context
	latency time.Duration
}

func (s *Session) writeLoop() {
	for {
		select {
		case msg, ok := <-s.send:
			if !ok {
				return
			}
			if err := wsjson.Write(s.ctx, s.conn, msg); err != nil {
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
	if msg.OrderID == "" ||
		(msg.Side != "buy" && msg.Side != "sell") ||
		(msg.OrderType != "limit" && msg.OrderType != "market") {
		s.send <- RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "invalid_price", Timestamp: now(),
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
	}

	// Ack immediately, before matching. Send is non-blocking (buffered channel).
	s.send <- AckMessage{Type: "ack", OrderID: msg.OrderID, Timestamp: order.EnteredAt}

	fills := s.ob.match(order)

	var reject *RejectMessage
	if order.OrderType == "market" && order.Remaining > 0 {
		// Market order could not fully fill — reject remaining
		reject = &RejectMessage{
			Type: "reject", OrderID: msg.OrderID,
			Reason: "no_liquidity", Timestamp: now(),
		}
	} else if order.Remaining > 0 {
		// Limit order rests on the book
		s.ob.rest(order)
	}

	s.ob.mu.Unlock()

	for i := range fills {
		s.send <- fills[i]
	}
	if reject != nil {
		s.send <- reject
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
	s.ob.remove(msg.OrderID, order.Side)
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
		defer ob.mu.Unlock()

		bidMap := make(map[float64]int64)
		askMap := make(map[float64]int64)
		for _, o := range ob.bids {
			bidMap[o.Price] += o.Remaining
		}
		for _, o := range ob.asks {
			askMap[o.Price] += o.Remaining
		}

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
		json.NewEncoder(w).Encode(SnapshotResponse{
			Bids: bids, Asks: asks, Timestamp: now(),
		})
	}
}

func streamHandler(ob *Orderbook, latency time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			log.Printf("accept: %v", err)
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

		go s.writeLoop()
		defer func() {
			s.ob.cancelSession(s.id)
			conn.Close(websocket.StatusNormalClosure, "")
		}()

		for {
			var msg IncomingMessage
			if err := wsjson.Read(ctx, conn, &msg); err != nil {
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
					Reason: "unknown_order", Timestamp: now(),
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
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /orderbook", orderbookHandler(ob))
	mux.HandleFunc("GET /stream", streamHandler(ob, latency))

	log.Printf("stub listening :%s (latency=%s)", port, latency)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
