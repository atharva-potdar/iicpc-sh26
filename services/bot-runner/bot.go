package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type IncomingMessage struct {
	Type      string  `json:"type"`
	OrderID   string  `json:"order_id"`
	Reason    string  `json:"reason"`
	FilledQty int64   `json:"filled_qty"`
	FillPrice float64 `json:"fill_price"`
	Remaining int64   `json:"remaining"`
	Timestamp int64   `json:"timestamp"`
}

type pendingOrder struct {
	sentAt time.Time
}

type Bot struct {
	id       int
	endpoint string
	seq      Sequence
	metrics  *BotMetrics
}

func NewBot(id int, endpoint string, seq Sequence) *Bot {
	return &Bot{
		id:       id,
		endpoint: endpoint,
		seq:      seq,
		metrics:  NewBotMetrics(),
	}
}

func (b *Bot) Run(ctx context.Context, duration time.Duration, ready chan<- struct{}, quiet chan<- struct{}) {
	var conn *websocket.Conn
	for {
		if ctx.Err() != nil {
			return
		}
		var err error
		conn, _, err = websocket.Dial(ctx, b.endpoint, nil)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "connection refused") {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		log.Printf("bot %d warmup: %v", b.id, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	type stepTemplate struct {
		isCancel  bool
		payload   []byte
		tag       string
		cancelTag string
	}
	templates := make([]stepTemplate, len(b.seq.Steps))
	for i, step := range b.seq.Steps {
		if step.Kind == StepOrder {
			tmpl := fmt.Sprintf(`{"type":"order","order_id":"%%s","side":"%s","order_type":"%s","price":%f,"quantity":%d}`, step.Side, step.OrderType, step.Price, step.Quantity)
			templates[i] = stepTemplate{
				isCancel:  false,
				payload:   []byte(tmpl),
				tag:       step.Tag,
			}
		} else {
			tmpl := `{"type":"cancel","order_id":"%s"}`
			templates[i] = stepTemplate{
				isCancel:  true,
				payload:   []byte(tmpl),
				cancelTag: step.CancelTag,
			}
		}
	}

	if ready != nil {
		ready <- struct{}{}
	}

	pending := make(map[string]pendingOrder)
	var mu sync.Mutex
	var inFlight atomic.Int32
	errCh := make(chan error, 1)

	// Reader Goroutine
	go func() {
		for {
			_, rawBytes, err := conn.Read(ctx)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			var msg IncomingMessage
			if err := json.Unmarshal(rawBytes, &msg); err == nil {
				mu.Lock()
				if p, ok := pending[msg.OrderID]; ok {
					switch msg.Type {
					case "ack":
						b.metrics.recordAck(time.Since(p.sentAt))
						b.metrics.acksRecv++
					case "fill":
						b.metrics.recordFill(time.Since(p.sentAt))
						b.metrics.fillsRecv++
						if msg.Remaining == 0 {
							delete(pending, msg.OrderID)
							inFlight.Add(-1)
						}
					case "reject":
						b.metrics.rejectsRecv++
						delete(pending, msg.OrderID)
						inFlight.Add(-1)
					case "cancel_ack":
						delete(pending, msg.OrderID)
						inFlight.Add(-1)
					}
				}
				mu.Unlock()
			}
		}
	}()

	// Cleanup Goroutine (Garbage Collector)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				mu.Lock()
				for id, order := range pending {
					if now.Sub(order.sentAt) > 5*time.Second {
						delete(pending, id)
						inFlight.Add(-1)
						b.metrics.connDrops++
					}
				}
				mu.Unlock()
			}
		}
	}()

	deadline := time.Now().Add(duration)
	iteration := 0
	tagToID := make(map[string]string)

	// Writer Loop
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		select {
		case err := <-errCh:
			log.Printf("bot %d read error: %v", b.id, err)
			mu.Lock()
			b.metrics.connDrops++
			mu.Unlock()
			goto drain
		default:
		}

		for _, tmpl := range templates {
			var oid string
			var payload []byte

			if tmpl.isCancel {
				targetID, ok := tagToID[tmpl.cancelTag]
				if !ok {
					continue
				}
				oid = targetID
				payload = bytes.Replace(tmpl.payload, []byte("%s"), []byte(targetID), 1)
			} else {
				oid = orderID(b.id, iteration, tmpl.tag)
				tagToID[tmpl.tag] = oid
				payload = bytes.Replace(tmpl.payload, []byte("%s"), []byte(oid), 1)

				// Backpressure check
				for inFlight.Load() > 20 {
					time.Sleep(1 * time.Millisecond)
				}

				mu.Lock()
				pending[oid] = pendingOrder{time.Now()}
				b.metrics.ordersSent++
				mu.Unlock()

				inFlight.Add(1)
			}

			if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
				log.Printf("bot %d write error: %v", b.id, err)
				mu.Lock()
				b.metrics.connDrops++
				mu.Unlock()
				goto drain
			}
		}
		iteration++
	}

drain:
	// Always close the connection before signaling quiet so the stub's
	// cancelSession cleans up any partial-iteration orders left on the book
	// (e.g. cancel_correctness probe sell cut off by the deadline).
	// Since ComputeExpected always returns an empty book, we need a clean
	// book, not live connections, during the snapshot.
	conn.Close(websocket.StatusNormalClosure, "done")

	// Give the server time to process cancelSession before the validator
	// queries GET /orderbook.
	time.Sleep(500 * time.Millisecond)

	// Signal that draining is complete.
	if quiet != nil {
		select {
		case quiet <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
	<-ctx.Done()
}
