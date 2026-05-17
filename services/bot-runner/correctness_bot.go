package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// CorrectnessResult holds the outcome of all correctness sequences.
type CorrectnessResult struct {
	Score           float64
	Passed          int
	Failed          int
	TotalAssertions int
	FailureDetails  []string
}

// CorrectnessBot runs deterministic sequences synchronously (one step at a
// time, wait for response, assert) to validate price-time priority, fill
// accuracy, and cancel behaviour.
type CorrectnessBot struct {
	endpoint string
	httpBase string
	client   *http.Client
}

func NewCorrectnessBot(wsEndpoint string) *CorrectnessBot {
	httpBase := strings.Replace(wsEndpoint, "ws://", "http://", 1)
	httpBase = strings.TrimSuffix(httpBase, "/stream")
	return &CorrectnessBot{
		endpoint: wsEndpoint,
		httpBase: httpBase,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (cb *CorrectnessBot) Run(ctx context.Context) CorrectnessResult {
	seqs := CorrectnessSequences()
	var result CorrectnessResult

	for _, seq := range seqs {
		slog.Info("correctness: running sequence", "name", seq.Name)
		passed, failures := cb.runSequence(ctx, seq)
		result.Passed += passed
		result.Failed += len(failures)
		for _, f := range failures {
			slog.Error("correctness: FAIL", "name", seq.Name, "failure", f)
			result.FailureDetails = append(result.FailureDetails, fmt.Sprintf("[%s] %s", seq.Name, f))
		}
	}

	result.TotalAssertions = result.Passed + result.Failed
	if result.TotalAssertions == 0 {
		result.Score = 1.0
	} else {
		result.Score = math.Max(0, float64(result.Passed)/float64(result.TotalAssertions))
	}
	return result
}

// runSequence opens a fresh WebSocket, sends each step synchronously, and
// returns a list of assertion failures (empty = all passed).
func (cb *CorrectnessBot) runSequence(ctx context.Context, seq Sequence) (int, []string) {
	var failures []string
	passed := 0

	conn, _, err := websocket.Dial(ctx, cb.endpoint, nil)
	if err != nil {
		return 0, []string{fmt.Sprintf("connect: %v", err)}
	}
	defer func() {
		if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
			slog.Error("correctness bot close error", "error", err)
		}
	}()

	tagToID := make(map[string]string)


	for i, step := range seq.Steps {
		var oid string
		var payload []byte

		if step.Kind == StepCancel {
			targetID, ok := tagToID[step.CancelTag]
			if !ok {
				failures = append(failures, fmt.Sprintf("step %d: cancel tag %q not found", i, step.CancelTag))
				continue
			}
			oid = targetID
			payload = []byte(fmt.Sprintf(`{"type":"cancel","order_id":"%s"}`, targetID))
		} else {
			oid = fmt.Sprintf("cx-s%d-%s", i, step.Tag)
			tagToID[step.Tag] = oid
			payload = []byte(fmt.Sprintf(
				`{"type":"order","order_id":"%s","side":"%s","order_type":"%s","price":%f,"quantity":%d}`,
				oid, step.Side, step.OrderType, step.Price, step.Quantity,
			))
		}

		// 5s deadline for the entire step (send + receive)
		stepDeadline := time.Now().Add(5 * time.Second)
		stepCtx, stepCancel := context.WithDeadline(ctx, stepDeadline)
		// We call stepCancel() at the end of the step processing.

		// Send
		if err := conn.Write(stepCtx, websocket.MessageText, payload); err != nil {
			stepCancel()
			failures = append(failures, fmt.Sprintf("step %d: write: %v", i, err))
			return passed, failures
		}

		// Read response(s) — we may get ack + fill, or just reject, or cancel_ack
		gotFill := false
		var fillPrice float64
		var fillQty int64
		gotReject := false
		gotAck := false
		gotCancelAck := false

		// Read messages until we have enough to assert.
		// For an order: expect ack, then possibly fill.
		// For a cancel: expect cancel_ack.
		for time.Now().Before(stepDeadline) {
			_, rawBytes, err := conn.Read(stepCtx)

			if err != nil {
				if ctx.Err() != nil {
					stepCancel()
					failures = append(failures, fmt.Sprintf("step %d: context cancelled", i))
					return passed, failures
				}
				// Timeout — stop reading
				break
			}

			var msg IncomingMessage
			if err := json.Unmarshal(rawBytes, &msg); err != nil {
				continue
			}

			switch msg.Type {
			case "ack":
				gotAck = true
			case "fill":
				gotFill = true
				fillPrice = msg.FillPrice
				fillQty = msg.FilledQty
			case "reject":
				gotReject = true
			case "cancel_ack":
				gotCancelAck = true
			}

			// Decide if we have enough responses to proceed
			if step.Kind == StepCancel && gotCancelAck {
				break
			}
			if step.ExpectReject && gotReject {
				break
			}
			if step.ExpectFill && gotAck && gotFill {
				break
			}
			if !step.ExpectFill && !step.ExpectReject && gotAck {
				break
			}
		}

		// Assertions

		// Cancel step: expect cancel_ack
		if step.Kind == StepCancel {
			if gotCancelAck {
				passed++
			} else {
				failures = append(failures, fmt.Sprintf("step %d: expected cancel_ack, got none", i))
			}
		}

		// Fill assertion
		if step.ExpectFill {
			if gotFill {
				passed++
				// Check fill price
				if step.ExpectFillPrice > 0 && math.Abs(fillPrice-step.ExpectFillPrice) > 0.01 {
					failures = append(failures, fmt.Sprintf("step %d: expected fill_price=%.2f, got=%.2f", i, step.ExpectFillPrice, fillPrice))
				} else if step.ExpectFillPrice > 0 {
					passed++
				}
				// Check fill qty
				if step.ExpectFillQty > 0 && fillQty != step.ExpectFillQty {
					failures = append(failures, fmt.Sprintf("step %d: expected fill_qty=%d, got=%d", i, step.ExpectFillQty, fillQty))
				} else if step.ExpectFillQty > 0 {
					passed++
				}
			} else {
				failures = append(failures, fmt.Sprintf("step %d: expected fill, got none", i))
			}
		}

		// Reject assertion
		if step.ExpectReject {
			if gotReject {
				passed++
			} else {
				failures = append(failures, fmt.Sprintf("step %d: expected reject, got none", i))
			}
		}

		// Orderbook state assertions
		if step.ExpectBookBids >= 0 || step.ExpectBookAsks >= 0 {
			// Small delay to let server settle
			time.Sleep(50 * time.Millisecond)
			bids, asks, err := cb.queryOrderbook()
			if err != nil {
				failures = append(failures, fmt.Sprintf("step %d: orderbook query: %v", i, err))
			} else {
				if step.ExpectBookBids >= 0 {
					if bids == step.ExpectBookBids {
						passed++
					} else {
						failures = append(failures, fmt.Sprintf("step %d: expected %d bid levels, got %d", i, step.ExpectBookBids, bids))
					}
				}
				if step.ExpectBookAsks >= 0 {
					if asks == step.ExpectBookAsks {
						passed++
					} else {
						failures = append(failures, fmt.Sprintf("step %d: expected %d ask levels, got %d", i, step.ExpectBookAsks, asks))
					}
				}
			}
		}
		stepCancel()
	}

	return passed, failures
}

func (cb *CorrectnessBot) queryOrderbook() (bids, asks int, err error) {
	resp, err := cb.client.Get(cb.httpBase + "/orderbook")
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("orderbook response body close error", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	var snap struct {
		Bids []json.RawMessage `json:"bids"`
		Asks []json.RawMessage `json:"asks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return 0, 0, err
	}
	return len(snap.Bids), len(snap.Asks), nil
}
