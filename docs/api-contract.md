# Submission API Contract v1

Contestants implement two interfaces:

- A WebSocket endpoint for all order flow (hot path)
- Two HTTP endpoints for health checking and correctness validation (cold path)

The platform connects via WebSocket before activating the bot fleet.
Each bot maintains one persistent WebSocket connection for its lifetime.

---

## HTTP Endpoints

### Health Check

GET /healthz

Response 200:
{
  "status": "ok"
}

### Orderbook Snapshot

GET /orderbook

Response 200:
{
  "bids": [
    { "price": number, "quantity": number }
  ],
  "asks": [
    { "price": number, "quantity": number }
  ],
  "timestamp": number   // unix nanoseconds
}

Notes:

- Bids sorted descending by price (best bid first).
- Asks sorted ascending by price (best ask first).
- Never called concurrently with load testing.
- Used exclusively for correctness validation.

---

## WebSocket Endpoint

WS /stream

One persistent connection per bot. The bot sends orders and cancels
as JSON text frames. The submission responds asynchronously with acks,
fills, and rejects as JSON text frames on the same connection.

There is no request/response pairing at the transport level —
messages in both directions are fire-and-forget frames.
Correlation is done by order_id.

---

## Client → Submission Messages

### Submit Order

{
  "type":      "order",
  "order_id":  string,    // client-generated UUID, must be unique per session
  "side":      "buy" | "sell",
  "order_type": "limit" | "market",
  "price":     number,    // required for limit orders, omit for market
  "quantity":  number     // integer units, must be > 0
}

### Cancel Order

{
  "type":     "cancel",
  "order_id": string      // must reference a live resting order
}

---

## Submission → Client Messages

### Ack

Sent immediately upon receiving a valid order, before any matching occurs.
This is the primary latency measurement point.

{
  "type":       "ack",
  "order_id":   string,
  "timestamp":  number    // unix nanoseconds, when order entered the book
}

### Fill

Sent when an order is fully or partially matched.
May arrive immediately after ack (if liquidity exists) or later
(when a resting limit order is matched by a future order).

{
  "type":        "fill",
  "order_id":    string,
  "filled_qty":  number,
  "fill_price":  number,
  "remaining":   number,   // 0 if fully filled
  "timestamp":   number    // unix nanoseconds, when match occurred
}

### Cancel Ack

{
  "type":      "cancel_ack",
  "order_id":  string,
  "timestamp": number
}

### Reject

Sent instead of ack when the order is malformed or violates rules.

{
  "type":     "reject",
  "order_id": string,
  "reason":   "invalid_price" | "invalid_quantity" | "duplicate_order_id"
              | "no_liquidity" | "unknown_order",
  "timestamp": number
}

Notes:

- "no_liquidity" is the reject reason for market orders with no opposing side.
- "unknown_order" is the reject reason for cancels referencing non-existent
  or already-filled orders.
- A reject is always terminal — the order will never fill after a reject.

---

## Scoring dimensions mapped to messages

Ack latency:    time from order frame received to ack frame sent (p50/p90/p99)
Fill latency:   time from order frame received to fill frame sent (p50/p90/p99)
Throughput:     max sustained orders/sec before rejects or connection drops
Correctness:    GET /orderbook asserted against expected state after
                deterministic order sequences

---

## Connection lifecycle

On connect:     submission may optionally send a welcome frame (ignored by platform)
On disconnect:  all resting orders for that session are cancelled
On error frame: submission sends a reject; connection stays alive
Fatal errors:   submission may close the connection with a WebSocket
                close code and reason string; platform records as failure
