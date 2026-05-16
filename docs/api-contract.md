# API Contract v1

Contestants implement a WebSocket endpoint for order flow and two HTTP endpoints for health checking and orderbook snapshots. The platform connects via WebSocket before activating the bot fleet. Each bot maintains one persistent WebSocket connection for its lifetime.

## HTTP Endpoints

### `GET /healthz`

Health check probe.

**Response 200:**
```json
{ "status": "ok" }
```

### `GET /orderbook`

Returns the current orderbook state. Used exclusively for correctness validation — never called concurrently with load testing.

**Response 200:**
```json
{
  "bids": [
    { "price": 100.0, "quantity": 10 }
  ],
  "asks": [
    { "price": 100.0, "quantity": 5 }
  ],
  "timestamp": 1234567890
}
```

- `bids` sorted descending by price (best bid first)
- `asks` sorted ascending by price (best ask first)
- `timestamp` is unix nanoseconds
- Each price level aggregates remaining quantity across all resting orders at that price

## WebSocket Endpoint

### `GET /stream`

One persistent connection per bot. The client sends orders and cancels as JSON text frames. The submission responds asynchronously with acks, fills, rejects, and cancel acks as JSON text frames on the same connection.

There is no request/response pairing at the transport level — messages in both directions are fire-and-forget frames. Correlation is done by `order_id`.

## Client → Submission Messages

### Submit Order

```json
{
  "type":       "order",
  "order_id":   "string",
  "side":       "buy" | "sell",
  "order_type": "limit" | "market",
  "price":      100.0,
  "quantity":   10
}
```

| Field | Required | Constraints |
|-------|----------|-------------|
| `type` | Yes | Must be `"order"` |
| `order_id` | Yes | Must be unique per session |
| `side` | Yes | `"buy"` or `"sell"` |
| `order_type` | Yes | `"limit"` or `"market"` |
| `price` | For limit orders | Must be > 0 for limit orders |
| `quantity` | Yes | Must be > 0 |

### Cancel Order

```json
{
  "type":     "cancel",
  "order_id": "string"
}
```

| Field | Required | Constraints |
|-------|----------|-------------|
| `type` | Yes | Must be `"cancel"` |
| `order_id` | Yes | Must reference a live resting order |

## Submission → Client Messages

### Ack

Sent immediately upon receiving a valid order, before any matching occurs. This is the primary latency measurement point.

```json
{
  "type":      "ack",
  "order_id":  "string",
  "timestamp": 1234567890
}
```

### Fill

Sent when an order is fully or partially matched. May arrive immediately after ack (if liquidity exists) or later (when a resting limit order is matched by a future order). Multiple fill messages may be sent for a single order if it matches against multiple resting orders.

```json
{
  "type":       "fill",
  "order_id":   "string",
  "filled_qty": 5,
  "fill_price": 100.0,
  "remaining":  5,
  "timestamp":  1234567890
}
```

- `remaining` is 0 if the order is fully filled
- `fill_price` is the price of the resting order that was matched (price-time priority)

### Cancel Ack

Sent when a cancel request is successfully processed.

```json
{
  "type":      "cancel_ack",
  "order_id":  "string",
  "timestamp": 1234567890
}
```

### Reject

Sent instead of ack when the order is malformed or violates rules. A reject is always terminal — the order will never fill after a reject.

```json
{
  "type":      "reject",
  "order_id":  "string",
  "reason":    "invalid_order" | "invalid_quantity" | "invalid_price" | "duplicate_order_id" | "no_liquidity" | "unknown_order" | "unknown_type",
  "timestamp": 1234567890
}
```

| Reason | Trigger |
|--------|---------|
| `invalid_order` | Missing `order_id`, invalid `side`, or invalid `order_type` |
| `invalid_quantity` | `quantity <= 0` |
| `invalid_price` | Limit order with `price <= 0` |
| `duplicate_order_id` | `order_id` already exists in the orderbook |
| `no_liquidity` | Market order could not fully fill (no opposing liquidity) |
| `unknown_order` | Cancel references a non-existent or already-filled order |
| `unknown_type` | Message `type` is neither `"order"` nor `"cancel"` |

## Matching Rules

- **Price-time priority**: Orders are matched first by best price, then by earliest entry time
- **Bids**: Higher price is better; at equal price, earlier entry wins
- **Asks**: Lower price is better; at equal price, earlier entry wins
- **Limit orders**: Rest on the book if not fully filled; match only at or better than their price
- **Market orders**: Must fill immediately and completely; reject any unfilled remainder with `no_liquidity`
- **Fills**: Occur at the resting order's price (the incoming order's price does not affect fill price)

## Connection Lifecycle

- **On connect**: Session is created with a unique ID
- **On disconnect**: All resting orders for that session are cancelled from the orderbook
- **On invalid message type**: Reject sent with `unknown_type`; connection stays alive
- **On WebSocket close**: Connection terminated, session orders cancelled

## Scoring Dimensions Mapped to Messages

| Dimension | Measurement |
|-----------|-------------|
| Ack latency | Time from order frame received to ack frame sent (p50/p90/p99) |
| Fill latency | Time from order frame received to fill frame sent (p50/p90/p99) |
| Throughput | Max sustained orders/sec before rejects or connection drops |
| Correctness | `GET /orderbook` asserted against expected state after deterministic order sequences |
