# Scoring

The scoring system evaluates contestant submissions across three dimensions: latency, throughput, and correctness. Each dimension produces a score in [0, 1], which are combined into a composite score.

## Two-Phase Test Protocol

### Phase 1: Correctness Validation

Runs before the load test. A single bot connects to the sandbox and executes 5 deterministic test sequences synchronously (one step at a time, waiting for responses, asserting expected behavior).

| Sequence | What It Tests | Assertions |
|----------|--------------|------------|
| `cx_basic_match` | Basic order matching at same price | Ack, fill with correct price/quantity, orderbook state |
| `cx_priority_fill` | Price-time priority (higher bid fills first) | Fill at higher price, not lower |
| `cx_partial_fill` | Partial fill handling | Partial fill quantity, remaining order on book |
| `cx_cancel` | Order cancellation correctness | Cancel ack, order removed from book, no spurious matches |
| `cx_reject_market_no_liquidity` | Market order reject on empty book | Reject with `no_liquidity` reason |

Total: 33 assertions across all sequences.

**Correctness score:** `passed_assertions / total_assertions`, clamped to [0, 1].

### Phase 2: Load Test

Runs after correctness validation. N bot goroutines (default: 50, configured by bot-orchestrator) each maintain a persistent WebSocket connection and continuously send order sequences for a fixed duration (default: 60s).

Each bot is assigned one of three sequence types:
- `basic_match`: buy + sell at same price (immediate fill)
- `partial_fill_ladder`: 3 bids at descending prices + market sweep
- `cancel_correctness`: place + cancel cycles

Bots use unique price bands (`(bot_id + 1) * 1000`) to prevent cross-bot interference.

## Composite Score Formula

```
composite = (latency_score * 0.35) + (throughput_score * 0.35) + (correctness_score * 0.30)
```

Weights: **Latency 35% | Throughput 35% | Correctness 30%**

### Latency Score

Weighted combination of acknowledgment latency percentiles:

```
weighted_latency_us = (ack_p50_us * 0.2) + (ack_p90_us * 0.3) + (ack_p99_us * 0.5)
latency_score = 1.0 - (weighted_latency_us / MAX_LATENCY_US)
```

Clamped to [0, 1].

- p99 carries the most weight (0.5) because tail latency is the primary discriminator under load
- p90 weight: 0.3
- p50 weight: 0.2
- `MAX_LATENCY_US` default: 50000 (50ms)

### Throughput Score

```
throughput_score = tps / MAX_TPS
```

Clamped to [0, 1].

- `tps` = `orders_sent / duration_seconds` (measured during Phase 2 load test)
- `MAX_TPS` default: 1000

### Correctness Score

```
correctness_score = Phase 1 passed_assertions / total_assertions
```

Clamped to [0, 1].

- Derived from the 5 deterministic correctness sequences
- Each sequence tests specific orderbook behavior (matching, priority, partial fills, cancellation, rejection)
- Total: 33 assertions across all sequences

## Latency Measurement

### Ack Latency

Measured from the moment an order frame is sent by the bot to when the ack frame is received. This is the primary latency metric used in scoring.

Percentiles computed via hdrhistogram (range: 1µs to 60s, 3 significant figures):
- p50, p90, p99, p99.9, max

### Fill Latency

Measured from the moment an order frame is sent to when a fill frame is received. Collected for informational purposes but not used in the scoring formula.

Percentiles computed via hdrhistogram (same range as ack latency).

## Throughput Calculation

```
tps = total_orders_sent / elapsed_duration_seconds
```

- `total_orders_sent`: sum of orders sent across all bot goroutines during Phase 2
- `elapsed_duration_seconds`: wall-clock time from warmup completion to all bots finishing
- Backpressure enforced: max 20 in-flight orders per bot

## Redis Leaderboard Mechanics

### Sorted Set

| Property | Value |
|----------|-------|
| Key | `leaderboard` |
| Type | Sorted Set |
| Score | `composite_score * 1000` (integer for precision) |
| Member | `"{submission_id}:{team_name}"` |

Higher score = better rank. `ZREVRANGE` returns members in descending score order.

### Detail Hash

| Property | Value |
|----------|-------|
| Key | `leaderboard_details` |
| Type | Hash |
| Field | `submission_id` |
| Value | JSON payload with full metrics |

### Pub/Sub Channel

| Property | Value |
|----------|-------|
| Channel | `leaderboard_updates` |
| Publisher | telemetry-ingester (on each `bot.metrics` event) |
| Subscriber | leaderboard-ws (fans out to WebSocket clients) |

### Update Flow

1. telemetry-ingester receives `bot.metrics` event
2. Computes composite score
3. `ZADD leaderboard <score * 1000> "{submission_id}:{team_name}"`
4. `HSET leaderboard_details {submission_id} <JSON entry>`
5. `PUBLISH leaderboard_updates <JSON entry>`
6. leaderboard-ws receives pub/sub message, broadcasts to all connected WebSocket clients

## TimescaleDB Tables

### `telemetry_events`

One row per `bot.metrics` event received.

| Column | Type |
|--------|------|
| `time` | timestamptz |
| `submission_id` | text |
| `bot_id` | text |
| `event_type` | text |
| `latency_us` | double precision |
| `order_id` | text |

### `submission_scores`

One row per submission. Upserted on each event (ON CONFLICT DO UPDATE — last write wins).

| Column | Type |
|--------|------|
| `submission_id` | text (primary key) |
| `team_name` | text |
| `p50_us` | double precision |
| `p90_us` | double precision |
| `p99_us` | double precision |
| `tps` | double precision |
| `correctness` | double precision |
| `composite` | double precision |
| `scored_at` | timestamptz |

## TODO

- Redis ZADD member format `"{submission_id}:{team_name}"` causes collisions on team rename (BUG.md #108)
- `TIMESCALEDB_DSN` default contains plaintext password (BUG.md reference in telemetry-ingester.md)
