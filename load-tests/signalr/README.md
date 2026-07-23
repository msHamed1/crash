# Crash SignalR load tests

The default runner exercises the public player flow through the real login API,
authenticated SignalR connections, RabbitMQ-backed game commands, round events,
bet decisions, and cashout notifications.

It distinguishes the two placement stages used by the backend:

- `PlayerBetAccepted` confirms that GameEngine reserved the stake in memory and
  returned a bet whose status should be `Placed`.
- DB acceptance later changes the runtime bet to `Accepted`, but that transition
  is intentionally silent.
- A permanent DB rejection compensates the provisional acceptance with
  `PlayerBetRejected`; the runner checks that the balance is refunded.

## Scenarios

Players are deterministically divided using `SCENARIO_MIX`.

| Scenario | Behavior | Main assertions |
| --- | --- | --- |
| `auto` | Places a bet with auto cashout enabled. | Target crossing, private cashout completion, payout and balance. |
| `manual` | Places a bet and invokes `CashOut` after a configured tick. | Request-to-result latency, payout and balance. |
| `loss` | Places without cashout. | Public crash/loss observation. DB loss completion is not externally observable yet. |
| `reject` | Sends a stake larger than the available balance. | Private rejection, correlation, and unchanged balance. |
| `duplicate` | Replays the exact correlation ID and payload. | Idempotent replay without a second reservation. It otherwise follows auto cashout. |

The default mix is:

```text
auto:40,manual:25,loss:15,reject:10,duplicate:10
```

## Run

```bash
cd load-tests/signalr
npm install
npm start
```

The comprehensive run is bounded to 120 seconds by default, then stops creating
new work, drains pending responses, prints the result, and exits non-zero when
strict assertions fail.

Example:

```bash
PLAYERS=500 \
CONNECT_CONCURRENCY=25 \
TEST_DURATION_SECONDS=300 \
ROUNDS_PER_PLAYER=5 \
RECONNECT_PERCENT=10 \
REPORT_JSON_PATH=./report.json \
npm start
```

The original placement-only runner remains available:

```bash
npm run start:basic
```

Run JavaScript syntax validation without connecting to services:

```bash
npm run check
```

## Configuration

### Workload

- `GATEWAY_URL` — default `http://localhost:5003`.
- `PLAYERS` — default `100`.
- `CONNECT_CONCURRENCY` — default `10`.
- `TEST_DURATION_SECONDS` — default `120`.
- `ROUNDS_PER_PLAYER` — default `0`, meaning unlimited until duration ends.
- `SCENARIO_MIX` — comma-separated `scenario:weight` entries.
- `USERNAME_PREFIX` — unique by default. Reuse it only when intentionally
  testing existing player balances.

### Betting

- `BET_AMOUNT` — default `5`.
- `REJECTION_BET_AMOUNT` — default `1000000`.
- `CURRENCY` — default `USD`.
- `AUTO_CASHOUT_AT` — default `2`.
- `MANUAL_CASHOUT_AT` — default `1.5`.

### Timeouts and reconnect

- `LOGIN_TIMEOUT_MS` — default `10000`.
- `BET_RESPONSE_TIMEOUT_MS` — default `10000`.
- `CASHOUT_RESPONSE_TIMEOUT_MS` — default `15000`.
- `DRAIN_TIMEOUT_MS` — default `10000`.
- `REPORT_INTERVAL_MS` — default `5000`.
- `RECONNECT_PERCENT` — default `0`.
- `RECONNECT_AFTER_MS` — default `30000`.
- `RECONNECT_DOWNTIME_MS` — default `1000`.

### Pass/fail thresholds

- `FAIL_ON_ERRORS` — default `true`.
- `MIN_BET_RESPONSE_RATE` — default `0.99`.
- `MIN_CASHOUT_SUCCESS_RATE` — default `0.95`.
- `MAX_PROVISIONAL_P95_MS` — default `0`, disabled.
- `MAX_CASHOUT_P95_MS` — default `0`, disabled.
- `REPORT_JSON_PATH` — optional JSON artifact path.

## Reported measurements

- Login, connection, provisional placement, rejection, duplicate replay,
  manual cashout, and auto-cashout latency distributions.
- Average, p50, p95, p99, and maximum latency.
- Connection peak, disconnects, automatic reconnects, and controlled reconnects.
- Unique rounds/ticks/crashes and per-client duplicate deliveries.
- Tick ordering and multiplier-regression violations.
- Expected versus unexpected rejection and cashout outcomes.
- Timeouts, unmatched responses, compensating DB rejections, and pending work.
- Stake reservation, refund, payout, profit/loss, and updated-balance invariants.
- Player-to-table and scenario distribution.

## Observability boundary

This test is comprehensive for the currently exposed realtime contract, but it
cannot prove every persistence invariant:

- Successful DB acceptance emits no second event.
- Losing-bet DB settlement emits no private completion event.
- There is no read-only bet/round snapshot endpoint for post-run reconciliation.

Therefore `PlayerBetAccepted` latency measures the in-memory decision and client
notification path—not DB commit latency. A production-grade financial load test
should additionally expose an authenticated read-only test snapshot or run a
separate DB reconciliation job that verifies one accepted row and exactly one
terminal result per `BetId`.
