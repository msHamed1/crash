# SignalR player load test

This test creates real player records through the login API, opens one authenticated
SignalR connection per player, and places one bet per player whenever that player's
table publishes `NewRoundInfo`.

Acceptance latency is end-to-end: immediately before invoking `Bet` until the private
`PlayerBetAccepted` event with the matching correlation/message ID is received.

## Run

```bash
cd load-tests/signalr
npm install
npm start
```

Override settings with environment variables:

```bash
PLAYERS=500 CONNECT_CONCURRENCY=20 BET_AMOUNT=1 npm start
```

Useful variables:

- `GATEWAY_URL` (default `http://localhost:5003`)
- `PLAYERS` (default `100`)
- `CONNECT_CONCURRENCY` (default `10`)
- `BET_AMOUNT` (default `1`)
- `AUTO_CASHOUT_AT` (default `2`)
- `BET_RESPONSE_TIMEOUT_MS` (default `10000`)
- `REPORT_INTERVAL_MS` (default `5000`)
- `USERNAME_PREFIX` (default is unique for every run)

Press Ctrl+C for the final report. Besides average latency, use p95 and p99 to
understand the experience of slower players. The output also reports player-to-table
distribution because clients on different tables do not receive the same rounds.
