# CrashPlatform

First production-shaped increment for a generic Crash game backend.

## Services

- `RngService`: gRPC service that generates round entropy, calculates a crash point, and stores the RNG result in MySQL.
- `GameEngine`: thin round engine API that starts a round and calls `RngService` over gRPC.
- `mysql`: local MySQL database for `rng_round_entropy`.

## Run

```bash
docker compose up --build
```

Start one round:

```bash
curl -X POST http://localhost:5002/rounds/start \
  -H "Content-Type: application/json" \
  -d '{"tableId":"table-1"}'
```

## Current Scope

This slice only creates round entropy. It does not yet include player betting, cash-out, wallet settlement, SignalR fanout, table ownership, or multi-instance fencing.
