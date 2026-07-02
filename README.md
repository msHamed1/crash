# CrashPlatform

First production-shaped increment for a generic Crash game backend.

## Services

- `RngService`: gRPC service that generates round entropy, calculates a crash point, and stores the RNG result in MySQL.
- `GameEngine`: thin round engine API that starts a round and calls `RngService` over gRPC.
- `RealtimeGateway`: SignalR gateway that authenticates player connections with JWT and forwards player messages to RabbitMQ.
- `rabbitmq`: durable broker for player messages routed by table id.
- `mysql`: local MySQL database for `rng_round_entropy`.

## Run

```bash
docker compose up --build
```

Start one round:

```bash
curl -X POST http://localhost:5002/rounds/start \
  -H "Content-Type: application/json" \
  -d '{"tableId":"table-1","roundId":"round-1","clientSeed":"player-seed-1","nonce":0}'
```

Submitting the same `tableId` and `roundId` again returns the stored RNG result instead of creating a second result.

## Realtime Player Messages

Connect clients to the SignalR hub at:

```text
http://localhost:5003/hubs/player
```

The bundled browser client is served by Docker at:

```text
http://client.dev
```

For local development, add this host mapping once:

```text
127.0.0.1 client.dev
```

The connection must include a JWT in `access_token` or `Authorization: Bearer <token>`. For local development the signing key is:

```text
local-dev-signalr-jwt-signing-key-change-me
```

The JWT payload must include:

```json
{
  "table_id": "table-1",
  "player_id": "player-1",
  "exp": 4102444800
}
```

Send player actions through the `SendMessage` hub method:

```json
{
  "type": "bet",
  "amount": 10,
  "currency": "USD",
  "autoCashOut": 2
}
```

`RealtimeGateway` publishes the message to RabbitMQ with the table id as the routing key and ensures a durable table queue exists. Each `GameEngine` instance consumes only the table queues configured in `GameEngine:TableIds`, then logs the messages it receives.

## Current Scope

This slice only creates round entropy. It does not yet include player betting, cash-out, wallet settlement, SignalR fanout, table ownership, or multi-instance fencing.
