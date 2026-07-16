import * as signalR from "@microsoft/signalr";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

const config = {
  gatewayUrl: process.env.GATEWAY_URL ?? "http://localhost:5003",
  players: positiveInteger("PLAYERS", 200),
  connectConcurrency: positiveInteger("CONNECT_CONCURRENCY", 10),
  betAmount: positiveNumber("BET_AMOUNT", 5),
  currency: process.env.CURRENCY ?? "USD",
  autoCashoutAt: positiveNumber("AUTO_CASHOUT_AT", 2),
  responseTimeoutMs: positiveInteger("BET_RESPONSE_TIMEOUT_MS", 10_000),
  reportIntervalMs: positiveInteger("REPORT_INTERVAL_MS", 5_000),
  usernamePrefix: process.env.USERNAME_PREFIX ?? `load-${Date.now()}`,
};

const metrics = {
  loginSucceeded: 0,
  loginFailed: 0,
  connected: 0,
  peakConnected: 0,
  disconnected: 0,
  reconnecting: 0,
  reconnected: 0,
  betsSent: 0,
  nonBettableRoundSnapshotsSkipped: 0,
  betInvokeFailed: 0,
  betsAccepted: 0,
  betsRejected: 0,
  betsTimedOut: 0,
  duplicateRoundEvents: 0,
  acceptanceLatenciesMs: [],
  rejectionLatenciesMs: [],
  rejectionCodes: new Map(),
};

const clients = [];
const tables = new Map();
const observedRounds = new Set();
let stopping = false;
let reportTimer;

console.log("Crash SignalR load test");
console.log(JSON.stringify(config, null, 2));

await runPool(
  Array.from({ length: config.players }, (_, index) => index + 1),
  config.connectConcurrency,
  connectPlayer,
);

console.log(`\nConnection ramp complete: ${metrics.connected}/${config.players} currently connected.`);
printTableDistribution();
if (tables.size > 1) {
  console.warn("WARNING: players span multiple tables. Increase table capacity if the test must target one table.");
}

reportTimer = setInterval(() => printReport(false), config.reportIntervalMs);
reportTimer.unref();
console.log("Waiting for rounds. Every connected player will bet once on every NewRoundInfo event.");
console.log("Press Ctrl+C to stop and print the final report.\n");

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Keep the process alive while SignalR clients receive round events.
await new Promise(() => {});

async function connectPlayer(index) {
  const username = `${config.usernamePrefix}-${index}`;
  let session;

  try {
    const response = await fetch(`${config.gatewayUrl}/api/players/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username }),
    });

    if (!response.ok) {
      throw new Error(`login returned ${response.status}: ${await response.text()}`);
    }

    session = await response.json();
    metrics.loginSucceeded++;
  } catch (error) {
    metrics.loginFailed++;
    console.error(`[${username}] ${error.message}`);
    return;
  }

  const player = {
    username,
    tableId: String(session.player.tableId),
    connection: null,
    connected: false,
    seenRounds: new Set(),
    pendingBets: new Map(),
  };

  const connection = new signalR.HubConnectionBuilder()
    .withUrl(session.hubUrl, { accessTokenFactory: () => session.token })
    .withAutomaticReconnect([0, 1_000, 2_000, 5_000, 10_000])
    .configureLogging(signalR.LogLevel.Error)
    .build();

  player.connection = connection;
  registerHandlers(player);

  connection.onreconnecting(() => {
    metrics.reconnecting++;
    markDisconnected(player);
  });

  connection.onreconnected(() => {
    metrics.reconnected++;
    markConnected(player);
  });

  connection.onclose(() => markDisconnected(player));

  try {
    await connection.start();
    clients.push(player);
    markConnected(player);
    tables.set(player.tableId, (tables.get(player.tableId) ?? 0) + 1);
  } catch (error) {
    metrics.loginFailed++;
    console.error(`[${username}] SignalR connection failed: ${error.message}`);
  }
}

function registerHandlers(player) {
  player.connection.on("NewRoundInfo", round => placeRoundBet(player, round));

  player.connection.on("PlayerBetAccepted", response => {
    completeBet(player, response, true);
  });

  player.connection.on("PlayerBetRejected", response => {
    completeBet(player, response, false);
  });
}

async function placeRoundBet(player, round) {
  const roundId = String(readField(round, "roundId", "RoundId"));
  const tableId = String(readField(round, "tableId", "TableId") ?? player.tableId);
  const roundKey = `${tableId}:${roundId}`;
  const startsAtValue = readField(round, "startsAt", "StartsAt");
  const startsAtMs = startsAtValue == null ? Number.NaN : Date.parse(startsAtValue);

  observedRounds.add(roundKey);
  if (player.seenRounds.has(roundKey)) {
    metrics.duplicateRoundEvents++;
    return;
  }
  player.seenRounds.add(roundKey);

  // The backend also sends NewRoundInfo as a state snapshot when a player joins.
  // Do not treat an already-running round as a newly opened betting window.
  if (!Number.isFinite(startsAtMs) || startsAtMs <= Date.now()) {
    metrics.nonBettableRoundSnapshotsSkipped++;
    return;
  }

  const correlationId = randomUUID();
  const startedAt = performance.now();
  const timeout = setTimeout(() => {
    if (player.pendingBets.delete(correlationId)) {
      metrics.betsTimedOut++;
    }
  }, config.responseTimeoutMs);

  player.pendingBets.set(correlationId, { startedAt, timeout, roundKey });
  metrics.betsSent++;

  try {
    await player.connection.invoke("Bet", {
      correlationId,
      roundId,
      amount: config.betAmount,
      currency: config.currency,
      autoCashoutEnabled: true,
      autoCashoutAt: config.autoCashoutAt,
    });
  } catch (error) {
    const pending = player.pendingBets.get(correlationId);
    if (pending) {
      clearTimeout(pending.timeout);
      player.pendingBets.delete(correlationId);
      metrics.betInvokeFailed++;
    }
    // A request that already timed out owns the terminal outcome. Closing connections during
    // shutdown must not count the same request again as an invocation failure.
    if (!stopping && pending) {
      console.error(`[${player.username}] Bet invoke failed for round ${roundId}: ${error.message}`);
    }
  }
}

function completeBet(player, response, accepted) {
  const messageId = String(readField(response, "messageId", "MessageId"));
  const pending = player.pendingBets.get(messageId);
  if (!pending) return;

  clearTimeout(pending.timeout);
  player.pendingBets.delete(messageId);
  const latency = performance.now() - pending.startedAt;

  if (accepted) {
    metrics.betsAccepted++;
    metrics.acceptanceLatenciesMs.push(latency);
  } else {
    metrics.betsRejected++;
    metrics.rejectionLatenciesMs.push(latency);
    const code = String(readField(response, "code", "Code") ?? "UNKNOWN");
    metrics.rejectionCodes.set(code, (metrics.rejectionCodes.get(code) ?? 0) + 1);
  }
}

function markConnected(player) {
  if (player.connected) return;
  player.connected = true;
  metrics.connected++;
  metrics.peakConnected = Math.max(metrics.peakConnected, metrics.connected);
}

function markDisconnected(player) {
  if (!player.connected) return;
  player.connected = false;
  metrics.connected--;
  metrics.disconnected++;
}

function printReport(final) {
  const pending = clients.reduce((total, player) => total + player.pendingBets.size, 0);
  const latency = summarize(metrics.acceptanceLatenciesMs);
  const label = final ? "FINAL" : new Date().toISOString();

  console.log(
    `[${label}] connected=${metrics.connected}/${config.players} peak=${metrics.peakConnected}` +
    ` tables=${tables.size} rounds=${observedRounds.size}` +
    ` bets sent=${metrics.betsSent} accepted=${metrics.betsAccepted}` +
    ` rejected=${metrics.betsRejected} timedOut=${metrics.betsTimedOut}` +
    ` invokeFailed=${metrics.betInvokeFailed} pending=${pending}` +
    ` closedSnapshotsSkipped=${metrics.nonBettableRoundSnapshotsSkipped}` +
    ` acceptMs avg=${format(latency.avg)} p50=${format(latency.p50)}` +
    ` p95=${format(latency.p95)} p99=${format(latency.p99)} max=${format(latency.max)}`,
  );

  if (metrics.rejectionCodes.size > 0) {
    const rejectionSummary = [...metrics.rejectionCodes.entries()]
      .sort((left, right) => right[1] - left[1])
      .map(([code, count]) => `${code}=${count}`)
      .join(", ");
    console.log(`  rejectionCodes: ${rejectionSummary}`);
  }
}

function printTableDistribution() {
  const distribution = [...tables.entries()]
    .sort(([left], [right]) => left.localeCompare(right, undefined, { numeric: true }))
    .map(([tableId, count]) => `table ${tableId}: ${count}`)
    .join(", ");
  console.log(`Table distribution: ${distribution || "none"}`);
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  clearInterval(reportTimer);
  console.log("\nStopping clients...");
  printReport(true);
  printTableDistribution();
  await Promise.allSettled(clients.map(player => player.connection.stop()));
  process.exit(0);
}

async function runPool(items, concurrency, worker) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++];
      await worker(item);
    }
  }));
}

function summarize(values) {
  if (values.length === 0) {
    return { avg: null, p50: null, p95: null, p99: null, max: null };
  }

  const sorted = [...values].sort((a, b) => a - b);
  return {
    avg: values.reduce((sum, value) => sum + value, 0) / values.length,
    p50: percentile(sorted, 0.50),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted.at(-1),
  };
}

function percentile(sorted, quantile) {
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1)];
}

function readField(value, camelCase, pascalCase) {
  return value?.[camelCase] ?? value?.[pascalCase];
}

function format(value) {
  return value == null ? "n/a" : value.toFixed(1);
}

function positiveInteger(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer.`);
  return value;
}

function positiveNumber(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number.`);
  return value;
}
