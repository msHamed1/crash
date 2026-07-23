import * as signalR from "@microsoft/signalr";
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";

const supportedScenarios = new Set(["auto", "manual", "loss", "reject", "duplicate"]);

const config = {
  gatewayUrl: process.env.GATEWAY_URL ?? "http://localhost:5003",
  players: positiveInteger("PLAYERS", 100),
  connectConcurrency: positiveInteger("CONNECT_CONCURRENCY", 10),
  betAmount: positiveNumber("BET_AMOUNT", 5),
  rejectionBetAmount: positiveNumber("REJECTION_BET_AMOUNT", 1_000_000),
  currency: process.env.CURRENCY ?? "USD",
  autoCashoutAt: multiplier("AUTO_CASHOUT_AT", 2),
  manualCashoutAt: multiplier("MANUAL_CASHOUT_AT", 1.5),
  scenarioMix: scenarioMix(process.env.SCENARIO_MIX ?? "auto:40,manual:25,loss:15,reject:10,duplicate:10"),
  roundsPerPlayer: nonNegativeInteger("ROUNDS_PER_PLAYER", 0),
  testDurationMs: positiveInteger("TEST_DURATION_SECONDS", 120) * 1_000,
  loginTimeoutMs: positiveInteger("LOGIN_TIMEOUT_MS", 10_000),
  betResponseTimeoutMs: positiveInteger("BET_RESPONSE_TIMEOUT_MS", 10_000),
  cashoutResponseTimeoutMs: positiveInteger("CASHOUT_RESPONSE_TIMEOUT_MS", 15_000),
  drainTimeoutMs: positiveInteger("DRAIN_TIMEOUT_MS", 10_000),
  reportIntervalMs: positiveInteger("REPORT_INTERVAL_MS", 5_000),
  reconnectPercent: percentage("RECONNECT_PERCENT", 0),
  reconnectAfterMs: positiveInteger("RECONNECT_AFTER_MS", 30_000),
  reconnectDowntimeMs: positiveInteger("RECONNECT_DOWNTIME_MS", 1_000),
  minBetResponseRate: ratio("MIN_BET_RESPONSE_RATE", 0.99),
  minCashoutSuccessRate: ratio("MIN_CASHOUT_SUCCESS_RATE", 0.95),
  maxProvisionalP95Ms: nonNegativeNumber("MAX_PROVISIONAL_P95_MS", 0),
  maxCashoutP95Ms: nonNegativeNumber("MAX_CASHOUT_P95_MS", 0),
  failOnErrors: booleanValue("FAIL_ON_ERRORS", true),
  reportJsonPath: process.env.REPORT_JSON_PATH?.trim() || null,
  usernamePrefix: process.env.USERNAME_PREFIX ?? `load-${Date.now()}`,
};

const metrics = {
  loginSucceeded: 0,
  loginFailed: 0,
  connectionSucceeded: 0,
  connectionFailed: 0,
  connected: 0,
  peakConnected: 0,
  disconnected: 0,
  reconnecting: 0,
  reconnected: 0,
  controlledReconnectAttempts: 0,
  controlledReconnectSucceeded: 0,
  controlledReconnectFailed: 0,

  newRoundDeliveries: 0,
  uniqueRounds: 0,
  tickDeliveries: 0,
  uniqueTicks: 0,
  crashDeliveries: 0,
  uniqueCrashes: 0,
  currentStateDeliveries: 0,
  duplicateDeliveries: 0,
  nonBettableRoundSnapshotsSkipped: 0,
  outOfOrderTicks: 0,
  multiplierRegressions: 0,

  betsSent: 0,
  betInvokeFailed: 0,
  provisionalAccepted: 0,
  initialRejected: 0,
  expectedRejections: 0,
  unexpectedRejections: 0,
  unexpectedAcceptances: 0,
  compensatedRejections: 0,
  betResponseTimedOut: 0,
  unmatchedBetResponses: 0,
  invalidPlacedStatuses: 0,

  duplicateRetriesSent: 0,
  duplicateRetryInvokeFailed: 0,
  duplicateRetriesAccepted: 0,
  duplicateRetriesRejected: 0,
  duplicateRetriesTimedOut: 0,

  manualCashoutsSent: 0,
  manualCashoutInvokeFailed: 0,
  autoCashoutTargetsReached: 0,
  cashoutsCompleted: 0,
  manualCashoutsCompleted: 0,
  autoCashoutsCompleted: 0,
  cashoutsTimedOut: 0,
  unmatchedCashouts: 0,
  unexpectedCashouts: 0,
  crashLossesObserved: 0,
  crashedBeforeCashoutTarget: 0,
  cashoutPendingAtCrash: 0,

  balanceInvariantFailures: 0,
  payoutInvariantFailures: 0,
  contractViolations: 0,

  loginLatenciesMs: [],
  connectionLatenciesMs: [],
  provisionalLatenciesMs: [],
  rejectionLatenciesMs: [],
  duplicateRetryLatenciesMs: [],
  manualCashoutLatenciesMs: [],
  autoCashoutLatenciesMs: [],
  rejectionCodes: new Map(),
  scenarioAssignments: new Map(),
};

const clients = [];
const tables = new Map();
const observedRounds = new Set();
const observedTickMessages = new Set();
const observedCrashMessages = new Set();
let stopping = false;
let stopReason;
let reportTimer;
let durationTimer;
let resolveStop;
const stopSignal = new Promise(resolve => {
  resolveStop = resolve;
});

process.on("SIGINT", () => requestStop("SIGINT"));
process.on("SIGTERM", () => requestStop("SIGTERM"));

await main();

async function main() {
  console.log("Crash SignalR comprehensive load test");
  console.log(JSON.stringify(serializableConfig(), null, 2));

  await runPool(
    Array.from({ length: config.players }, (_, index) => index + 1),
    config.connectConcurrency,
    connectPlayer,
  );

  console.log(`\nConnection ramp complete: ${metrics.connected}/${config.players} currently connected.`);
  printTableDistribution();
  printScenarioDistribution();

  if (clients.length === 0) {
    requestStop("no clients connected");
  } else {
    reportTimer = setInterval(() => printReport(false), config.reportIntervalMs);
    reportTimer.unref();

    durationTimer = setTimeout(
      () => requestStop(`duration ${config.testDurationMs / 1_000}s reached`),
      config.testDurationMs,
    );
    durationTimer.unref();

    console.log(
      "Waiting for rounds. The configured scenario mix will run on every bettable NewRoundInfo event.",
    );
    console.log("Press Ctrl+C to stop, drain pending outcomes, and print the final report.\n");
  }

  const reason = await stopSignal;
  await shutdown(reason);
}

async function connectPlayer(index) {
  if (stopping) return;

  const username = `${config.usernamePrefix}-${index}`;
  let session;
  const loginStartedAt = performance.now();

  try {
    const response = await fetch(`${config.gatewayUrl}/api/players/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username }),
      signal: AbortSignal.timeout(config.loginTimeoutMs),
    });

    if (!response.ok) {
      throw new Error(`login returned ${response.status}: ${await response.text()}`);
    }

    session = await response.json();
    metrics.loginSucceeded++;
    metrics.loginLatenciesMs.push(performance.now() - loginStartedAt);
  } catch (error) {
    metrics.loginFailed++;
    console.error(`[${username}] login failed: ${error.message}`);
    return;
  }

  const sessionPlayer = readField(session, "player", "Player");
  const playerId = String(readField(sessionPlayer, "id", "Id"));
  const tableId = String(readField(sessionPlayer, "tableId", "TableId"));
  const initialBalance = Number(readField(sessionPlayer, "balanceInUSD", "BalanceInUSD"));
  const token = readField(session, "token", "Token");
  const hubUrl = readField(session, "hubUrl", "HubUrl");

  if (!playerId || !tableId || !token || !hubUrl || !Number.isFinite(initialBalance)) {
    metrics.connectionFailed++;
    console.error(`[${username}] login response is missing player, table, balance, token, or hub URL.`);
    return;
  }

  const scenario = chooseScenario(index);
  incrementMap(metrics.scenarioAssignments, scenario);

  const player = {
    index,
    username,
    playerId,
    tableId,
    scenario,
    balance: initialBalance,
    connection: null,
    connected: false,
    roundsAttempted: 0,
    seenMessages: new Set(),
    seenRounds: new Set(),
    roundStates: new Map(),
    pendingBets: new Map(),
    bets: new Map(),
    rejectedBetIds: new Set(),
    activeBetByRound: new Map(),
    reconnectTimer: null,
  };

  const connection = new signalR.HubConnectionBuilder()
    .withUrl(hubUrl, { accessTokenFactory: () => token })
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

  const connectionStartedAt = performance.now();
  try {
    await connection.start();
    metrics.connectionSucceeded++;
    metrics.connectionLatenciesMs.push(performance.now() - connectionStartedAt);
    clients.push(player);
    markConnected(player);
    tables.set(player.tableId, (tables.get(player.tableId) ?? 0) + 1);
    scheduleControlledReconnect(player);
  } catch (error) {
    metrics.connectionFailed++;
    console.error(`[${username}] SignalR connection failed: ${error.message}`);
  }
}

function registerHandlers(player) {
  player.connection.on("NewRoundInfo", message => handleNewRound(player, message));
  player.connection.on("RoundTick", message => handleRoundTick(player, message));
  player.connection.on("RoundCrashed", message => handleRoundCrashed(player, message));
  player.connection.on("CurrentState", message => {
    metrics.currentStateDeliveries++;
    recordMessage(player, "CurrentState", message);
  });
  player.connection.on("PlayerBetAccepted", response => handleBetAccepted(player, response));
  player.connection.on("PlayerBetRejected", response => handleBetRejected(player, response));
  player.connection.on("BetCashedOut", response => handleBetCashedOut(player, response));
}

async function handleNewRound(player, round) {
  metrics.newRoundDeliveries++;
  if (!recordMessage(player, "NewRoundInfo", round)) return;

  const roundId = stringField(round, "roundId", "RoundId");
  const tableId = stringField(round, "tableId", "TableId") ?? player.tableId;
  const startsAtValue = readField(round, "startsAt", "StartsAt");
  const startsAtMs = startsAtValue == null ? Number.NaN : Date.parse(startsAtValue);

  if (!roundId) {
    metrics.contractViolations++;
    return;
  }

  const roundKey = `${tableId}:${roundId}`;
  if (!observedRounds.has(roundKey)) {
    observedRounds.add(roundKey);
    metrics.uniqueRounds = observedRounds.size;
  }

  if (player.seenRounds.has(roundKey)) {
    metrics.duplicateDeliveries++;
    return;
  }
  player.seenRounds.add(roundKey);
  player.roundStates.set(roundKey, {
    roundId,
    tableId,
    startsAtMs,
    lastTickSequence: null,
    lastMultiplier: 1,
    crashed: false,
  });

  if (stopping) return;
  if (!Number.isFinite(startsAtMs) || startsAtMs <= Date.now()) {
    metrics.nonBettableRoundSnapshotsSkipped++;
    return;
  }
  if (config.roundsPerPlayer > 0 && player.roundsAttempted >= config.roundsPerPlayer) {
    return;
  }

  player.roundsAttempted++;
  await placeRoundBet(player, roundId, roundKey);
}

function handleRoundTick(player, tick) {
  metrics.tickDeliveries++;
  if (!recordMessage(player, "RoundTick", tick)) return;

  const messageId = stringField(tick, "messageId", "MessageId");
  if (messageId) {
    observedTickMessages.add(messageId);
    metrics.uniqueTicks = observedTickMessages.size;
  }

  const roundId = stringField(tick, "roundId", "RoundId");
  const tableId = stringField(tick, "tableId", "TableId") ?? player.tableId;
  const roundKey = `${tableId}:${roundId}`;
  const multiplierValue = Number(readField(tick, "currentMultiplier", "CurrentMultiplier"));
  const tickSequence = Number(readField(tick, "tickSequence", "TickSequence"));
  const state = player.roundStates.get(roundKey) ?? {
    roundId,
    tableId,
    startsAtMs: Number.NaN,
    lastTickSequence: null,
    lastMultiplier: 1,
    crashed: false,
  };

  if (!roundId || !Number.isFinite(multiplierValue) || !Number.isFinite(tickSequence)) {
    metrics.contractViolations++;
    return;
  }

  if (state.lastTickSequence != null && tickSequence <= state.lastTickSequence) {
    metrics.outOfOrderTicks++;
  }
  if (state.lastMultiplier != null && multiplierValue < state.lastMultiplier) {
    metrics.multiplierRegressions++;
  }

  state.lastTickSequence = tickSequence;
  state.lastMultiplier = multiplierValue;
  player.roundStates.set(roundKey, state);

  const betId = player.activeBetByRound.get(roundKey);
  const bet = betId ? player.bets.get(betId) : null;
  if (!bet || isTerminalBet(bet)) return;

  if (bet.scenario === "manual" && multiplierValue >= config.manualCashoutAt) {
    void requestManualCashout(player, bet);
  }

  if ((bet.scenario === "auto" || bet.scenario === "duplicate") &&
      multiplierValue >= config.autoCashoutAt &&
      bet.autoTargetReachedAt == null) {
    bet.autoTargetReachedAt = performance.now();
    metrics.autoCashoutTargetsReached++;
    startCashoutTimeout(player, bet);
  }
}

function handleRoundCrashed(player, crash) {
  metrics.crashDeliveries++;
  if (!recordMessage(player, "RoundCrashed", crash)) return;

  const messageId = stringField(crash, "messageId", "MessageId");
  if (messageId) {
    observedCrashMessages.add(messageId);
    metrics.uniqueCrashes = observedCrashMessages.size;
  }

  const roundId = stringField(crash, "roundId", "RoundId");
  const tableId = stringField(crash, "tableId", "TableId") ?? player.tableId;
  const roundKey = `${tableId}:${roundId}`;
  const crashMultiplier = Number(readField(crash, "currentMultiplier", "CurrentMultiplier"));
  const state = player.roundStates.get(roundKey);
  if (state) state.crashed = true;

  const betId = player.activeBetByRound.get(roundKey);
  const bet = betId ? player.bets.get(betId) : null;
  if (!bet || isTerminalBet(bet)) return;

  bet.crashObservedAt = performance.now();
  bet.crashMultiplier = crashMultiplier;

  const target = bet.scenario === "manual"
    ? config.manualCashoutAt
    : config.autoCashoutAt;
  const shouldHaveRequestedCashout =
    (bet.scenario === "manual" && bet.manualCashoutRequestedAt != null) ||
    ((bet.scenario === "auto" || bet.scenario === "duplicate") &&
      bet.autoTargetReachedAt != null);

  if (shouldHaveRequestedCashout) {
    metrics.cashoutPendingAtCrash++;
    return;
  }

  if (bet.scenario === "loss" || Number.isFinite(crashMultiplier) && crashMultiplier < target) {
    bet.status = "loss-observed";
    metrics.crashLossesObserved++;
    if (bet.scenario !== "loss") metrics.crashedBeforeCashoutTarget++;
  }
}

async function placeRoundBet(player, roundId, roundKey) {
  const correlationId = randomUUID();
  const scenario = player.scenario;
  const amount = scenario === "reject" ? config.rejectionBetAmount : config.betAmount;
  const autoCashoutEnabled = scenario === "auto" || scenario === "duplicate";
  const request = {
    correlationId,
    roundId,
    amount,
    currency: config.currency,
    autoCashoutEnabled,
    autoCashoutAt: autoCashoutEnabled ? config.autoCashoutAt : null,
  };
  const pending = {
    correlationId,
    roundId,
    roundKey,
    scenario,
    request,
    preBetBalance: player.balance,
    startedAt: performance.now(),
    timeout: null,
  };

  pending.timeout = setTimeout(() => {
    if (player.pendingBets.delete(correlationId)) {
      metrics.betResponseTimedOut++;
    }
  }, config.betResponseTimeoutMs);

  player.pendingBets.set(correlationId, pending);
  metrics.betsSent++;

  try {
    await player.connection.invoke("Bet", request);
  } catch (error) {
    const activePending = player.pendingBets.get(correlationId);
    if (activePending) {
      clearTimeout(activePending.timeout);
      player.pendingBets.delete(correlationId);
      metrics.betInvokeFailed++;
    }
    if (!stopping && activePending) {
      console.error(`[${player.username}] Bet invoke failed for round ${roundId}: ${error.message}`);
    }
  }
}

function handleBetAccepted(player, response) {
  const messageId = stringField(response, "messageId", "MessageId");
  if (!messageId) {
    metrics.contractViolations++;
    return;
  }

  const pending = player.pendingBets.get(messageId);
  if (!pending) {
    handleDuplicateAcceptance(player, messageId);
    return;
  }

  clearTimeout(pending.timeout);
  player.pendingBets.delete(messageId);
  metrics.provisionalAccepted++;
  metrics.provisionalLatenciesMs.push(performance.now() - pending.startedAt);

  const betPayload = readField(response, "bet", "Bet");
  const betId = stringField(betPayload, "betId", "BetId");
  const roundId = stringField(betPayload, "roundId", "RoundId");
  const stakeAmount = Number(readField(betPayload, "stakeAmount", "StakeAmount"));
  const status = String(readField(betPayload, "status", "Status") ?? "").toLowerCase();
  const updatedBalance = Number(readField(response, "updatedBalance", "UpdatedBalance"));

  if (pending.scenario === "reject") {
    metrics.unexpectedAcceptances++;
  }
  if (betId !== messageId || roundId !== pending.roundId || !Number.isFinite(stakeAmount)) {
    metrics.contractViolations++;
  }
  if (status !== "placed") {
    metrics.invalidPlacedStatuses++;
  }

  const expectedReservedBalance = pending.preBetBalance - pending.request.amount;
  if (!approximatelyEqual(updatedBalance, expectedReservedBalance)) {
    metrics.balanceInvariantFailures++;
  }
  if (Number.isFinite(updatedBalance)) player.balance = updatedBalance;

  const bet = {
    betId: betId ?? messageId,
    roundId: pending.roundId,
    roundKey: pending.roundKey,
    scenario: pending.scenario,
    stakeAmount: Number.isFinite(stakeAmount) ? stakeAmount : pending.request.amount,
    preBetBalance: pending.preBetBalance,
    reservedBalance: updatedBalance,
    status: "placed",
    provisionalAcceptedAt: performance.now(),
    request: pending.request,
    duplicateRetryStartedAt: null,
    duplicateRetryTimeout: null,
    manualCashoutRequestedAt: null,
    autoTargetReachedAt: null,
    cashoutTimeout: null,
    crashObservedAt: null,
  };

  player.bets.set(bet.betId, bet);
  player.activeBetByRound.set(pending.roundKey, bet.betId);

  if (pending.scenario === "duplicate") {
    void sendDuplicateRetry(player, bet);
  }
}

function handleDuplicateAcceptance(player, messageId) {
  const bet = player.bets.get(messageId);
  if (!bet) {
    metrics.unmatchedBetResponses++;
    return;
  }
  if (bet.duplicateRetryStartedAt == null) {
    metrics.duplicateDeliveries++;
    return;
  }

  clearTimeout(bet.duplicateRetryTimeout);
  bet.duplicateRetryTimeout = null;
  metrics.duplicateRetriesAccepted++;
  metrics.duplicateRetryLatenciesMs.push(performance.now() - bet.duplicateRetryStartedAt);
  bet.duplicateRetryStartedAt = null;
}

function handleBetRejected(player, response) {
  const messageId = stringField(response, "messageId", "MessageId");
  const code = stringField(response, "code", "Code") ?? "UNKNOWN";
  const updatedBalance = Number(readField(response, "updatedBalance", "UpdatedBalance"));
  incrementMap(metrics.rejectionCodes, code);

  if (!messageId) {
    metrics.contractViolations++;
    return;
  }

  const pending = player.pendingBets.get(messageId);
  if (pending) {
    clearTimeout(pending.timeout);
    player.pendingBets.delete(messageId);
    metrics.initialRejected++;
    metrics.rejectionLatenciesMs.push(performance.now() - pending.startedAt);

    if (pending.scenario === "reject") {
      metrics.expectedRejections++;
    } else {
      metrics.unexpectedRejections++;
    }
    if (Number.isFinite(updatedBalance) &&
        !approximatelyEqual(updatedBalance, pending.preBetBalance)) {
      metrics.balanceInvariantFailures++;
    }
    if (Number.isFinite(updatedBalance)) player.balance = updatedBalance;
    player.rejectedBetIds.add(messageId);
    return;
  }

  const bet = player.bets.get(messageId);
  if (!bet) {
    if (player.rejectedBetIds.has(messageId)) {
      metrics.duplicateDeliveries++;
      return;
    }
    metrics.unmatchedBetResponses++;
    return;
  }
  if (bet.status === "rejected") {
    metrics.duplicateDeliveries++;
    return;
  }

  if (bet.duplicateRetryStartedAt != null) {
    clearTimeout(bet.duplicateRetryTimeout);
    bet.duplicateRetryTimeout = null;
    bet.duplicateRetryStartedAt = null;
    metrics.duplicateRetriesRejected++;
  }

  clearTimeout(bet.cashoutTimeout);
  bet.cashoutTimeout = null;
  bet.status = "rejected";
  player.rejectedBetIds.add(messageId);
  metrics.compensatedRejections++;

  if (Number.isFinite(updatedBalance) &&
      !approximatelyEqual(updatedBalance, bet.preBetBalance)) {
    metrics.balanceInvariantFailures++;
  }
  if (Number.isFinite(updatedBalance)) player.balance = updatedBalance;
}

async function sendDuplicateRetry(player, bet) {
  if (stopping || bet.duplicateRetryStartedAt != null) return;

  bet.duplicateRetryStartedAt = performance.now();
  bet.duplicateRetryTimeout = setTimeout(() => {
    if (bet.duplicateRetryStartedAt != null) {
      bet.duplicateRetryStartedAt = null;
      metrics.duplicateRetriesTimedOut++;
    }
  }, config.betResponseTimeoutMs);
  metrics.duplicateRetriesSent++;

  try {
    await player.connection.invoke("Bet", bet.request);
  } catch (error) {
    clearTimeout(bet.duplicateRetryTimeout);
    bet.duplicateRetryTimeout = null;
    bet.duplicateRetryStartedAt = null;
    metrics.duplicateRetryInvokeFailed++;
    if (!stopping) {
      console.error(`[${player.username}] duplicate bet retry failed: ${error.message}`);
    }
  }
}

async function requestManualCashout(player, bet) {
  if (stopping || bet.manualCashoutRequestedAt != null || isTerminalBet(bet)) return;

  bet.manualCashoutRequestedAt = performance.now();
  metrics.manualCashoutsSent++;
  startCashoutTimeout(player, bet);

  try {
    await player.connection.invoke("CashOut", {
      correlationId: randomUUID(),
      roundId: bet.roundId,
      betId: bet.betId,
    });
  } catch (error) {
    metrics.manualCashoutInvokeFailed++;
    clearTimeout(bet.cashoutTimeout);
    bet.cashoutTimeout = null;
    if (!stopping) {
      console.error(`[${player.username}] cashout invoke failed for ${bet.betId}: ${error.message}`);
    }
  }
}

function startCashoutTimeout(player, bet) {
  if (bet.cashoutTimeout != null) return;

  bet.cashoutTimeout = setTimeout(() => {
    if (!isTerminalBet(bet)) {
      metrics.cashoutsTimedOut++;
      bet.cashoutTimeout = null;
    }
  }, config.cashoutResponseTimeoutMs);
}

function handleBetCashedOut(player, response) {
  if (!recordMessage(player, "BetCashedOut", response)) return;

  const betId = stringField(response, "betId", "BetId");
  const bet = betId ? player.bets.get(betId) : null;
  if (!bet) {
    metrics.unmatchedCashouts++;
    return;
  }
  if (bet.scenario === "loss" || bet.scenario === "reject") {
    metrics.unexpectedCashouts++;
  }

  clearTimeout(bet.cashoutTimeout);
  bet.cashoutTimeout = null;
  bet.status = "cashed-out";
  metrics.cashoutsCompleted++;

  const completedAt = performance.now();
  if (bet.scenario === "manual") {
    metrics.manualCashoutsCompleted++;
    if (bet.manualCashoutRequestedAt != null) {
      metrics.manualCashoutLatenciesMs.push(completedAt - bet.manualCashoutRequestedAt);
    }
  } else {
    metrics.autoCashoutsCompleted++;
    const startedAt = bet.autoTargetReachedAt ?? bet.provisionalAcceptedAt;
    metrics.autoCashoutLatenciesMs.push(completedAt - startedAt);
  }

  const multiplierValue = Number(readField(response, "cashoutMultiplier", "CashoutMultiplier"));
  const payoutAmount = Number(readField(response, "payoutAmount", "PayoutAmount"));
  const netResultAmount = Number(readField(response, "netResultAmount", "NetResultAmount"));
  const updatedBalance = Number(readField(response, "updatedBalance", "UpdatedBalance"));
  const roundId = stringField(response, "roundId", "RoundId");

  if (roundId !== bet.roundId || !Number.isFinite(multiplierValue)) {
    metrics.contractViolations++;
  }

  const expectedPayout = bet.stakeAmount * multiplierValue;
  if (!approximatelyEqual(payoutAmount, expectedPayout) ||
      !approximatelyEqual(netResultAmount, expectedPayout - bet.stakeAmount)) {
    metrics.payoutInvariantFailures++;
  }

  const expectedBalance = bet.reservedBalance + payoutAmount;
  if (!approximatelyEqual(updatedBalance, expectedBalance)) {
    metrics.balanceInvariantFailures++;
  }
  if (Number.isFinite(updatedBalance)) player.balance = updatedBalance;
}

function scheduleControlledReconnect(player) {
  if (config.reconnectPercent <= 0) return;
  const reconnectPlayers = Math.ceil(config.players * config.reconnectPercent / 100);
  if (player.index > reconnectPlayers) return;

  player.reconnectTimer = setTimeout(async () => {
    if (stopping) return;
    metrics.controlledReconnectAttempts++;

    try {
      await player.connection.stop();
      await delay(config.reconnectDowntimeMs);
      if (stopping) return;
      await player.connection.start();
      markConnected(player);
      metrics.controlledReconnectSucceeded++;
    } catch (error) {
      metrics.controlledReconnectFailed++;
      console.error(`[${player.username}] controlled reconnect failed: ${error.message}`);
    }
  }, config.reconnectAfterMs);
  player.reconnectTimer.unref();
}

function recordMessage(player, eventName, message) {
  const messageId = stringField(message, "messageId", "MessageId");
  if (!messageId) {
    metrics.contractViolations++;
    return true;
  }

  const key = `${eventName}:${messageId}`;
  if (player.seenMessages.has(key)) {
    metrics.duplicateDeliveries++;
    return false;
  }

  player.seenMessages.add(key);
  return true;
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
  const report = buildReport();
  const label = final ? "FINAL" : new Date().toISOString();

  console.log(
    `[${label}] connected=${metrics.connected}/${config.players} peak=${metrics.peakConnected}` +
    ` tables=${tables.size} rounds=${metrics.uniqueRounds} ticks=${metrics.uniqueTicks}` +
    ` bets sent=${metrics.betsSent} placed=${metrics.provisionalAccepted}` +
    ` rejected=${metrics.initialRejected} compensated=${metrics.compensatedRejections}` +
    ` betTimeout=${metrics.betResponseTimedOut} pending=${report.pendingBets}` +
    ` cashout sent=${metrics.manualCashoutsSent}+${metrics.autoCashoutTargetsReached}` +
    ` completed=${metrics.cashoutsCompleted} cashoutTimeout=${metrics.cashoutsTimedOut}` +
    ` contract=${metrics.contractViolations} balance=${metrics.balanceInvariantFailures}` +
    ` payout=${metrics.payoutInvariantFailures}` +
    ` placedMs p50=${format(report.latencies.provisional.p50)}` +
    ` p95=${format(report.latencies.provisional.p95)}` +
    ` p99=${format(report.latencies.provisional.p99)}` +
    ` cashoutMs p95=${format(report.latencies.cashoutCombined.p95)}`,
  );

  if (metrics.rejectionCodes.size > 0) {
    console.log(`  rejectionCodes: ${mapSummary(metrics.rejectionCodes)}`);
  }
}

function buildReport() {
  const pendingBets = clients.reduce(
    (total, player) => total + player.pendingBets.size,
    0,
  );
  const pendingCashouts = clients.reduce(
    (total, player) => total + [...player.bets.values()]
      .filter(bet => bet.cashoutTimeout != null).length,
    0,
  );
  const respondedBets = metrics.provisionalAccepted + metrics.initialRejected;
  const expectedCashouts = metrics.manualCashoutsSent + metrics.autoCashoutTargetsReached;
  const combinedCashoutLatencies = [
    ...metrics.manualCashoutLatenciesMs,
    ...metrics.autoCashoutLatenciesMs,
  ];

  return {
    generatedAt: new Date().toISOString(),
    stopReason,
    config: serializableConfig(),
    counters: Object.fromEntries(
      Object.entries(metrics)
        .filter(([, value]) => typeof value === "number"),
    ),
    pendingBets,
    pendingCashouts,
    rates: {
      betResponse: rate(respondedBets, metrics.betsSent),
      cashoutSuccess: rate(metrics.cashoutsCompleted, expectedCashouts),
    },
    latencies: {
      login: summarize(metrics.loginLatenciesMs),
      connection: summarize(metrics.connectionLatenciesMs),
      provisional: summarize(metrics.provisionalLatenciesMs),
      rejection: summarize(metrics.rejectionLatenciesMs),
      duplicateRetry: summarize(metrics.duplicateRetryLatenciesMs),
      manualCashout: summarize(metrics.manualCashoutLatenciesMs),
      autoCashout: summarize(metrics.autoCashoutLatenciesMs),
      cashoutCombined: summarize(combinedCashoutLatencies),
    },
    rejectionCodes: Object.fromEntries(metrics.rejectionCodes),
    scenarioAssignments: Object.fromEntries(metrics.scenarioAssignments),
    tableDistribution: Object.fromEntries(tables),
  };
}

function evaluate(report) {
  const failures = [];
  const requireZero = [
    ["login failures", metrics.loginFailed],
    ["connection failures", metrics.connectionFailed],
    ["controlled reconnect failures", metrics.controlledReconnectFailed],
    ["bet invocation failures", metrics.betInvokeFailed],
    ["bet response timeouts", metrics.betResponseTimedOut],
    ["unexpected acceptances", metrics.unexpectedAcceptances],
    ["unexpected rejections", metrics.unexpectedRejections],
    ["compensating DB rejections", metrics.compensatedRejections],
    ["invalid provisional statuses", metrics.invalidPlacedStatuses],
    ["unmatched bet responses", metrics.unmatchedBetResponses],
    ["duplicate retry invocation failures", metrics.duplicateRetryInvokeFailed],
    ["duplicate retry rejections", metrics.duplicateRetriesRejected],
    ["duplicate retry timeouts", metrics.duplicateRetriesTimedOut],
    ["cashout invocation failures", metrics.manualCashoutInvokeFailed],
    ["cashout timeouts", metrics.cashoutsTimedOut],
    ["unmatched cashouts", metrics.unmatchedCashouts],
    ["unexpected cashouts", metrics.unexpectedCashouts],
    ["contract violations", metrics.contractViolations],
    ["out-of-order ticks", metrics.outOfOrderTicks],
    ["multiplier regressions", metrics.multiplierRegressions],
    ["balance invariant failures", metrics.balanceInvariantFailures],
    ["payout invariant failures", metrics.payoutInvariantFailures],
  ];

  for (const [label, value] of requireZero) {
    if (value > 0) failures.push(`${label}: ${value}`);
  }
  if (report.pendingBets > 0) failures.push(`pending bet responses after drain: ${report.pendingBets}`);
  if (report.pendingCashouts > 0) {
    failures.push(`pending cashout responses after drain: ${report.pendingCashouts}`);
  }
  if (metrics.uniqueRounds === 0) failures.push("no bettable rounds were observed");
  if (report.rates.betResponse < config.minBetResponseRate) {
    failures.push(
      `bet response rate ${formatPercent(report.rates.betResponse)} < ` +
      `${formatPercent(config.minBetResponseRate)}`,
    );
  }

  const expectedCashouts = metrics.manualCashoutsSent + metrics.autoCashoutTargetsReached;
  if (expectedCashouts > 0 && report.rates.cashoutSuccess < config.minCashoutSuccessRate) {
    failures.push(
      `cashout success rate ${formatPercent(report.rates.cashoutSuccess)} < ` +
      `${formatPercent(config.minCashoutSuccessRate)}`,
    );
  }
  if (config.maxProvisionalP95Ms > 0 &&
      report.latencies.provisional.p95 > config.maxProvisionalP95Ms) {
    failures.push(
      `provisional p95 ${format(report.latencies.provisional.p95)}ms > ` +
      `${config.maxProvisionalP95Ms}ms`,
    );
  }
  if (config.maxCashoutP95Ms > 0 &&
      report.latencies.cashoutCombined.p95 > config.maxCashoutP95Ms) {
    failures.push(
      `cashout p95 ${format(report.latencies.cashoutCombined.p95)}ms > ` +
      `${config.maxCashoutP95Ms}ms`,
    );
  }

  return failures;
}

async function shutdown(reason) {
  if (!stopping) stopping = true;
  stopReason = reason;
  clearInterval(reportTimer);
  clearTimeout(durationTimer);

  console.log(`\nStopping new work: ${reason}. Draining pending outcomes...`);
  await waitForDrain(config.drainTimeoutMs);

  const report = buildReport();
  printReport(true);
  printTableDistribution();
  printScenarioDistribution();

  const failures = evaluate(report);

  if (config.reportJsonPath) {
    try {
      await writeFile(config.reportJsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
      console.log(`JSON report written to ${config.reportJsonPath}`);
    } catch (error) {
      failures.push(`could not write JSON report: ${error.message}`);
    }
  }

  if (failures.length === 0) {
    console.log("RESULT: PASS");
  } else {
    console.error("RESULT: FAIL");
    for (const failure of failures) console.error(`  - ${failure}`);
  }

  for (const player of clients) clearPlayerTimers(player);
  await Promise.allSettled(clients.map(player => player.connection.stop()));

  if (config.failOnErrors && failures.length > 0) {
    process.exitCode = 1;
  }
}

async function waitForDrain(timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const pending = clients.reduce(
      (total, player) => total + player.pendingBets.size +
        [...player.bets.values()].filter(bet =>
          bet.cashoutTimeout != null || bet.duplicateRetryStartedAt != null).length,
      0,
    );
    if (pending === 0) return;
    await delay(100);
  }
}

function clearPlayerTimers(player) {
  clearTimeout(player.reconnectTimer);
  for (const pending of player.pendingBets.values()) clearTimeout(pending.timeout);
  for (const bet of player.bets.values()) {
    clearTimeout(bet.cashoutTimeout);
    clearTimeout(bet.duplicateRetryTimeout);
  }
}

async function runPool(items, concurrency, worker) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (!stopping && next < items.length) {
      const item = items[next++];
      await worker(item);
    }
  }));
}

function requestStop(reason) {
  if (stopReason != null) return;
  stopReason = reason;
  stopping = true;
  resolveStop(reason);
}

function chooseScenario(index) {
  const totalWeight = config.scenarioMix.reduce((sum, entry) => sum + entry.weight, 0);
  const position = (index - 1) % totalWeight;
  let cursor = 0;

  for (const entry of config.scenarioMix) {
    cursor += entry.weight;
    if (position < cursor) return entry.name;
  }
  return config.scenarioMix.at(-1).name;
}

function printTableDistribution() {
  console.log(`Table distribution: ${mapSummary(tables) || "none"}`);
}

function printScenarioDistribution() {
  console.log(`Scenario distribution: ${mapSummary(metrics.scenarioAssignments) || "none"}`);
}

function mapSummary(map) {
  return [...map.entries()]
    .sort(([left], [right]) => String(left).localeCompare(String(right), undefined, { numeric: true }))
    .map(([key, value]) => `${key}=${value}`)
    .join(", ");
}

function summarize(values) {
  if (values.length === 0) {
    return { count: 0, avg: null, p50: null, p95: null, p99: null, max: null };
  }

  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: values.length,
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

function serializableConfig() {
  return {
    ...config,
    scenarioMix: config.scenarioMix
      .map(entry => `${entry.name}:${entry.weight}`)
      .join(","),
  };
}

function scenarioMix(raw) {
  const entries = raw.split(",").map(part => {
    const [nameValue, weightValue] = part.split(":");
    const name = nameValue?.trim().toLowerCase();
    const weight = Number(weightValue);

    if (!supportedScenarios.has(name) || !Number.isInteger(weight) || weight <= 0) {
      throw new Error(
        `Invalid SCENARIO_MIX entry "${part}". ` +
        `Use comma-separated name:weight entries from ${[...supportedScenarios].join(", ")}.`,
      );
    }
    return { name, weight };
  });

  if (entries.length === 0) throw new Error("SCENARIO_MIX must not be empty.");
  return entries;
}

function isTerminalBet(bet) {
  return bet.status === "cashed-out" ||
    bet.status === "rejected" ||
    bet.status === "loss-observed";
}

function incrementMap(map, key) {
  map.set(key, (map.get(key) ?? 0) + 1);
}

function rate(numerator, denominator) {
  return denominator === 0 ? 0 : numerator / denominator;
}

function approximatelyEqual(left, right, tolerance = 0.01) {
  return Number.isFinite(left) &&
    Number.isFinite(right) &&
    Math.abs(left - right) <= tolerance;
}

function readField(value, camelCase, pascalCase) {
  return value?.[camelCase] ?? value?.[pascalCase];
}

function stringField(value, camelCase, pascalCase) {
  const field = readField(value, camelCase, pascalCase);
  return field == null ? null : String(field);
}

function format(value) {
  return value == null ? "n/a" : value.toFixed(1);
}

function formatPercent(value) {
  return `${(value * 100).toFixed(2)}%`;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function positiveInteger(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return value;
}

function nonNegativeInteger(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  return value;
}

function positiveNumber(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }
  return value;
}

function nonNegativeNumber(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number.`);
  }
  return value;
}

function multiplier(name, fallback) {
  const value = positiveNumber(name, fallback);
  if (value <= 1) throw new Error(`${name} must be greater than 1.`);
  return value;
}

function percentage(name, fallback) {
  const value = nonNegativeNumber(name, fallback);
  if (value > 100) throw new Error(`${name} must be between 0 and 100.`);
  return value;
}

function ratio(name, fallback) {
  const value = nonNegativeNumber(name, fallback);
  if (value > 1) throw new Error(`${name} must be between 0 and 1.`);
  return value;
}

function booleanValue(name, fallback) {
  const raw = process.env[name];
  if (raw == null) return fallback;
  if (raw.toLowerCase() === "true") return true;
  if (raw.toLowerCase() === "false") return false;
  throw new Error(`${name} must be true or false.`);
}
