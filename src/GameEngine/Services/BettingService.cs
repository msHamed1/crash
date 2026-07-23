using System.Collections.Concurrent;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Contracts.Messaging.EngineToGateway.Bets;
using Crash.Domain.Entities;
using Crash.Domain.Options;
using Crash.Domain.State;
using GameEngine.Application.Commands.Bets;
using GameEngine.Application.Results;
using GameEngine.Messaging.Publishers;

namespace GameEngine.Services;

public sealed class BettingService(
    IWssGatewayPublisher wssPublisher,
    ILogger<BettingService> logger,
    GameEngineOptions options,
    IDbWorkerPublisher dbWorkerPublisher)
{
    private readonly ConcurrentDictionary<string, byte> _pendingSettlements = new();

    public async Task PlaceBetAsync(
        PlaceBetCommand command,
        TableRuntimeState table,
        CancellationToken ct)
    {
        
        // Validate memory-> reserve balance -> Placed
        //    -> notify player with PlayerBetAccepted
        //     -> persist asynchronously
        //
        // DB success -> Accepted -> return silently
        // DB rejection -> rollback reservation -> PlayerBetRejected
        
        if (!long.TryParse(command.PlayerId, out var playerId))
        {
            logger.LogWarning(
                "Rejecting bet {CorrelationId}: invalid player id {PlayerId}.",
                command.CorrelationId,
                command.PlayerId);
            return;
        }

        if (!long.TryParse(command.RoundId, out var roundId))
        {
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "INVALID_ROUND_ID", "RoundId must be a valid number.", 0, ct);
            return;
        }

        if (string.IsNullOrWhiteSpace(command.CorrelationId))
        {
            logger.LogWarning("Rejecting a bet without a correlation ID for player {PlayerId}.", playerId);
            return;
        }

        if (!string.Equals(command.Currency, "USD", StringComparison.OrdinalIgnoreCase))
        {
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "UNSUPPORTED_CURRENCY", "Only USD bets are currently supported.", 0, ct);
            return;
        }

        var currentRound = table.GetCurrentRoundSnapshot();
        if (currentRound?.RoundId != roundId)
        {
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "ROUND_NOT_BETTABLE", "The round is no longer accepting bets.", 0, ct);
            return;
        }

        if (!table.GetPlayer(playerId, out var player) || player is null)
        {
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "PLAYER_NOT_AT_TABLE", "The player has not joined this table.", 0, ct);
            return;
        }

        // BetId is the request correlation ID, making redelivery idempotent at the unique DB index.
        // var bet = table.AddNewBet(
        //  player,
        //   command.Amount,
        //     roundId,
        //     command.CorrelationId,
        //     command.Currency,
        //     
        //     
        //     command.Currency.ToUpperInvariant());

        var bet = table.AddNewBet(
            player:player,
            amount:command.Amount,
            roundId:roundId,
            betId:command.CorrelationId,
            currency:  command.Currency.ToUpperInvariant(),
            autoCashoutEnabled:command.AutoCashoutEnabled,
            autoCashoutMultiplier:command.AutoCashoutMultiplier
            );

        if (bet is null)
        {
            var existingBet = table.GetPlayerBet(roundId, playerId);
            if (existingBet?.BetId == command.CorrelationId)
            {
                await PublishAccepted(command, table.TableId, playerId, player.Balance, existingBet, ct);
                return;
            }

            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "BET_REJECTED", "The bet is invalid, duplicated, or betting is closed.", player.Balance, ct);
            return;
        }

        try
        {
            // The runtime decision is authoritative for the live game. Notify the
            // player as soon as the stake is reserved; the payload still exposes
            // the bet as Placed until the DB worker confirms persistence.
            await PublishAccepted(
                command,
                table.TableId,
                playerId,
                player.Balance,
                bet,
                ct);

            await dbWorkerPublisher.PublishAsync(new DbWorkerMessageEnvelope(MessageId : Guid.NewGuid(),
                    CreatedAt : DateTimeOffset.UtcNow,
                    Payload: new BetAcceptedForPersistence(AcceptedAt : DateTimeOffset.UtcNow,
                        AutoCashoutMultiplier : command.AutoCashoutMultiplier,
                        BetId : command.CorrelationId,
                        Currency : command.Currency.ToUpperInvariant(),
                        FencingToken : table.FencingToken,
                        PlayerId : playerId,
                        RoundId :roundId,
                        Sequence : 1,
                        StakeAmount : bet.StakeAmount,
                        TableId :table.TableId)
                    ,
                    Type : DbWorkerMessageType.BetAccepted)
            , ct);

        }
        catch (Exception exception)
        {
            // Publishing the player notification or the durable DB command failed.
            // Release the runtime reservation and compensate any provisional
            // acceptance that may already have reached the player.
            table.RollbackBetInMemory(bet, player);
            logger.LogError(exception,
                "Failed to place bet {BetId} for player {PlayerId} in round {RoundId}.",
                bet.BetId, playerId, roundId);
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "BET_PROCESSING_FAILED", "The bet could not be processed.", player.Balance, ct);
            return;
        }

        // DB-worker completion promotes Placed to Accepted. It does not emit a
        // second player acceptance notification.
    }

    private Task PublishAccepted(
        PlaceBetCommand command,
        long tableId,
        long playerId,
        decimal updatedBalance,
        Bet bet,
        CancellationToken ct)
    {
        return wssPublisher.PublishAsync(new BetAccepted
        {
            TableId = tableId,
            MessageId = command.CorrelationId,
            PlayerId = playerId,
            UpdatedBalance = updatedBalance,
            Bet = bet
        }, ct);
    }
    
    
    

    private Task PublishRejected(
        string correlationId,
        long tableId,
        long playerId,
        string code,
        string reason,
        decimal updatedBalance,
        CancellationToken ct)
    {
        return wssPublisher.PublishAsync(new BetRejected
        {
            TableId = tableId,
            MessageId =correlationId,
            PlayerId = playerId,
            UpdatedBalance = updatedBalance,
            Code = code,
            Reason = reason
        }, ct);
    }

    public async Task CashOutAsync(
        CashOutBetCommand command,
        TableRuntimeState table,
        CancellationToken ct)
    {
        if (!long.TryParse(command.PlayerId, out var playerId)
            || !long.TryParse(command.RoundId, out var roundId))
        {
            return;
        }

        var round = table.GetCurrentRoundSnapshot();
        var bet = table.GetPlayerBet(roundId, playerId);
        if (round is null
            || round.RoundId != roundId
            || round.IsCrashed
            || round.CurrentMultiplier < 1.00m
            || bet?.BetId != command.BetId
            || bet.Status != BetStatus.Accepted)
        {
            return;
        }

        await PublishSettlementAsync(
            bet,
            BetSettlementStatus.CashedOut,
            round.CurrentMultiplier,
            bet.StakeAmount * round.CurrentMultiplier,
            table,
            ct);
    }

    public async Task<DbResultHandlingOutcome> ProcessBetPersistenceCompleted(
        DbBetPersistenceCompletedCommand message,
        CancellationToken ct)
    {
        if (!long.TryParse(message.TableId, out var tableId))
        {
            return DbResultHandlingOutcome.Invalid(
                $"TableId '{message.TableId}' is not a valid number.");
        }

        if (!long.TryParse(message.RoundId, out var messageRoundId))
        {
            return DbResultHandlingOutcome.Invalid(
                $"RoundId '{message.RoundId}' is not a valid number.");
        }

        if (!options.Tables.TryGetValue(tableId, out var table))
        {
            // Ownership/table registration can race with a result consumer.
            // Allow a short bounded retry in the consumer, but never requeue forever.
            return DbResultHandlingOutcome.Retryable(
                $"Table {tableId} is not available in this engine.");
        }

        var isCommitted = message.Status is
            DbWorkerResultStatus.Committed or
            DbWorkerResultStatus.AlreadyProcessed;

        if (isCommitted &&
            message.ResultType == DbWorkerResultMessageType.BetSettled)
        {
            // The database is terminal even when this result arrived after the
            // engine moved to another round.
            _pendingSettlements.TryRemove(message.BetId, out _);
        }

        var round = table.GetCurrentRound();
        if (round is null || round.RoundId != messageRoundId)
        {
            // A previous-round result must never overwrite the current runtime
            // balance/state. MySQL already contains the authoritative outcome.
            return DbResultHandlingOutcome.Stale(
                $"Result round {messageRoundId} is no longer current for table {tableId}.");
        }

        if (message.Status == DbWorkerResultStatus.Rejected)
        {
            return await HandleRejectedBetAcceptanceAsync(
                message,
                table,
                messageRoundId,
                ct);
        }

        if (!isCommitted)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Unsupported DB result status {message.Status}.");
        }

        return message.ResultType switch
        {
            DbWorkerResultMessageType.BetAccepted =>
                HandleCommittedBetAcceptance(message, table, messageRoundId),

            DbWorkerResultMessageType.BetCancelled =>
                await HandleCommittedBetCancellationAsync(message, table, ct),

            DbWorkerResultMessageType.BetSettled =>
                await HandleCommittedBetSettlementAsync(
                    message,
                    table,
                    messageRoundId,
                    ct),

            _ => DbResultHandlingOutcome.Invalid(
                $"Unsupported DB result type {message.ResultType}.")
        };
    }

    private async Task<DbResultHandlingOutcome> HandleRejectedBetAcceptanceAsync(
        DbBetPersistenceCompletedCommand message,
        TableRuntimeState table,
        long roundId,
        CancellationToken ct)
    {
        if (message.ResultType != DbWorkerResultMessageType.BetAccepted)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Rejected result type {message.ResultType} is not supported.");
        }

        logger.LogWarning(
            "DB worker rejected bet {BetId} for player {PlayerId}. Code={ErrorCode}; Detail={ErrorMessage}.",
            message.BetId,
            message.PlayerId,
            message.ErrorCode,
            message.ErrorMessage);

        if (!table.GetPlayer(message.PlayerId, out var player) || player is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Player {message.PlayerId} is unavailable for rejected bet {message.BetId}.");
        }

        var bet = table.GetPlayerBet(roundId, message.PlayerId);
        if (bet is null)
        {
            // A prior delivery may have refunded the stake but failed while
            // publishing the player notification.
            await PublishRejected(
                message.BetId,
                table.TableId,
                message.PlayerId,
                message.ErrorCode ?? "BET_PERSISTENCE_REJECTED",
                "The bet could not be accepted.",
                player.Balance,
                ct);

            return DbResultHandlingOutcome.Handled(
                $"Rejected bet {message.BetId} was already removed; notification was replayed.");
        }

        if (!string.Equals(bet.BetId, message.BetId, StringComparison.Ordinal))
        {
            return DbResultHandlingOutcome.Invalid(
                $"Rejected result bet {message.BetId} does not match runtime bet {bet.BetId}.");
        }

        if (bet.IsPersisted || bet.Status != BetStatus.Placed)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Rejected bet {message.BetId} is already in runtime state {bet.Status}.");
        }

        if (!table.RollbackBetInMemory(bet, player))
        {
            var currentBet = table.GetPlayerBet(roundId, message.PlayerId);
            if (currentBet is not null)
            {
                return DbResultHandlingOutcome.Retryable(
                    $"Rejected bet {message.BetId} could not be removed from the current round.");
            }
        }

        await PublishRejected(
            message.BetId,
            table.TableId,
            message.PlayerId,
            message.ErrorCode ?? "BET_PERSISTENCE_REJECTED",
            "The bet could not be accepted.",
            player.Balance,
            ct);

        return DbResultHandlingOutcome.Handled(
            $"Rejected bet {message.BetId} was refunded and the player was notified.");
    }

    private static DbResultHandlingOutcome HandleCommittedBetAcceptance(
        DbBetPersistenceCompletedCommand message,
        TableRuntimeState table,
        long roundId)
    {
        var bet = table.GetPlayerBet(roundId, message.PlayerId);
        if (bet is null)
        {
            // The ticker may already have cancelled and refunded an unconfirmed
            // bet while this acceptance result was in flight.
            return DbResultHandlingOutcome.Handled(
                $"Accepted bet {message.BetId} is no longer present in runtime memory.");
        }

        if (!string.Equals(bet.BetId, message.BetId, StringComparison.Ordinal))
        {
            return DbResultHandlingOutcome.Invalid(
                $"Accepted result bet {message.BetId} does not match runtime bet {bet.BetId}.");
        }

        if (bet.IsPersisted && bet.Status == BetStatus.Accepted)
        {
            return DbResultHandlingOutcome.Handled(
                $"Accepted bet {message.BetId} was already applied.");
        }

        if (bet.Status != BetStatus.Placed)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Accepted bet {message.BetId} is already in runtime state {bet.Status}.");
        }

        if (!table.GetPlayer(message.PlayerId, out var player) || player is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Player {message.PlayerId} is unavailable for accepted bet {message.BetId}.");
        }

        var persistedBet = table.SetBetIsPersisted(message.PlayerId, roundId);
        if (persistedBet is null)
        {
            return DbResultHandlingOutcome.Retryable(
                $"Accepted bet {message.BetId} changed while its result was being applied.");
        }

        table.SetPlayerBalance(player, message.UpdatedBalance);
        persistedBet.Player.BalanceInUSD = message.UpdatedBalance;

        return DbResultHandlingOutcome.Handled(
            $"Accepted bet {message.BetId} was applied to runtime memory.");
    }

    private async Task<DbResultHandlingOutcome> HandleCommittedBetCancellationAsync(
        DbBetPersistenceCompletedCommand message,
        TableRuntimeState table,
        CancellationToken ct)
    {
        if (!table.GetPlayer(message.PlayerId, out var player) || player is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Player {message.PlayerId} is unavailable for cancelled bet {message.BetId}.");
        }

        table.SetPlayerBalance(player, message.UpdatedBalance);
        await PublishRejected(
            message.BetId,
            table.TableId,
            message.PlayerId,
            "BET_CANCELLED",
            "The bet was cancelled before the round started.",
            message.UpdatedBalance,
            ct);

        return DbResultHandlingOutcome.Handled(
            $"Cancelled bet {message.BetId} balance and notification were applied.");
    }

    private async Task<DbResultHandlingOutcome> HandleCommittedBetSettlementAsync(
        DbBetPersistenceCompletedCommand message,
        TableRuntimeState table,
        long roundId,
        CancellationToken ct)
    {
        if (message.SettlementStatus is null || message.SettledAt is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Settled bet {message.BetId} is missing its status or settlement timestamp.");
        }

        var bet = table.GetPlayerBet(roundId, message.PlayerId);
        if (bet is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Settled bet {message.BetId} is missing from the current runtime round.");
        }

        if (!string.Equals(bet.BetId, message.BetId, StringComparison.Ordinal))
        {
            return DbResultHandlingOutcome.Invalid(
                $"Settled result bet {message.BetId} does not match runtime bet {bet.BetId}.");
        }

        if (message.SettlementStatus == BetSettlementStatus.Lost)
        {
            if (bet.Status == BetStatus.Lost)
            {
                return DbResultHandlingOutcome.Handled(
                    $"Lost settlement for bet {message.BetId} was already applied.");
            }

            if (bet.Status == BetStatus.Placed)
            {
                return DbResultHandlingOutcome.Retryable(
                    $"Lost settlement for bet {message.BetId} arrived before acceptance.");
            }

            if (bet.Status != BetStatus.Accepted)
            {
                return DbResultHandlingOutcome.Invalid(
                    $"Lost settlement conflicts with runtime state {bet.Status} for bet {message.BetId}.");
            }

            if (table.ApplyCommittedLoss(message.BetId, message.SettledAt.Value))
            {
                return DbResultHandlingOutcome.Handled(
                    $"Lost settlement for bet {message.BetId} was applied.");
            }

            var currentBet = table.GetPlayerBet(roundId, message.PlayerId);
            return currentBet?.Status switch
            {
                BetStatus.Lost => DbResultHandlingOutcome.Handled(
                    $"Lost settlement for bet {message.BetId} was applied concurrently."),
                BetStatus.Placed => DbResultHandlingOutcome.Retryable(
                    $"Lost settlement for bet {message.BetId} is waiting for acceptance."),
                _ => DbResultHandlingOutcome.Invalid(
                    $"Lost settlement for bet {message.BetId} could not be applied.")
            };
        }

        if (message.SettlementStatus != BetSettlementStatus.CashedOut ||
            message.CashoutMultiplier is null)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Unsupported settlement status {message.SettlementStatus} for bet {message.BetId}.");
        }

        if (bet.Status == BetStatus.CashedOut)
        {
            // Re-publish with the stable cashout message ID. This repairs a
            // previous delivery that updated memory but failed at notification.
            await PublishCashoutAsync(message, table.TableId, ct);
            return DbResultHandlingOutcome.Handled(
                $"Cashout settlement for bet {message.BetId} was already applied; notification was replayed.");
        }

        if (bet.Status == BetStatus.Placed)
        {
            return DbResultHandlingOutcome.Retryable(
                $"Cashout settlement for bet {message.BetId} arrived before acceptance.");
        }

        if (bet.Status != BetStatus.Accepted)
        {
            return DbResultHandlingOutcome.Invalid(
                $"Cashout settlement conflicts with runtime state {bet.Status} for bet {message.BetId}.");
        }

        var applied = table.ApplyCommittedCashout(
            message.BetId,
            message.CashoutMultiplier.Value,
            message.PayoutAmount,
            message.UpdatedBalance,
            message.SettledAt.Value);

        if (!applied)
        {
            var currentBet = table.GetPlayerBet(roundId, message.PlayerId);
            if (currentBet?.Status == BetStatus.CashedOut)
            {
                await PublishCashoutAsync(message, table.TableId, ct);
                return DbResultHandlingOutcome.Handled(
                    $"Cashout settlement for bet {message.BetId} was applied concurrently.");
            }

            return currentBet?.Status == BetStatus.Placed
                ? DbResultHandlingOutcome.Retryable(
                    $"Cashout settlement for bet {message.BetId} is waiting for acceptance.")
                : DbResultHandlingOutcome.Invalid(
                    $"Cashout settlement for bet {message.BetId} could not be applied.");
        }

        await PublishCashoutAsync(message, table.TableId, ct);
        return DbResultHandlingOutcome.Handled(
            $"Cashout settlement for bet {message.BetId} was applied and published.");
    }

    public async Task ProcessAutoCashoutsAsync(
        TableRuntimeState table,
        long roundId,
        decimal currentMultiplier,
        CancellationToken ct)
    {
        var candidates = table.GetAutoCashoutCandidates(
            roundId,
            currentMultiplier);

        foreach (var candidate in candidates)
        {
            var bet = table.GetPlayerBet(candidate.RoundId, candidate.PlayerId);
            if (bet is null)
                continue;

            await PublishSettlementAsync(
                bet,
                BetSettlementStatus.CashedOut,
                candidate.CashoutMultiplier,
                candidate.StakeAmount * candidate.CashoutMultiplier,
                table,
                ct);
        }
    }

    private Task PublishCashoutAsync(
        DbBetPersistenceCompletedCommand result,
        long tableId,
        CancellationToken ct)
    {
        // A stable message ID lets the gateway/client deduplicate a retried settlement event.
        return wssPublisher.PublishAsync(new BetCashedOut
        {
            TableId = tableId,
            MessageId = $"cashout:{result.BetId}",
            PlayerId = result.PlayerId,
            BetId = result.BetId,
            RoundId = long.Parse(result.RoundId),
            CashoutMultiplier = result.CashoutMultiplier!.Value,
            PayoutAmount = result.PayoutAmount,
            NetResultAmount = result.ProfitLoss,
            UpdatedBalance = result.UpdatedBalance,
            CashedOutAt = result.SettledAt!.Value
        }, ct);
    }
    
    
    public async Task SettleCrashedRoundAsync(
        TableRuntimeState table,
        long roundId,
        CancellationToken ct)
    {
        var bets = table.GetCurrentRound()?.GetBetsSnapshot()
            .Where(bet => bet.RoundId == roundId && bet.Status == BetStatus.Accepted)
            .ToArray() ?? [];

        foreach (var bet in bets)
        {
            await PublishSettlementAsync(
                bet,
                BetSettlementStatus.Lost,
                null,
                0,
                table,
                ct);
        }
    }

    private async Task PublishSettlementAsync(
        Bet bet,
        BetSettlementStatus status,
        decimal? cashoutMultiplier,
        decimal payoutAmount,
        TableRuntimeState table,
        CancellationToken ct)
    {
        if (!_pendingSettlements.TryAdd(bet.BetId, 0))
            return;

        var settledAt = DateTimeOffset.UtcNow;
        try
        {
            await dbWorkerPublisher.PublishAsync(new DbWorkerMessageEnvelope(
            MessageId: Guid.NewGuid(),
            Type: DbWorkerMessageType.BetSettled,
            Payload: new BetSettledForPersistence(
                BetId: bet.BetId,
                PlayerId: bet.PlayerId,
                TableId: table.TableId,
                RoundId: bet.RoundId,
                Status: status,
                PayoutAmount: payoutAmount,
                ProfitLoss: payoutAmount - bet.StakeAmount,
                CashoutMultiplier: cashoutMultiplier,
                FencingToken: table.FencingToken,
                Sequence: 2,
                SettledAt: settledAt),
                CreatedAt: settledAt), ct);
        }
        catch
        {
            _pendingSettlements.TryRemove(bet.BetId, out _);
            throw;
        }
    }
}
