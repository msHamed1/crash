using System.Collections.Concurrent;
using Crash.Contracts.Messaging.DbWorkers;
using Crash.Contracts.Messaging.EngineToGateway.Bets;
using Crash.Domain.Entities;
using Crash.Domain.Options;
using Crash.Domain.State;
using GameEngine.Application.Commands.Bets;
using GameEngine.Messaging.Publishers;

namespace GameEngine.Services;

public sealed class BettingService(
    IWssGatewayPublisher publisher,
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

     //   PlaceBetResult result;
        try
        {
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
            // A failed DB transaction must release the balance reserved in runtime state.
            table.RollbackBetInMemory(bet, player);
            logger.LogError(exception,
                "Failed to place bet {BetId} for player {PlayerId} in round {RoundId}.",
                bet.BetId, playerId, roundId);
            await PublishRejected(command.CorrelationId, table.TableId, playerId,
                "BET_PROCESSING_FAILED", "The bet could not be processed.", player.Balance, ct);
            return;
        }

        // if (!result.IsAccepted || result.Bet is null)
        // {
        //     table.RollbackBetInMemory(bet, player);
        //     await PublishRejected(command.CorrelationId, table.TableId, playerId,
        //         result.Code, result.Message, player.Balance, ct);
        //     return;
        // }

        // The accepted notification is emitted from the DB-worker completion
        // handler, after the transaction commits and memory is synchronized.
    }

    private Task PublishAccepted(
        PlaceBetCommand command,
        long tableId,
        long playerId,
        decimal updatedBalance,
        Bet bet,
        CancellationToken ct)
    {
        return publisher.PublishAsync(new BetAccepted
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
        return publisher.PublishAsync(new BetRejected
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

    public async Task<bool> ProcessBetPersistenceCompleted(
        DbBetPersistenceCompletedCommand message,
        CancellationToken ct)
    {
        if (!long.TryParse(message.TableId, out var tableId) ||
            !options.Tables.TryGetValue(tableId, out var table))
        {
            return false;
        }

        if (!long.TryParse(message.RoundId, out var messageRoundId))
        {
            return false;
        }

        var round = table.GetCurrentRound();

        if (round is null || round.RoundId != messageRoundId)
        {
            return false;
        }

        if (message.Status is not (DbWorkerResultStatus.Committed or DbWorkerResultStatus.AlreadyProcessed))
            return false;

        if (message.ResultType == DbWorkerResultMessageType.BetCancelled)
        {
            if (!table.GetPlayer(message.PlayerId, out var cancelledPlayer) || cancelledPlayer is null)
                return false;

            table.SetPlayerBalance(cancelledPlayer, message.UpdatedBalance);
            await PublishRejected(message.BetId, table.TableId, message.PlayerId,
                "BET_CANCELLED", "The bet was cancelled before the round started.",
                message.UpdatedBalance, ct);
            return true;
        }

        var bet = round.GetBet(message.PlayerId);
        if (bet is null)
        {
            // The ticker may already have removed and refunded an unconfirmed
            // bet while this acceptance acknowledgement was in flight.
            return message.ResultType == DbWorkerResultMessageType.BetAccepted;
        }

        if (message.ResultType == DbWorkerResultMessageType.BetAccepted)
        {
            if (bet.IsPersisted)
                return true;

            var persistedBet = table.SetBetIsPersisted(message.PlayerId, messageRoundId);
            if (persistedBet is null || !table.GetPlayer(message.PlayerId, out var acceptedPlayer) || acceptedPlayer is null)
                return false;

            table.SetPlayerBalance(acceptedPlayer, message.UpdatedBalance);
            persistedBet.Player.BalanceInUSD = message.UpdatedBalance;
            await publisher.PublishAsync(new BetAccepted
            {
                TableId = table.TableId,
                MessageId = persistedBet.BetId,
                PlayerId = message.PlayerId,
                UpdatedBalance = message.UpdatedBalance,
                Bet = persistedBet
            }, ct);
            return true;
        }

        if (message.ResultType != DbWorkerResultMessageType.BetSettled ||
            message.SettlementStatus is null || message.SettledAt is null)
            return false;

        _pendingSettlements.TryRemove(message.BetId, out _);

        if (message.SettlementStatus == BetSettlementStatus.Lost)
        {
            var currentBet = table.GetPlayerBet(messageRoundId, message.PlayerId);
            return currentBet?.Status == BetStatus.Lost ||
                   table.ApplyCommittedLoss(message.BetId, message.SettledAt.Value);
        }

        if (message.SettlementStatus != BetSettlementStatus.CashedOut ||
            message.CashoutMultiplier is null)
            return false;

        var applied = table.ApplyCommittedCashout(
            message.BetId,
            message.CashoutMultiplier.Value,
            message.PayoutAmount,
            message.UpdatedBalance,
            message.SettledAt.Value);

        if (!applied)
            return table.GetPlayerBet(messageRoundId, message.PlayerId)?.Status == BetStatus.CashedOut;

        await PublishCashoutAsync(message, table.TableId, ct);
        return true;
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
        return publisher.PublishAsync(new BetCashedOut
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
