using Crash.Contracts.Messaging.DbWorkers;
using Crash.Contracts.Messaging.EngineToGateway.Bets;
using Crash.Domain.Entities;
using Crash.Domain.Options;
using Crash.Domain.State;
using Crash.Persistence.Repositories;
using Crash.Persistence.Results;
using Crash.Persistence.Results.Settlement;
using GameEngine.Messaging;
using GameEngine.Application.Commands.Bets;
using GameEngine.Messaging.Publishers;

namespace GameEngine.Services;

public sealed class BettingService(
    IWssGatewayPublisher publisher,
    ILogger<BettingService> logger,
    GameEngineOptions options,
    IDbWorkerPublisher  dbWorkerPublisher,
    IServiceScopeFactory scopeFactory)
{
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
            // using var scope = scopeFactory.CreateScope();
            // var repository = scope.ServiceProvider.GetRequiredService<IBetRepository>();
            // result = await repository.TryPlaceBetAsync(
            //     bet,
            //     table.TableId,
            //     table.FencingToken,
            //     ct);
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

        table.SetPlayerBalance(player,  bet.Player.BalanceInUSD);
        await PublishAccepted(
            command,
            table.TableId,
            playerId,
            bet.Player.BalanceInUSD,
            bet,
            ct);
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

        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IBetRepository>();
        var result = await repository.TrySettleCashoutAsync(
            command.BetId,
            playerId,
            roundId,
            round.CurrentMultiplier,
            false,
            table.TableId,
            table.FencingToken,
            ct);

        if (!result.Succeeded)
            return;

        table.ApplyCommittedCashout(
            result.BetId,
            result.CashoutMultiplier,
            result.PayoutAmount,
            result.UpdatedBalance,
            result.SettledAt);

        await PublishCashoutAsync(result, table.TableId, ct);
    }

    public bool ProcessBetPersistenceCompleted(
        DbBetPersistenceCompletedCommand message)
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

        var bet = round.GetBet(message.PlayerId);

        if (bet is null)
        {
            return false;
        }

        if (message.IsCreated)
        {
            table.SetBetIsPersisted(message.PlayerId, messageRoundId);
            return true;
        }

        table.GetPlayer(message.PlayerId, out var player);

        var cancelledBet = table.TryCancelBet(
            playerId: message.PlayerId,
            roundId: messageRoundId);

           PublishRejected(Guid.NewGuid().ToString(), table.TableId, player.PlayerId,
            "BET_PROCESSING_FAILED", "The bet could not be processed.",  player.Balance, CancellationToken.None);
         return cancelledBet is not null;
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
            using var scope = scopeFactory.CreateScope();

            var repository =
                scope.ServiceProvider.GetRequiredService<IBetRepository>();

            var result = await repository.TrySettleCashoutAsync(
                candidate.BetId,
                candidate.PlayerId,
                candidate.RoundId,
                candidate.CashoutMultiplier,
                true,
                table.TableId,
                table.FencingToken,
                ct);

            if (!result.Succeeded)
                continue;

            // Update memory only after the DB transaction commits.
            table.ApplyCommittedCashout(
                result.BetId,
                result.CashoutMultiplier,
                result.PayoutAmount,
                result.UpdatedBalance,
                result.SettledAt);

            // Publish successful cashout message after memory is synchronized.
            await PublishCashoutAsync(result, table.TableId, ct);
        }
    }

    private Task PublishCashoutAsync(
        CashoutSettlementResult result,
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
            RoundId = result.RoundId,
            CashoutMultiplier = result.CashoutMultiplier,
            PayoutAmount = result.PayoutAmount,
            NetResultAmount = result.NetResultAmount,
            UpdatedBalance = result.UpdatedBalance,
            CashedOutAt = result.SettledAt
        }, ct);
    }
    
    
    public async Task SettleCrashedRoundAsync(
        TableRuntimeState table,
        long roundId,
        CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();

        var repository =
            scope.ServiceProvider.GetRequiredService<IBetRepository>();

        var results =
            await repository.SettleOpenBetsAsLostAsync(
                roundId,
                table.TableId,
                table.FencingToken,
                ct);

        foreach (var result in results)
        {
            table.ApplyCommittedLoss(
                result.BetId,
                result.SettledAt);
        }
    }
}
