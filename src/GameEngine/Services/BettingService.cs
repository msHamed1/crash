using Crash.Contracts.Messaging.EngineToGateway.Bets;
using Crash.Domain.Entities;
using Crash.Domain.State;
using Crash.Persistence.Repositories;
using Crash.Persistence.Results;
using GameEngine.Messaging;
using GameEngine.Application.Commands.Bets;

namespace GameEngine.Services;

public sealed class BettingService(
    IClientMessagePublisher publisher,
    ILogger<BettingService> logger,
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
            await PublishRejected(command, table.TableId, playerId,
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
            await PublishRejected(command, table.TableId, playerId,
                "UNSUPPORTED_CURRENCY", "Only USD bets are currently supported.", 0, ct);
            return;
        }

        var currentRound = table.GetCurrentRoundSnapshot();
        if (currentRound?.RoundId != roundId)
        {
            await PublishRejected(command, table.TableId, playerId,
                "ROUND_NOT_BETTABLE", "The round is no longer accepting bets.", 0, ct);
            return;
        }

        if (!table.GetPlayer(playerId, out var player) || player is null)
        {
            await PublishRejected(command, table.TableId, playerId,
                "PLAYER_NOT_AT_TABLE", "The player has not joined this table.", 0, ct);
            return;
        }

        // BetId is the request correlation ID, making redelivery idempotent at the unique DB index.
        var bet = table.AddNewBet(
            player,
            command.Amount,
            roundId,
            command.CorrelationId,
            command.Currency.ToUpperInvariant());

        if (bet is null)
        {
            var existingBet = table.GetPlayerBet(roundId, playerId);
            if (existingBet?.BetId == command.CorrelationId)
            {
                await PublishAccepted(command, table.TableId, playerId, player.Balance, existingBet, ct);
                return;
            }

            await PublishRejected(command, table.TableId, playerId,
                "BET_REJECTED", "The bet is invalid, duplicated, or betting is closed.", player.Balance, ct);
            return;
        }

        PlaceBetResult result;
        try
        {
            using var scope = scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IBetRepository>();
            result = await repository.TryPlaceBetAsync(
                bet,
                table.TableId,
                table.FencingToken,
                ct);
        }
        catch (Exception exception)
        {
            // A failed DB transaction must release the balance reserved in runtime state.
            table.RollbackBetInMemory(bet, player);
            logger.LogError(exception,
                "Failed to place bet {BetId} for player {PlayerId} in round {RoundId}.",
                bet.BetId, playerId, roundId);
            await PublishRejected(command, table.TableId, playerId,
                "BET_PROCESSING_FAILED", "The bet could not be processed.", player.Balance, ct);
            return;
        }

        if (!result.IsAccepted || result.Bet is null)
        {
            table.RollbackBetInMemory(bet, player);
            await PublishRejected(command, table.TableId, playerId,
                result.Code, result.Message, player.Balance, ct);
            return;
        }

        table.SetPlayerBalance(player, result.UpdatedBalance);
        await PublishAccepted(
            command,
            table.TableId,
            playerId,
            result.UpdatedBalance,
            result.Bet,
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
        PlaceBetCommand command,
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
            MessageId = command.CorrelationId,
            PlayerId = playerId,
            UpdatedBalance = updatedBalance,
            Code = code,
            Reason = reason
        }, ct);
    }
}
