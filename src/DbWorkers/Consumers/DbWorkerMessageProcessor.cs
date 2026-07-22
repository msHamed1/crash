using Crash.Contracts.Messaging.DbWorkers;
using Crash.Domain.Entities;
using Crash.Persistence;
using DbWorkers.Application;
using Microsoft.EntityFrameworkCore;

namespace DbWorkers.Consumers;
/// <summary>
/// Persists one durable DB-worker message.
/// Implementations must return only after the database transaction commits,
/// or after confirming that the MessageId was already processed.
/// Transient database failures should be thrown so the RabbitMQ consumer can
/// retry the message. Permanent conflicts should throw
/// <see cref="PermanentDbMessageException"/>.
/// </summary>
public interface IDbWorkerMessageProcessor
{
    Task<DbMessageProcessResult> ProcessAsync(
        DbWorkerMessageEnvelope message,
        CancellationToken cancellationToken);
}

public sealed class DbWorkerMessageProcessor(
    DataContext db,
    ILogger<DbWorkerMessageProcessor> logger)
    : IDbWorkerMessageProcessor
{
    public async Task<DbMessageProcessResult> ProcessAsync(
        DbWorkerMessageEnvelope message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (await WasAlreadyProcessed(message.MessageId, cancellationToken))
            return DbMessageProcessResult.AlreadyProcessed;

        await using var transaction =
            await db.Database.BeginTransactionAsync(cancellationToken);

        try
        {
            // Check again inside the transaction. The unique primary key remains the
            // final protection if two deliveries race this check.
            if (await WasAlreadyProcessed(message.MessageId, cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return DbMessageProcessResult.AlreadyProcessed;
            }
 
            switch (message)
            {
                case
                {
                    Type: DbWorkerMessageType.BetAccepted,
                    Payload: BetAcceptedForPersistence accepted
                }:
                    await PersistAcceptedBet(accepted, cancellationToken);
                    break;

                case
                {
                    Type: DbWorkerMessageType.BetSettled,
                    Payload: BetSettledForPersistence settled
                }:
                    await PersistSettledBet(settled, cancellationToken);
                    break;

                default:
                    throw new InvalidDbMessageException(
                        $"Message type {message.Type} does not match payload " +
                        $"{message.Payload.GetType().Name}.");
            }

            db.ProcessedDbMessages.Add(new ProcessedDbMessage
            {
                MessageId = message.MessageId,
                MessageType = message.Type.ToString(),
                TableId = message.Payload.TableId,
                RoundId = message.Payload.RoundId,
                Sequence = message.Payload.Sequence,
                ProcessedAt = DateTimeOffset.UtcNow
            });

            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            logger.LogInformation(
                "Persisted DB-worker message {MessageId} of type {MessageType}.",
                message.MessageId,
                message.Type);

            return DbMessageProcessResult.Committed;
        }
        catch (DbUpdateException)
        {
            await transaction.RollbackAsync(CancellationToken.None);
            db.ChangeTracker.Clear();

            // A delivery may be committed by another worker just before our
            // insert. In that case the unique inbox key converts the race into
            // an idempotent success.
            if (await WasAlreadyProcessed(message.MessageId, cancellationToken))
                return DbMessageProcessResult.AlreadyProcessed;

            throw;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private Task<bool> WasAlreadyProcessed(
        Guid messageId,
        CancellationToken cancellationToken)
    {
        return db.ProcessedDbMessages
            .AsNoTracking()
            .AnyAsync(message => message.MessageId == messageId, cancellationToken);
    }

    private async Task PersistAcceptedBet(
        BetAcceptedForPersistence message,
        CancellationToken cancellationToken)
    {
        ValidateAcceptedMessage(message);
        if (message.PlayerId == 1)
        {
            await Task.Delay(TimeSpan.FromSeconds(8), cancellationToken);
            
        }
        var existingBet = await db.Bets
            .FromSqlInterpolated(
                $"SELECT * FROM Bets WHERE BetId = {message.BetId} FOR UPDATE")
            .SingleOrDefaultAsync(cancellationToken);

        if (existingBet is not null)
        {
            EnsureSameAcceptedBet(existingBet, message);
            return;
        }

        var round = await db.Rounds
            .AsNoTracking()
            .SingleOrDefaultAsync(round => round.Id == message.RoundId, cancellationToken);

        if (round is null)
        {
            // The round-created event may still be ahead of this event in the
            // durable queue. A normal exception tells the consumer to retry.
            throw new InvalidOperationException(
                $"Round {message.RoundId} has not been persisted yet.");
        }

        if (round.TableId != message.TableId ||
            round.FencingToken != message.FencingToken)
        {
            throw new PermanentDbMessageException(
                $"Bet {message.BetId} does not match the persisted round ownership.");
        }

        var player = await db.Players
            .FromSqlInterpolated(
                $"SELECT * FROM Players WHERE Id = {message.PlayerId} FOR UPDATE")
            .SingleOrDefaultAsync(cancellationToken);

        if (player is null)
        {
            throw new PermanentDbMessageException(
                $"Player {message.PlayerId} does not exist for accepted bet {message.BetId}.");
        }

        if (player.BalanceInUSD < message.StakeAmount)
        {
            throw new PermanentDbMessageException(
                $"Player {message.PlayerId} has insufficient persisted balance " +
                $"for operator-accepted bet {message.BetId}.");
        }

        player.BalanceInUSD -= message.StakeAmount;

        db.Bets.Add(new Bet
        {
            BetId = message.BetId,
            PlayerId = message.PlayerId,
            RoundId = message.RoundId,
            StakeAmount = message.StakeAmount,
            Currency = message.Currency.ToUpperInvariant(),
            AutoCashoutEnabled = message.AutoCashoutMultiplier.HasValue,
            AutoCashoutMultiplier = message.AutoCashoutMultiplier,
            Status = BetStatus.Accepted,
            PersistenceSequence = message.Sequence,
            AcceptedAt = message.AcceptedAt,
            CreatedAt = message.AcceptedAt
        });
    }

    private async Task PersistSettledBet(
        BetSettledForPersistence message,
        CancellationToken cancellationToken)
    {
        ValidateSettledMessage(message);

        var bet = await db.Bets
            .FromSqlInterpolated(
                $"SELECT * FROM Bets WHERE BetId = {message.BetId} FOR UPDATE")
            .SingleOrDefaultAsync(cancellationToken);

        if (bet is null)
        {
            // Acceptance may be in front of this event in another worker. Retry
            // rather than turning an operator-accepted settlement into a DLQ item.
            throw new InvalidOperationException(
                $"Accepted bet {message.BetId} has not been persisted yet.");
        }

        if (bet.PlayerId != message.PlayerId || bet.RoundId != message.RoundId)
        {
            throw new PermanentDbMessageException(
                $"Settlement identity does not match bet {message.BetId}.");
        }

        var roundMatches = await db.Rounds
            .AsNoTracking()
            .AnyAsync(round =>
                    round.Id == message.RoundId &&
                    round.TableId == message.TableId &&
                    round.FencingToken == message.FencingToken,
                cancellationToken);

        if (!roundMatches)
        {
            throw new PermanentDbMessageException(
                $"Settlement for bet {message.BetId} does not match round ownership.");
        }

        var targetStatus = ToBetStatus(message.Status);

        if (IsTerminal(bet.Status))
        {
            EnsureSameSettlement(bet, message, targetStatus);
            return;
        }

        if (bet.Status != BetStatus.Accepted)
        {
            throw new PermanentDbMessageException(
                $"Bet {message.BetId} cannot transition from {bet.Status} to {targetStatus}.");
        }

        if (message.Sequence <= bet.PersistenceSequence)
        {
            throw new PermanentDbMessageException(
                $"Settlement sequence {message.Sequence} is not newer than " +
                $"{bet.PersistenceSequence} for bet {message.BetId}.");
        }

        var player = await db.Players
            .FromSqlInterpolated(
                $"SELECT * FROM Players WHERE Id = {message.PlayerId} FOR UPDATE")
            .SingleOrDefaultAsync(cancellationToken);

        if (player is null)
        {
            throw new PermanentDbMessageException(
                $"Player {message.PlayerId} does not exist for settlement {message.BetId}.");
        }

        player.BalanceInUSD += message.PayoutAmount;

        bet.Status = targetStatus;
        bet.PayoutAmount = message.PayoutAmount;
        bet.Pl = message.ProfitLoss;
        bet.CashedOutAtMultiplier = message.CashoutMultiplier;
        bet.CashedOutAt = targetStatus == BetStatus.CashedOut
            ? message.SettledAt
            : null;
        bet.SettledAt = message.SettledAt;
        bet.PersistenceSequence = message.Sequence;
    }

    private static void ValidateAcceptedMessage(BetAcceptedForPersistence message)
    {
        if (string.IsNullOrWhiteSpace(message.BetId) ||
            message.PlayerId <= 0 ||
            message.TableId <= 0 ||
            message.RoundId <= 0 ||
            message.StakeAmount <= 0 ||
            string.IsNullOrWhiteSpace(message.Currency) ||
            message.FencingToken <= 0 ||
            message.Sequence <= 0)
        {
            throw new InvalidDbMessageException("The accepted-bet payload is invalid.");
        }

        if (message.AutoCashoutMultiplier is < 1.00m)
            throw new InvalidDbMessageException("Auto-cashout multiplier must be at least 1.00.");
    }

    private static void ValidateSettledMessage(BetSettledForPersistence message)
    {
        if (string.IsNullOrWhiteSpace(message.BetId) ||
            message.PlayerId <= 0 ||
            message.TableId <= 0 ||
            message.RoundId <= 0 ||
            message.PayoutAmount < 0 ||
            message.FencingToken <= 0 ||
            message.Sequence <= 0)
        {
            throw new InvalidDbMessageException("The settled-bet payload is invalid.");
        }

        if (message.Status == BetSettlementStatus.Lost && message.PayoutAmount != 0)
            throw new InvalidDbMessageException("A lost bet cannot have a payout.");

        if (message.Status == BetSettlementStatus.CashedOut &&
            message.CashoutMultiplier is null or < 1.00m)
        {
            throw new InvalidDbMessageException(
                "A cashed-out bet requires a multiplier of at least 1.00.");
        }
    }

    private static void EnsureSameAcceptedBet(
        Bet bet,
        BetAcceptedForPersistence message)
    {
        if (bet.PlayerId != message.PlayerId ||
            bet.RoundId != message.RoundId ||
            bet.StakeAmount != message.StakeAmount ||
            !string.Equals(bet.Currency, message.Currency, StringComparison.OrdinalIgnoreCase))
        {
            throw new PermanentDbMessageException(
                $"BetId {message.BetId} already exists with different immutable values.");
        }
    }

    private static void EnsureSameSettlement(
        Bet bet,
        BetSettledForPersistence message,
        BetStatus targetStatus)
    {
        if (bet.Status != targetStatus ||
            bet.PayoutAmount != message.PayoutAmount ||
            bet.Pl != message.ProfitLoss ||
            bet.CashedOutAtMultiplier != message.CashoutMultiplier)
        {
            throw new PermanentDbMessageException(
                $"Bet {message.BetId} already has a different terminal result.");
        }
    }

    private static BetStatus ToBetStatus(BetSettlementStatus status)
    {
        return status switch
        {
            BetSettlementStatus.CashedOut => BetStatus.CashedOut,
            BetSettlementStatus.Lost => BetStatus.Lost,
            BetSettlementStatus.Won => BetStatus.Won,
            BetSettlementStatus.Cashback => BetStatus.Cashback,
            _ => throw new InvalidDbMessageException(
                $"Unsupported settlement status {status}.")
        };
    }

    private static bool IsTerminal(BetStatus status)
    {
        return status is BetStatus.CashedOut
            or BetStatus.Lost
            or BetStatus.Won
            or BetStatus.Cashback;
    }
}
