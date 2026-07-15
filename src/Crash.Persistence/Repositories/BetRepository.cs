using Crash.Domain.Entities;
using Crash.Persistence.Results;
using Crash.Persistence.Results.Settlement;
using Microsoft.EntityFrameworkCore;

namespace Crash.Persistence.Repositories;

public interface IBetRepository
{
    Task<Bet> UpdateBet(Bet bet, CancellationToken ct);
    Task<Bet?> GetBet(string betId, CancellationToken ct);
    Task<List<Bet>> GetPlayerBets(long playerId, CancellationToken ct);
    Task<bool> Exists(string betId, CancellationToken ct);
    Task<Bet?> GetPendingBet(string betId, CancellationToken ct);
    Task<PlaceBetResult> TryPlaceBetAsync(
        Bet bet,
        long tableId,
        long fencingToken,
        CancellationToken ct);
    
    Task<CashoutSettlementResult> TrySettleCashoutAsync(
        string betId,
        long playerId,
        long roundId,
        decimal cashoutMultiplier,
        bool requireAutoCashoutConfiguration,
        long tableId,
        long fencingToken,
        CancellationToken ct);
    
    Task<IReadOnlyList<LostBetSettlementResult>>
        SettleOpenBetsAsLostAsync(
            long roundId,
            long tableId,
            long fencingToken,
            CancellationToken ct);
}

public class BetRepository(DataContext db) : IBetRepository
{
    public async Task<PlaceBetResult> TryPlaceBetAsync(
        Bet bet,
        long tableId,
        long fencingToken,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(bet);

        // The impossible Condition because we already check programmatically 
        if (bet.StakeAmount <= 0)
            throw new ArgumentOutOfRangeException(nameof(bet.StakeAmount));

        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        try
        {
            // Lock ownership first. A former engine must not commit bets after losing its lease.
            var table = await db.Tables
                .FromSqlInterpolated($"SELECT * FROM Tables WHERE Id = {tableId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (table is null
                || table.FencingToken != fencingToken
                || table.LeaseExpiresAt is null
                || table.LeaseExpiresAt <= DateTimeOffset.UtcNow)
            {
                await transaction.RollbackAsync(ct);
                return PlaceBetResult.Rejected(
                    PlaceBetStatus.TableOwnershipLost,
                    "This engine no longer owns the table.");
            }

            // Serializing on the round row prevents a bet from racing the transition to Running.
            var round = await db.Rounds
                .FromSqlInterpolated($"SELECT * FROM Rounds WHERE Id = {bet.RoundId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (round is null
                || round.TableId != tableId
                || round.FencingToken != fencingToken
                || round.Status != RoundStatus.Betting)
            {
                await transaction.RollbackAsync(ct);
                return PlaceBetResult.Rejected(
                    PlaceBetStatus.RoundNotBettable,
                    "The round is no longer accepting bets.");
            }

            // This predicate is the authoritative balance check and prevents concurrent overspending.
            var affectedPlayers = await db.Players
                .Where(player => player.Id == bet.PlayerId && player.BalanceInUSD >= bet.StakeAmount)
                .ExecuteUpdateAsync(setters => setters
                        .SetProperty(
                            player => player.BalanceInUSD,
                            player => player.BalanceInUSD - bet.StakeAmount),
                    ct);

            if (affectedPlayers != 1)
            {
                var playerExists = await db.Players
                    .AnyAsync(player => player.Id == bet.PlayerId, ct);

                await transaction.RollbackAsync(ct);
                return PlaceBetResult.Rejected(
                    playerExists ? PlaceBetStatus.InsufficientBalance : PlaceBetStatus.PlayerNotFound,
                    playerExists
                        ? "The player does not have enough balance for this bet."
                        : "The player does not exist.");
            }

            bet.Status = BetStatus.Accepted;
            await db.Bets.AddAsync(bet, ct);
            await db.SaveChangesAsync(ct);

            var updatedBalance = await db.Players
                .AsNoTracking()
                .Where(player => player.Id == bet.PlayerId)
                .Select(player => player.BalanceInUSD)
                .SingleAsync(ct);

            await transaction.CommitAsync(ct);
            return PlaceBetResult.Success(bet, updatedBalance);
        }
        catch (DbUpdateException exception) when (IsDuplicateBetException(exception))
        {
            await transaction.RollbackAsync(CancellationToken.None);

            // A broker redelivery after an engine restart may not exist in runtime memory.
            // Return the original success only when every immutable request field matches.
            db.ChangeTracker.Clear();
            var existingBet = await db.Bets
                .AsNoTracking()
                .SingleOrDefaultAsync(existing => existing.BetId == bet.BetId, ct);

            if (existingBet is not null
                && existingBet.PlayerId == bet.PlayerId
                && existingBet.RoundId == bet.RoundId
                && existingBet.StakeAmount == bet.StakeAmount
                && existingBet.Currency == bet.Currency
                && existingBet.Status == BetStatus.Accepted)
            {
                var currentBalance = await db.Players
                    .AsNoTracking()
                    .Where(player => player.Id == bet.PlayerId)
                    .Select(player => player.BalanceInUSD)
                    .SingleAsync(ct);

                return PlaceBetResult.Success(existingBet, currentBalance);
            }

            return PlaceBetResult.Rejected(
                PlaceBetStatus.DuplicateBet,
                "This bet request has already been processed.");
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static bool IsDuplicateBetException(DbUpdateException exception)
    {
        return exception.InnerException is MySqlConnector.MySqlException
        {
            ErrorCode: MySqlConnector.MySqlErrorCode.DuplicateKeyEntry
        };
    }

    public async Task<CashoutSettlementResult> TrySettleCashoutAsync(
        string betId,
        long playerId,
        long roundId,
        decimal cashoutMultiplier,
        bool requireAutoCashoutConfiguration,
        long tableId,
        long fencingToken,
        CancellationToken ct)
    {
        if (cashoutMultiplier < 1.00m)
            throw new ArgumentOutOfRangeException(nameof(cashoutMultiplier));

        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        try
        {
            // Use the same lock order as bet placement: table, round, then bet.
            var table = await db.Tables
                .FromSqlInterpolated($"SELECT * FROM Tables WHERE Id = {tableId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (table is null
                || table.FencingToken != fencingToken
                || table.LeaseExpiresAt is null
                || table.LeaseExpiresAt <= DateTimeOffset.UtcNow)
            {
                await transaction.RollbackAsync(ct);
                return FailedCashout(betId);
            }

            var round = await db.Rounds
                .FromSqlInterpolated($"SELECT * FROM Rounds WHERE Id = {roundId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (round is null
                || round.TableId != tableId
                || round.FencingToken != fencingToken
                || round.StartTime > DateTimeOffset.UtcNow
                || round.Status is not (RoundStatus.Betting or RoundStatus.Running))
            {
                await transaction.RollbackAsync(ct);
                return FailedCashout(betId);
            }

            var bet = await db.Bets
                .FromSqlInterpolated($"SELECT * FROM Bets WHERE BetId = {betId} FOR UPDATE")
                .SingleOrDefaultAsync(ct);

            if (bet is null || bet.PlayerId != playerId || bet.RoundId != roundId)
            {
                await transaction.RollbackAsync(ct);
                return FailedCashout(betId);
            }

            // A retried command returns the committed result without crediting the player twice.
            if (bet.Status == BetStatus.CashedOut
                && bet.CashedOutAtMultiplier == cashoutMultiplier
                && bet.SettledAt.HasValue)
            {
                var existingBalance = await db.Players
                    .AsNoTracking()
                    .Where(player => player.Id == playerId)
                    .Select(player => player.BalanceInUSD)
                    .SingleAsync(ct);

                await transaction.CommitAsync(ct);
                return SuccessfulCashout(bet, existingBalance);
            }

            if (bet.Status != BetStatus.Accepted
                || (requireAutoCashoutConfiguration
                    && (bet.AutoCashoutEnabled != true
                        || bet.AutoCashoutMultiplier != cashoutMultiplier)))
            {
                await transaction.RollbackAsync(ct);
                return FailedCashout(betId);
            }

            var settledAt = DateTimeOffset.UtcNow;
            var payoutAmount = bet.StakeAmount * cashoutMultiplier;

            bet.Status = BetStatus.CashedOut;
            bet.CashedOutAtMultiplier = cashoutMultiplier;
            bet.CashedOutAt = settledAt;
            bet.SettledAt = settledAt;
            bet.PayoutAmount = payoutAmount;
            bet.Pl = payoutAmount - bet.StakeAmount;

            var affectedPlayers = await db.Players
                .Where(player => player.Id == playerId)
                .ExecuteUpdateAsync(setters => setters
                        .SetProperty(
                            player => player.BalanceInUSD,
                            player => player.BalanceInUSD + payoutAmount),
                    ct);

            if (affectedPlayers != 1)
            {
                await transaction.RollbackAsync(ct);
                return FailedCashout(betId);
            }

            await db.SaveChangesAsync(ct);

            var updatedBalance = await db.Players
                .AsNoTracking()
                .Where(player => player.Id == playerId)
                .Select(player => player.BalanceInUSD)
                .SingleAsync(ct);

            await transaction.CommitAsync(ct);
            return SuccessfulCashout(bet, updatedBalance);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    public async Task<IReadOnlyList<LostBetSettlementResult>> SettleOpenBetsAsLostAsync(
        long roundId,
        long tableId,
        long fencingToken,
        CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        try
        {
            var table = await db.Tables
                .FromSqlInterpolated($"SELECT * FROM Tables WHERE Id = {tableId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (table is null
                || table.FencingToken != fencingToken
                || table.LeaseExpiresAt is null
                || table.LeaseExpiresAt <= DateTimeOffset.UtcNow)
            {
                await transaction.RollbackAsync(ct);
                return [];
            }

            var round = await db.Rounds
                .FromSqlInterpolated($"SELECT * FROM Rounds WHERE Id = {roundId} FOR UPDATE")
                .AsNoTracking()
                .SingleOrDefaultAsync(ct);

            if (round is null
                || round.TableId != tableId
                || round.FencingToken != fencingToken
                || round.Status != RoundStatus.Crashed)
            {
                await transaction.RollbackAsync(ct);
                return [];
            }

            // Lock every bet in the round so manual/automatic cashout cannot race loss settlement.
            var roundBets = await db.Bets
                .FromSqlInterpolated($"SELECT * FROM Bets WHERE RoundId = {roundId} FOR UPDATE")
                .ToListAsync(ct);

            var settledAt = DateTimeOffset.UtcNow;
            var openBets = roundBets
                .Where(bet => bet.Status == BetStatus.Accepted)
                .ToArray();

            foreach (var bet in openBets)
            {
                bet.Status = BetStatus.Lost;
                bet.PayoutAmount = 0;
                bet.Pl = -bet.StakeAmount;
                bet.SettledAt = settledAt;
            }

            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);

            return openBets
                .Select(bet => new LostBetSettlementResult(
                    bet.BetId,
                    bet.PlayerId,
                    settledAt))
                .ToArray();
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static CashoutSettlementResult FailedCashout(string betId) => new()
    {
        Succeeded = false,
        BetId = betId
    };

    private static CashoutSettlementResult SuccessfulCashout(Bet bet, decimal updatedBalance) => new()
    {
        Succeeded = true,
        BetId = bet.BetId,
        PlayerId = bet.PlayerId,
        RoundId = bet.RoundId,
        CashoutMultiplier = bet.CashedOutAtMultiplier!.Value,
        PayoutAmount = bet.PayoutAmount,
        NetResultAmount = bet.Pl,
        UpdatedBalance = updatedBalance,
        SettledAt = bet.SettledAt!.Value
    };

    public async Task<Bet> UpdateBet(Bet bet, CancellationToken ct)
    {
        db.Bets.Update(bet);
        await db.SaveChangesAsync(ct);
        return bet;
    }

    public Task<Bet?> GetBet(string betId, CancellationToken ct)
    {
        return db.Bets.FirstOrDefaultAsync(bet => bet.BetId == betId, ct);
    }

    public Task<List<Bet>> GetPlayerBets(long playerId, CancellationToken ct)
    {
        return db.Bets
            .Where(bet => bet.PlayerId == playerId)
            .OrderByDescending(bet => bet.CreatedAt)
            .ToListAsync(ct);
    }

    public Task<bool> Exists(string betId, CancellationToken ct)
    {
        return db.Bets.AnyAsync(bet => bet.BetId == betId, ct);
    }

    public Task<Bet?> GetPendingBet(string betId, CancellationToken ct)
    {
        return db.Bets.FirstOrDefaultAsync(
            bet => bet.BetId == betId && bet.Status == BetStatus.Placed,
            ct);
    }
}
