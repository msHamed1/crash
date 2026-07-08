namespace Crash.Domain.Contracts.Commands;

public abstract record RoundCommand
{
    public string TableId { get; init; } = "";
    public string RoundId { get; init; } = "";
}

public sealed record PlayerBetCommand(
    string PlayerId,
    // string TableId,
    // string RoundId,
    decimal Amount
) : RoundCommand;

public sealed record PlayerCashOutCommand(
    string PlayerId,
    // string TableId,
    // string RoundId,
    decimal CurrentMultiplier,
    string BetId
) : RoundCommand;

public sealed record RoundTickCommand(
    // string TableId,
    // string RoundId,
    decimal CurrentMultiplier
    ):RoundCommand;

public sealed record RoundCrashCommand(
    // string TableId,
    // string RoundId,
    decimal CurrentMultiplier
    ):RoundCommand;


    
    