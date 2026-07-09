namespace Crash.Domain.Contracts.Commands;

public abstract record RoundCommand
{
    public abstract string MessageType { get; }
    public string TableId { get; init; } = "";
    public string RoundId { get; init; } = "";
}

public sealed record PlayerBetCommand : RoundCommand
{
    public string PlayerId {get; set; }
    // string TableId,
    // string RoundId,
    public decimal Amount  {get; set; }
    public override string MessageType => "Bet";
}

public sealed record PlayerCashOutCommand : RoundCommand
{
    public string PlayerId {get; set; }
    // string TableId,
    // string RoundId,
    public decimal CurrentMultiplier {get; set; }
    public string BetId {get; set; }
    public override string MessageType => "Cashout";

}

public sealed record RoundTickCommand : RoundCommand
{
    // string TableId,
    // string RoundId,
    public decimal CurrentMultiplier {get; set; }
    public override string MessageType => "Tick";
}

public sealed record RoundCrashCommand : RoundCommand
{
    // string TableId,
    // string RoundId,
    public decimal CurrentMultiplier {get; set; }
    public override string MessageType => "Crash";

}


public sealed record PlayerJoinedCommand : RoundCommand
{
    // string TableId,
    // string RoundId,
    public string PlayerId {get; set; }
    public override string MessageType => "Joined";

}

public sealed record NewRoundCommand : RoundCommand
{
    public override string MessageType => "New";
}


    
    