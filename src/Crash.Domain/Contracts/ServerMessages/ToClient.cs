using Crash.Domain.Entities;

namespace Crash.Domain.Contracts.ServerMessages;

public abstract record ToClient
{
    public abstract string MessageType { get; }
    public long TableId { get; init; }
    public string MessageId { get; init; }
     
}

public record NewRoundInfo : ToClient
{
    public override string MessageType => "NewRoundInfo";
    
    public required long RoundId { get; set; }
    public decimal CurrentMultiplier { get; set; } = 1.00m;
    public DateTimeOffset StartsAt { get; set; }
    public bool IsCrashed { get; set; }
}

public record CurrentState: ToClient
{
    public override string MessageType => "CurrentState";
    public long PlayerId { get; set; }
    public required long RoundId { get; set; }
    public decimal CurrentMultiplier { get; set; } = 1.00m;
    public DateTimeOffset StartsAt { get; set; }
    public bool IsCrashed { get; set; }

    public string ConnectionId { get; set; }
}

public record RoundTick :ToClient
{
    public override string MessageType => "RoundTick";
    public long RoundId { get; init; }
    public decimal CurrentMultiplier { get; init; }
    
}


public record RoundCrashed: ToClient
{
    public override string MessageType => "RoundCrashed";
    public required long RoundId { get; set; }
    public decimal CurrentMultiplier { get; set; } = 1.00m;
    public bool IsCrashed { get; set; }

 }
