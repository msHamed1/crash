using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.RoundMessages;

public sealed record NewMultiplierEvent : MessageEnvelope<NewMultiplierData>
{
    public override string MessageType => "new-multiplier";
}

public sealed record NewMultiplierData
{
    public decimal CurrentMultiplier { get; init; }
    public long ElapsedTimeFromStart { get; init; }
}