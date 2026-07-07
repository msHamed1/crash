using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.RoundMessages;

public sealed record CrashEvent : MessageEnvelope<CrashData>
{
    public override string MessageType => "crash";
}

public sealed record CrashData
{
    public decimal CrashMultiplier { get; init; }
    public long RoundIndex { get; init; }
    public string RoundId { get; init; } = default!;
}