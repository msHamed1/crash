using Crash.Domain.Contracts.Common;

namespace Crash.Domain.Contracts.RoundMessages;

public sealed record NewRoundEvent : MessageEnvelope<NewRoundData>
{
    public override string MessageType => "new-round";
}

public sealed record NewRoundData
{
    public long RoundIndex { get; init; }
    public string RoundId { get; init; } = default!;
    public TimingConfig TimingConfig { get; init; } = default!;
}