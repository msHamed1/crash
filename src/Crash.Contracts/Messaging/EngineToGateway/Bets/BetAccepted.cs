using Crash.Domain.Entities;

namespace Crash.Contracts.Messaging.EngineToGateway.Bets;

public sealed record BetAccepted : GatewayMessage
{
    // Preserve the existing wire value while the CLR type uses the clearer convention.
    public override string MessageType => "PlayerBetAccepted";
    public long PlayerId { get; init; }
    public decimal UpdatedBalance { get; init; }
    public required Bet Bet { get; init; }
}
