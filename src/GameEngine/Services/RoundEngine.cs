using Crash.Rng;

namespace GameEngine.Services;

public sealed record StartRoundRequest(
    string? TableId,
    string? RoundId,
    string? ClientSeed,
    ulong? Nonce);

public sealed record StartedRoundResponse(
    string TableId,
    string RoundId,
    string RngId,
    string ServerSeedHash,
    string EntropyHex,
    string ClientSeed,
    ulong Nonce,
    string RngAlgorithmVersion,
    decimal CrashPoint,
    DateTimeOffset CreatedAt);

public sealed class RoundEngine(Rng.RngClient rngClient)
{
    public async Task<StartedRoundResponse> StartRoundAsync(
        StartRoundRequest request,
        CancellationToken cancellationToken)
    {
        var tableId = string.IsNullOrWhiteSpace(request.TableId)
            ? "default-table"
            : request.TableId.Trim();

        var roundId = string.IsNullOrWhiteSpace(request.RoundId)
            ? $"round-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}"
            : request.RoundId.Trim();
        var clientSeed = string.IsNullOrWhiteSpace(request.ClientSeed)
            ? Guid.NewGuid().ToString("N")
            : request.ClientSeed.Trim();

        var entropy = await rngClient.GenerateRoundEntropyAsync(
            new GenerateRoundEntropyRequest
            {
                TableId = tableId,
                RoundId = roundId,
                ClientSeed = clientSeed,
                Nonce = request.Nonce ?? 0UL
            },
            cancellationToken: cancellationToken);

        return new StartedRoundResponse(
            entropy.TableId,
            entropy.RoundId,
            entropy.RngId,
            entropy.ServerSeedHash,
            entropy.EntropyHex,
            entropy.ClientSeed,
            entropy.Nonce,
            entropy.RngAlgorithmVersion,
            (decimal) entropy.CrashPoint,
            DateTimeOffset.FromUnixTimeMilliseconds(entropy.CreatedAtUnixMs));
    }
}
