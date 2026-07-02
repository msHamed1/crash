using Crash.Rng;

namespace GameEngine.Services;

public sealed record StartRoundRequest(string? TableId);

public sealed record StartedRoundResponse(
    string TableId,
    string RoundId,
    string RngId,
    string ServerSeedHash,
    string EntropyHex,
    double CrashPoint,
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

        var roundId = $"round-{DateTimeOffset.UtcNow:yyyyMMddHHmmssfff}-{Guid.NewGuid():N}";
        var entropy = await rngClient.GenerateRoundEntropyAsync(
            new GenerateRoundEntropyRequest
            {
                TableId = tableId,
                RoundId = roundId
            },
            cancellationToken: cancellationToken);

        return new StartedRoundResponse(
            entropy.TableId,
            entropy.RoundId,
            entropy.RngId,
            entropy.ServerSeedHash,
            entropy.EntropyHex,
            entropy.CrashPoint,
            DateTimeOffset.FromUnixTimeMilliseconds(entropy.CreatedAtUnixMs));
    }
}
