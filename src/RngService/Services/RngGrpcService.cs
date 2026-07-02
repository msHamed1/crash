using System.Security.Cryptography;
using System.Text;
using Crash.Rng;
using Grpc.Core;
using RngService.Data;

namespace RngService.Services;

public sealed class RngGrpcService(IRngRepository repository) : Rng.RngBase
{
    public override async Task<GenerateRoundEntropyResponse> GenerateRoundEntropy(
        GenerateRoundEntropyRequest request,
        ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.TableId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "table_id is required."));
        }

        if (string.IsNullOrWhiteSpace(request.RoundId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "round_id is required."));
        }

        var entropyBytes = RandomNumberGenerator.GetBytes(32);
        var serverSeed = RandomNumberGenerator.GetBytes(32);
        var createdAt = DateTimeOffset.UtcNow;
        var entropyHex = Convert.ToHexString(entropyBytes).ToLowerInvariant();
        var serverSeedHash = Convert.ToHexString(SHA256.HashData(serverSeed)).ToLowerInvariant();
        var crashPoint = CalculateCrashPoint(entropyBytes);

        var entropy = new RngRoundEntropy(
            Guid.NewGuid(),
            request.TableId,
            request.RoundId,
            serverSeedHash,
            entropyHex,
            crashPoint,
            createdAt);

        await repository.InsertAsync(entropy, context.CancellationToken);

        return new GenerateRoundEntropyResponse
        {
            RngId = entropy.RngId.ToString(),
            TableId = entropy.TableId,
            RoundId = entropy.RoundId,
            ServerSeedHash = entropy.ServerSeedHash,
            EntropyHex = entropy.EntropyHex,
            CrashPoint = entropy.CrashPoint,
            CreatedAtUnixMs = entropy.CreatedAt.ToUnixTimeMilliseconds()
        };
    }

    private static double CalculateCrashPoint(byte[] entropyBytes)
    {
        var sample = BitConverter.ToUInt64(entropyBytes, 0) >> 12;
        var normalized = Math.Max(sample / (double)(1UL << 52), 0.000001d);
        var point = 0.99d / normalized;

        return Math.Round(Math.Clamp(point, 1.0d, 1000.0d), 2);
    }
}
