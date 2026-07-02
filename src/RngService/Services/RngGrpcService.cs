using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Crash.Rng;
using Grpc.Core;
using RngService.Data;
using RngService.Options;

namespace RngService.Services;

public sealed class RngGrpcService(IRngRepository repository, RngOptions options) : Rng.RngBase
{
    // Store the exact algorithm name with each round so old rounds can still be verified after future RNG changes.
    private const string RngAlgorithmVersion = "crash-hmac-sha256-v1";
    private const double ReturnToPlayer = 0.96d;
    private static readonly double TwoTo52 = Math.Pow(2d, 52);

    public override async Task<GenerateRoundEntropyResponse> GenerateRoundEntropy(
        GenerateRoundEntropyRequest request,
        ServerCallContext context)
    {
        var tableId = NormalizeRequired(request.TableId, "table_id");
        var roundId = NormalizeRequired(request.RoundId, "round_id");
        var clientSeed = NormalizeRequired(request.ClientSeed, "client_seed");

        // Commit to the server seed publicly by storing its SHA-256 hash, but keep the original seed encrypted.
        var serverSeed = RandomNumberGenerator.GetBytes(32);
        var createdAt = DateTimeOffset.UtcNow;
        var entropyBytes = GenerateEntropy(serverSeed, clientSeed, request.Nonce);
        var entropyHex = Convert.ToHexString(entropyBytes).ToLowerInvariant();
        var serverSeedHash = Convert.ToHexString(SHA256.HashData(serverSeed)).ToLowerInvariant();
        var serverSeedCiphertext = EncryptServerSeed(serverSeed, options.ServerSeedEncryptionKey);
        var crashPoint = CalculateCrashPoint(entropyBytes);

        var entropy = new RngRoundEntropy(
            Guid.NewGuid(),
            tableId,
            roundId,
            serverSeedHash,
            serverSeedCiphertext,
            entropyHex,
            clientSeed,
            request.Nonce,
            RngAlgorithmVersion,
            crashPoint,
            createdAt);

        // A retry for the same table and round must return the original result, not generate a new crash point.
        var storedEntropy = await repository.InsertOrGetAsync(entropy, context.CancellationToken);

        return new GenerateRoundEntropyResponse
        {
            RngId = storedEntropy.RngId.ToString(),
            TableId = storedEntropy.TableId,
            RoundId = storedEntropy.RoundId,
            ServerSeedHash = storedEntropy.ServerSeedHash,
            EntropyHex = storedEntropy.EntropyHex,
            CrashPoint = storedEntropy.CrashPoint,
            CreatedAtUnixMs = storedEntropy.CreatedAt.ToUnixTimeMilliseconds(),
            ClientSeed = storedEntropy.ClientSeed,
            Nonce = storedEntropy.Nonce,
            RngAlgorithmVersion = storedEntropy.RngAlgorithmVersion
        };
    }

    private static string NormalizeRequired(string value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"{fieldName} is required."));
        }

        return value.Trim();
    }

    private static byte[] GenerateEntropy(byte[] serverSeed, string clientSeed, ulong nonce)
    {
        // HMAC ties the outcome to the committed server seed plus caller-provided client seed and nonce.
        var message = Encoding.UTF8.GetBytes($"{clientSeed}:{nonce}");
        return HMACSHA256.HashData(serverSeed, message);
    }

    private static string EncryptServerSeed(byte[] serverSeed, string base64Key)
    {
        byte[] key;
        try
        {
            key = Convert.FromBase64String(base64Key);
        }
        catch (FormatException exception)
        {
            throw new InvalidOperationException("Rng:ServerSeedEncryptionKey must be a base64-encoded 32-byte key.", exception);
        }

        if (key.Length != 32)
        {
            throw new InvalidOperationException("Rng:ServerSeedEncryptionKey must be a base64-encoded 32-byte key.");
        }

        var nonce = RandomNumberGenerator.GetBytes(12);
        var ciphertext = new byte[serverSeed.Length];
        var tag = new byte[16];

        using var aes = new AesGcm(key, tag.Length);
        aes.Encrypt(nonce, serverSeed, ciphertext, tag);

        // Persist nonce + authentication tag + ciphertext together; all three are needed to reveal the seed later.
        var encryptedSeed = new byte[nonce.Length + tag.Length + ciphertext.Length];
        Buffer.BlockCopy(nonce, 0, encryptedSeed, 0, nonce.Length);
        Buffer.BlockCopy(tag, 0, encryptedSeed, nonce.Length, tag.Length);
        Buffer.BlockCopy(ciphertext, 0, encryptedSeed, nonce.Length + tag.Length, ciphertext.Length);

        // Clear sensitive material after deriving the public commitment and encrypted copy.
        CryptographicOperations.ZeroMemory(key);
        CryptographicOperations.ZeroMemory(serverSeed);

        return Convert.ToBase64String(encryptedSeed);
    }

    private static double CalculateCrashPoint(byte[] entropyBytes)
    {
        // Use the first 52 bits so the sample fits exactly in the precision range of a double.
        var sample = BinaryPrimitives.ReadUInt64BigEndian(entropyBytes.AsSpan(0, 8)) >> 12;
        var normalized = sample / TwoTo52;

        // Inverse-transform sampling gives the crash multiplier curve; 0.96 sets the intended 96% RTP.
        var point = ReturnToPlayer / (1d - normalized);

        if (point < 1d)
        {
            return 1d;
        }

        // Floor to two decimals so rounding never pays a multiplier higher than the generated value.
        return Math.Floor(point * 100d) / 100d;
    }
}
