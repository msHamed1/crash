using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Crash.Domain.Options;
using Microsoft.AspNetCore.SignalR;
 
namespace RealtimeGateway.Hubs;

public sealed record PlayerConnectionContext(string TableId, string PlayerId);

public interface IJwtConnectionValidator
{
    PlayerConnectionContext Validate(HubCallerContext context);
}

public sealed class JwtConnectionValidator(JwtOptions options) : IJwtConnectionValidator
{
    public PlayerConnectionContext Validate(HubCallerContext context)
    {
        var token = GetBearerToken(context)
            ?? throw new HubException("JWT is required.");

        var payload = ValidateToken(token);
        var tableId = GetRequiredClaim(payload, "table_id", "tableId");
        var playerId = GetRequiredClaim(payload, "player_id", "playerId");

        return new PlayerConnectionContext(tableId, playerId);
    }

    private JsonElement ValidateToken(string token)
    {
        var parts = token.Split('.');
        if (parts.Length != 3)
        {
            throw new HubException("JWT format is invalid.");
        }

        var headerJson = DecodeBase64Url(parts[0]);
        using var header = JsonDocument.Parse(headerJson);
        if (!header.RootElement.TryGetProperty("alg", out var algorithm)
            || !string.Equals(algorithm.GetString(), "HS256", StringComparison.Ordinal))
        {
            throw new HubException("JWT algorithm must be HS256.");
        }

        var signatureInput = Encoding.ASCII.GetBytes($"{parts[0]}.{parts[1]}");
        var key = Encoding.UTF8.GetBytes(options.SigningKey);
        var expectedSignature = HMACSHA256.HashData(key, signatureInput);
        var actualSignature = DecodeBase64UrlBytes(parts[2]);

        if (!CryptographicOperations.FixedTimeEquals(expectedSignature, actualSignature))
        {
            throw new HubException("JWT signature is invalid.");
        }

        var payloadJson = DecodeBase64Url(parts[1]);
        using var payload = JsonDocument.Parse(payloadJson);

        if (payload.RootElement.TryGetProperty("exp", out var expiresAt)
            && expiresAt.ValueKind == JsonValueKind.Number
            && expiresAt.GetInt64() < DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        {
            throw new HubException("JWT is expired.");
        }

        return payload.RootElement.Clone();
    }

    private static string? GetBearerToken(HubCallerContext context)
    {
        var httpContext = context.GetHttpContext();
        var accessToken = httpContext?.Request.Query["access_token"].FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(accessToken))
        {
            return accessToken;
        }

        var authorization = httpContext?.Request.Headers.Authorization.FirstOrDefault();
        const string bearerPrefix = "Bearer ";
        if (authorization is not null
            && authorization.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return authorization[bearerPrefix.Length..].Trim();
        }

        return null;
    }

    private static string GetRequiredClaim(JsonElement payload, string snakeCaseName, string camelCaseName)
    {
        if (payload.TryGetProperty(snakeCaseName, out var snakeCaseValue)
            && !string.IsNullOrWhiteSpace(snakeCaseValue.GetString()))
        {
            return snakeCaseValue.GetString()!;
        }

        if (payload.TryGetProperty(camelCaseName, out var camelCaseValue)
            && !string.IsNullOrWhiteSpace(camelCaseValue.GetString()))
        {
            return camelCaseValue.GetString()!;
        }

        throw new HubException($"{snakeCaseName} claim is required.");
    }

    private static string DecodeBase64Url(string value)
    {
        return Encoding.UTF8.GetString(DecodeBase64UrlBytes(value));
    }

    private static byte[] DecodeBase64UrlBytes(string value)
    {
        var padded = value.Replace('-', '+').Replace('_', '/');
        padded = padded.PadRight(padded.Length + (4 - padded.Length % 4) % 4, '=');

        return Convert.FromBase64String(padded);
    }
}
