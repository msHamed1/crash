using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace RealtimeGateway.Dev;

public static class DevJwtTokenFactory
{
    public static string Create(string signingKey, string tableId, string username, string name)
    {
        var header = new Dictionary<string, object>
        {
            ["alg"] = "HS256",
            ["typ"] = "JWT"
        };
        var payload = new Dictionary<string, object>
        {
            ["table_id"] = tableId,
            ["player_id"] = username,
            ["name"] = name,
            ["exp"] = DateTimeOffset.UtcNow.AddHours(12).ToUnixTimeSeconds()
        };

        var encodedHeader = Encode(JsonSerializer.SerializeToUtf8Bytes(header));
        var encodedPayload = Encode(JsonSerializer.SerializeToUtf8Bytes(payload));
        var signatureInput = Encoding.ASCII.GetBytes($"{encodedHeader}.{encodedPayload}");
        var signature = HMACSHA256.HashData(Encoding.UTF8.GetBytes(signingKey), signatureInput);

        return $"{encodedHeader}.{encodedPayload}.{Encode(signature)}";
    }

    private static string Encode(byte[] value)
    {
        return Convert.ToBase64String(value)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
