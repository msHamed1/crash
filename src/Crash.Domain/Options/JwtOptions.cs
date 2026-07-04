namespace Crash.Domain.Options;

public sealed class JwtOptions
{
    public const string SectionName = "Jwt";

    public string SigningKey { get; init; } = string.Empty;
}
