namespace RngService.Options;

public sealed class RngOptions
{
    public const string SectionName = "Rng";

    public string ServerSeedEncryptionKey { get; init; } = string.Empty;
}
