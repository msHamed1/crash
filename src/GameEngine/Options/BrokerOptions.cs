namespace GameEngine.Options;

public sealed class BrokerOptions
{
    public const string SectionName = "Broker";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "crash.player-messages";
}
