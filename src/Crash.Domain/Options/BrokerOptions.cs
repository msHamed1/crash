namespace Crash.Domain.Options;

public sealed class DbBrokerOptions
{
    public const string SectionName = "DbBroker";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "db.worker-messages";
}

public sealed class PlayerBrokerOptions
{
    public const string SectionName = "Broker";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "crash.player-messages";
}


public sealed class RgsBrokerOptions
{
    public const string SectionName = "Broker";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "crash.rgs-messages";
}