namespace Crash.Domain.Options;

public sealed class DbBrokerOptions
{
    public const string SectionName = "DbBroker";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "db.worker-messages";
    public string ExchangeResultName { get; init; } = "db.worker-results";
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


public sealed class FanoutOptions
{
    public const string SectionName = "Fanout";

    public string HostName { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string ExchangeName { get; init; } = "crash.fanout-messages";
    public string QueueName { get; init; } = "crash.fanout";
    public string RoutingKey { get; init; } = "ClientMessages";
}