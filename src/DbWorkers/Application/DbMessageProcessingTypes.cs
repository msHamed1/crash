namespace DbWorkers.Application;

/// <summary>
/// Successful outcomes from processing a durable DB-worker message.
/// Both outcomes are safe for the RabbitMQ consumer to acknowledge.
/// </summary>
public sealed record DbMessageProcessResult(
    bool AlreadyProcessed,
    decimal UpdatedBalance);

/// <summary>
/// Indicates that the message payload is malformed or does not match its
/// declared message type. Retrying the same payload will not fix it.
/// </summary>
public sealed class InvalidDbMessageException : Exception
{
    public InvalidDbMessageException(string message)
        : base(message)
    {
    }

    public InvalidDbMessageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Indicates a non-transient processing conflict that must be retained for
/// operational investigation instead of being retried continuously.
/// </summary>
public sealed class PermanentDbMessageException : Exception
{
    public PermanentDbMessageException(string message)
        : base(message)
    {
    }

    public PermanentDbMessageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
