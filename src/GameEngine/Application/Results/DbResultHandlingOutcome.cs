namespace GameEngine.Application.Results;

/// <summary>
/// Tells the RabbitMQ consumer whether a DB-worker result is complete,
/// permanently unusable, or safe to retry.
/// </summary>
public enum DbResultHandlingDisposition
{
    Handled,
    Stale,
    Invalid,
    Retryable
}

public sealed record DbResultHandlingOutcome(
    DbResultHandlingDisposition Disposition,
    string Reason)
{
    public static DbResultHandlingOutcome Handled(string reason) =>
        new(DbResultHandlingDisposition.Handled, reason);

    public static DbResultHandlingOutcome Stale(string reason) =>
        new(DbResultHandlingDisposition.Stale, reason);

    public static DbResultHandlingOutcome Invalid(string reason) =>
        new(DbResultHandlingDisposition.Invalid, reason);

    public static DbResultHandlingOutcome Retryable(string reason) =>
        new(DbResultHandlingDisposition.Retryable, reason);
}
