namespace Crash.Domain.State;


/// <summary>
/// Represents the runtime state of a player connected to a table.
///
/// This object exists only while the player is participating in a game.
/// Long-term player information should be loaded from the database when needed.
/// </summary>
public sealed class PlayerRuntimeState
{
    public required long PlayerId { get; init; }
    public required string Username { get; init; }
    public decimal Balance { get; set; }
}