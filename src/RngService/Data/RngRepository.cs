using MySqlConnector;
using RngService.Options;

namespace RngService.Data;

public sealed record RngRoundEntropy(
    Guid RngId,
    string TableId,
    string RoundId,
    string ServerSeedHash,
    string EntropyHex,
    double CrashPoint,
    DateTimeOffset CreatedAt);

public interface IRngRepository
{
    Task InitializeAsync(CancellationToken cancellationToken);
    Task InsertAsync(RngRoundEntropy entropy, CancellationToken cancellationToken);
}

public sealed class RngRepository(MySqlOptions options) : IRngRepository
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(options.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS rng_round_entropy (
                rng_id CHAR(36) NOT NULL PRIMARY KEY,
                table_id VARCHAR(64) NOT NULL,
                round_id VARCHAR(64) NOT NULL,
                server_seed_hash CHAR(64) NOT NULL,
                entropy_hex VARCHAR(128) NOT NULL,
                crash_point DOUBLE NOT NULL,
                created_at_utc DATETIME(6) NOT NULL,
                UNIQUE KEY ux_rng_round_entropy_table_round (table_id, round_id),
                KEY ix_rng_round_entropy_created_at (created_at_utc)
            );
            """;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task InsertAsync(RngRoundEntropy entropy, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(options.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO rng_round_entropy
                (rng_id, table_id, round_id, server_seed_hash, entropy_hex, crash_point, created_at_utc)
            VALUES
                (@rng_id, @table_id, @round_id, @server_seed_hash, @entropy_hex, @crash_point, @created_at_utc);
            """;
        command.Parameters.AddWithValue("@rng_id", entropy.RngId.ToString());
        command.Parameters.AddWithValue("@table_id", entropy.TableId);
        command.Parameters.AddWithValue("@round_id", entropy.RoundId);
        command.Parameters.AddWithValue("@server_seed_hash", entropy.ServerSeedHash);
        command.Parameters.AddWithValue("@entropy_hex", entropy.EntropyHex);
        command.Parameters.AddWithValue("@crash_point", entropy.CrashPoint);
        command.Parameters.AddWithValue("@created_at_utc", entropy.CreatedAt.UtcDateTime);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
