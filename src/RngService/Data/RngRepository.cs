using MySqlConnector;
using RngService.Options;

namespace RngService.Data;

public sealed record RngRoundEntropy(
    Guid RngId,
    string TableId,
    string RoundId,
    string ServerSeedHash,
    string ServerSeedCiphertext,
    string EntropyHex,
    string ClientSeed,
    ulong Nonce,
    string RngAlgorithmVersion,
    double CrashPoint,
    DateTimeOffset CreatedAt);

public interface IRngRepository
{
    Task InitializeAsync(CancellationToken cancellationToken);
    Task<RngRoundEntropy> InsertOrGetAsync(RngRoundEntropy entropy, CancellationToken cancellationToken);
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
                server_seed_ciphertext VARCHAR(256) NOT NULL,
                entropy_hex VARCHAR(128) NOT NULL,
                client_seed VARCHAR(128) NOT NULL,
                nonce BIGINT UNSIGNED NOT NULL,
                rng_algorithm_version VARCHAR(32) NOT NULL,
                crash_point DOUBLE NOT NULL,
                created_at_utc DATETIME(6) NOT NULL,
                UNIQUE KEY ux_rng_round_entropy_table_round (table_id, round_id),
                KEY ix_rng_round_entropy_created_at (created_at_utc)
            );
            """;

        await command.ExecuteNonQueryAsync(cancellationToken);

        // Keep local/dev databases moving forward without requiring a separate migration runner for this sample.
        await AddColumnIfMissingAsync(connection, "server_seed_ciphertext", "VARCHAR(256) NULL", cancellationToken);
        await AddColumnIfMissingAsync(connection, "client_seed", "VARCHAR(128) NULL", cancellationToken);
        await AddColumnIfMissingAsync(connection, "nonce", "BIGINT UNSIGNED NULL", cancellationToken);
        await AddColumnIfMissingAsync(connection, "rng_algorithm_version", "VARCHAR(32) NULL", cancellationToken);
    }

    public async Task<RngRoundEntropy> InsertOrGetAsync(RngRoundEntropy entropy, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(options.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO rng_round_entropy
                (rng_id, table_id, round_id, server_seed_hash, server_seed_ciphertext, entropy_hex, client_seed, nonce, rng_algorithm_version, crash_point, created_at_utc)
            VALUES
                (@rng_id, @table_id, @round_id, @server_seed_hash, @server_seed_ciphertext, @entropy_hex, @client_seed, @nonce, @rng_algorithm_version, @crash_point, @created_at_utc)
            ON DUPLICATE KEY UPDATE rng_id = rng_id;
            """;
        command.Parameters.AddWithValue("@rng_id", entropy.RngId.ToString());
        command.Parameters.AddWithValue("@table_id", entropy.TableId);
        command.Parameters.AddWithValue("@round_id", entropy.RoundId);
        command.Parameters.AddWithValue("@server_seed_hash", entropy.ServerSeedHash);
        command.Parameters.AddWithValue("@server_seed_ciphertext", entropy.ServerSeedCiphertext);
        command.Parameters.AddWithValue("@entropy_hex", entropy.EntropyHex);
        command.Parameters.AddWithValue("@client_seed", entropy.ClientSeed);
        command.Parameters.AddWithValue("@nonce", entropy.Nonce);
        command.Parameters.AddWithValue("@rng_algorithm_version", entropy.RngAlgorithmVersion);
        command.Parameters.AddWithValue("@crash_point", entropy.CrashPoint);
        command.Parameters.AddWithValue("@created_at_utc", entropy.CreatedAt.UtcDateTime);

        await command.ExecuteNonQueryAsync(cancellationToken);

        // Always read the persisted row so duplicate requests return the original RNG values.
        return await GetByTableRoundAsync(connection, entropy.TableId, entropy.RoundId, cancellationToken)
            ?? throw new InvalidOperationException("RNG row was not found after insert.");
    }

    private static async Task AddColumnIfMissingAsync(
        MySqlConnection connection,
        string columnName,
        string columnDefinition,
        CancellationToken cancellationToken)
    {
        await using var existsCommand = connection.CreateCommand();
        existsCommand.CommandText = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'rng_round_entropy'
              AND COLUMN_NAME = @column_name;
            """;
        existsCommand.Parameters.AddWithValue("@column_name", columnName);

        var columnExists = Convert.ToInt32(await existsCommand.ExecuteScalarAsync(cancellationToken)) > 0;
        if (columnExists)
        {
            return;
        }

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            ALTER TABLE rng_round_entropy
            ADD COLUMN {columnName} {columnDefinition};
            """;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<RngRoundEntropy?> GetByTableRoundAsync(
        MySqlConnection connection,
        string tableId,
        string roundId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT rng_id, table_id, round_id, server_seed_hash, server_seed_ciphertext, entropy_hex,
                   client_seed, nonce, rng_algorithm_version, crash_point, created_at_utc
            FROM rng_round_entropy
            WHERE table_id = @table_id AND round_id = @round_id;
            """;
        command.Parameters.AddWithValue("@table_id", tableId);
        command.Parameters.AddWithValue("@round_id", roundId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new RngRoundEntropy(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
            reader.GetString(5),
            reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
            reader.IsDBNull(7) ? 0UL : reader.GetFieldValue<ulong>(7),
            reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
            reader.GetDouble(9),
            new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc)));
    }
}
