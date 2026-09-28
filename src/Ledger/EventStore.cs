using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Ledger;

/// <summary>Another writer appended to this stream since it was loaded. Reload and retry.</summary>
public class ConcurrencyException(Guid streamId, int expected, int actual)
    : Exception($"Stream {streamId} is at version {actual} but the write expected {expected}.");

public record StoredEvent(long Sequence, Guid StreamId, int Version, DomainEvent Event, DateTime OccurredAt);

public interface IEventStore
{
    /// <summary>Appends events atomically, only if the stream is still at <paramref name="expectedVersion"/>.</summary>
    Task AppendAsync(Guid streamId, int expectedVersion, IReadOnlyList<DomainEvent> events, CancellationToken ct = default);

    Task<IReadOnlyList<StoredEvent>> ReadStreamAsync(Guid streamId, CancellationToken ct = default);

    /// <summary>Reads events across all streams in global order, for projections.</summary>
    Task<IReadOnlyList<StoredEvent>> ReadAllAsync(long afterSequence, int take, CancellationToken ct = default);
}

/// <summary>
/// Append-only event log in SQLite. The primary key (stream_id, version) is what enforces optimistic
/// concurrency: two writers cannot both create version N of the same stream.
/// </summary>
public class SqliteEventStore : IEventStore
{
    private static readonly Dictionary<string, Type> Types = typeof(DomainEvent).Assembly.GetTypes()
        .Where(t => t.IsSubclassOf(typeof(DomainEvent)) && !t.IsAbstract)
        .ToDictionary(t => t.Name);

    private readonly string _connectionString;
    private readonly TimeProvider _time;

    public SqliteEventStore(string connectionString, TimeProvider? time = null)
    {
        _connectionString = connectionString;
        _time = time ?? TimeProvider.System;

        using var connection = Open();
        Exec(connection, """
            CREATE TABLE IF NOT EXISTS events (
                sequence    INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id   TEXT    NOT NULL,
                version     INTEGER NOT NULL,
                type        TEXT    NOT NULL,
                data        TEXT    NOT NULL,
                occurred_at TEXT    NOT NULL,
                UNIQUE (stream_id, version));
            """);
    }

    public async Task AppendAsync(Guid streamId, int expectedVersion, IReadOnlyList<DomainEvent> events, CancellationToken ct = default)
    {
        if (events.Count == 0) return;

        await using var connection = await OpenAsync(ct);
        await using var tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

        await using (var check = connection.CreateCommand())
        {
            check.Transaction = tx;
            check.CommandText = "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $s";
            check.Parameters.AddWithValue("$s", streamId.ToString());
            var actual = Convert.ToInt32(await check.ExecuteScalarAsync(ct));
            if (actual != expectedVersion) throw new ConcurrencyException(streamId, expectedVersion, actual);
        }

        var version = expectedVersion;
        var now = _time.GetUtcNow().UtcDateTime.ToString("O");
        foreach (var e in events)
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO events (stream_id, version, type, data, occurred_at) VALUES ($s, $v, $t, $d, $o)";
            insert.Parameters.AddWithValue("$s", streamId.ToString());
            insert.Parameters.AddWithValue("$v", ++version);
            insert.Parameters.AddWithValue("$t", e.GetType().Name);
            insert.Parameters.AddWithValue("$d", JsonSerializer.Serialize(e, e.GetType()));
            insert.Parameters.AddWithValue("$o", now);

            try
            {
                await insert.ExecuteNonQueryAsync(ct);
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 19) // UNIQUE violated: a concurrent writer got there first
            {
                throw new ConcurrencyException(streamId, expectedVersion, version);
            }
        }

        await tx.CommitAsync(ct); // all events of the batch, or none
    }

    public async Task<IReadOnlyList<StoredEvent>> ReadStreamAsync(Guid streamId, CancellationToken ct = default) =>
        await ReadAsync("WHERE stream_id = $s ORDER BY version", cmd => cmd.Parameters.AddWithValue("$s", streamId.ToString()), ct);

    public async Task<IReadOnlyList<StoredEvent>> ReadAllAsync(long afterSequence, int take, CancellationToken ct = default) =>
        await ReadAsync("WHERE sequence > $after ORDER BY sequence LIMIT $take", cmd =>
        {
            cmd.Parameters.AddWithValue("$after", afterSequence);
            cmd.Parameters.AddWithValue("$take", take);
        }, ct);

    private async Task<IReadOnlyList<StoredEvent>> ReadAsync(string clause, Action<SqliteCommand> bind, CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT sequence, stream_id, version, type, data, occurred_at FROM events {clause}";
        bind(cmd);

        var result = new List<StoredEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var type = Types[reader.GetString(3)];
            var e = (DomainEvent)JsonSerializer.Deserialize(reader.GetString(4), type)!;
            result.Add(new StoredEvent(
                reader.GetInt64(0), Guid.Parse(reader.GetString(1)), reader.GetInt32(2), e,
                DateTime.Parse(reader.GetString(5), null, System.Globalization.DateTimeStyles.RoundtripKind)));
        }

        return result;
    }

    private SqliteConnection Open()
    {
        var connection = new SqliteConnection(_connectionString);
        connection.Open();
        return connection;
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static void Exec(SqliteConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
