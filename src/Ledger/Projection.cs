using Microsoft.Data.Sqlite;

namespace Ledger;

public record AccountView(Guid Id, string Owner, string Currency, long BalanceCents, string Status, int TransactionCount);

/// <summary>
/// The read side (CQRS). Builds a query-friendly table from the event log and remembers how far it has read.
/// Because the events are the source of truth, this table is disposable: it can be dropped and rebuilt at any time,
/// which is how you add new reports or fix a projection bug without a data migration.
/// </summary>
public class BalanceProjection
{
    private readonly string _connectionString;
    private readonly IEventStore _store;

    public BalanceProjection(string connectionString, IEventStore store)
    {
        _connectionString = connectionString;
        _store = store;

        using var connection = Open();
        Exec(connection, """
            CREATE TABLE IF NOT EXISTS account_views (
                id TEXT PRIMARY KEY, owner TEXT NOT NULL, currency TEXT NOT NULL,
                balance_cents INTEGER NOT NULL, status TEXT NOT NULL, transaction_count INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS projection_checkpoint (id INTEGER PRIMARY KEY CHECK (id = 1), last_sequence INTEGER NOT NULL);
            INSERT OR IGNORE INTO projection_checkpoint (id, last_sequence) VALUES (1, 0);
            """);
    }

    /// <summary>Applies any events not yet projected. Safe to call repeatedly; returns how many were applied.</summary>
    public async Task<int> CatchUpAsync(CancellationToken ct = default)
    {
        var applied = 0;

        while (true)
        {
            await using var connection = await OpenAsync(ct);
            await using var tx = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

            var checkpoint = await ScalarAsync(connection, tx, "SELECT last_sequence FROM projection_checkpoint WHERE id = 1", ct);
            var batch = await _store.ReadAllAsync(checkpoint, 200, ct);
            if (batch.Count == 0) return applied;

            foreach (var stored in batch) await ApplyAsync(connection, tx, stored.Event, ct);

            // View changes and checkpoint move together, so a crash can never double-apply or skip an event.
            await using var move = connection.CreateCommand();
            move.Transaction = tx;
            move.CommandText = "UPDATE projection_checkpoint SET last_sequence = $s WHERE id = 1";
            move.Parameters.AddWithValue("$s", batch[^1].Sequence);
            await move.ExecuteNonQueryAsync(ct);

            await tx.CommitAsync(ct);
            applied += batch.Count;
        }
    }

    /// <summary>Throws the read model away and replays every event from the beginning.</summary>
    public async Task<int> RebuildAsync(CancellationToken ct = default)
    {
        await using (var connection = await OpenAsync(ct))
        {
            Exec(connection, "DELETE FROM account_views; UPDATE projection_checkpoint SET last_sequence = 0 WHERE id = 1;");
        }

        return await CatchUpAsync(ct);
    }

    public async Task<AccountView?> GetAsync(Guid id, CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT id, owner, currency, balance_cents, status, transaction_count FROM account_views WHERE id = $id";
        cmd.Parameters.AddWithValue("$id", id.ToString());
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader) : null;
    }

    public async Task<IReadOnlyList<AccountView>> ListAsync(CancellationToken ct = default)
    {
        await using var connection = await OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT id, owner, currency, balance_cents, status, transaction_count FROM account_views ORDER BY owner, id";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<AccountView>();
        while (await reader.ReadAsync(ct)) rows.Add(Read(reader));
        return rows;
    }

    private static AccountView Read(SqliteDataReader r) =>
        new(Guid.Parse(r.GetString(0)), r.GetString(1), r.GetString(2), r.GetInt64(3), r.GetString(4), r.GetInt32(5));

    private static async Task ApplyAsync(SqliteConnection connection, SqliteTransaction tx, DomainEvent e, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;

        switch (e)
        {
            case AccountOpened o:
                cmd.CommandText = "INSERT INTO account_views VALUES ($id, $owner, $cur, 0, 'Open', 0)";
                cmd.Parameters.AddWithValue("$id", o.AccountId.ToString());
                cmd.Parameters.AddWithValue("$owner", o.Owner);
                cmd.Parameters.AddWithValue("$cur", o.Currency);
                break;
            case MoneyDeposited d:
                cmd.CommandText = "UPDATE account_views SET balance_cents = balance_cents + $a, transaction_count = transaction_count + 1 WHERE id = $id";
                cmd.Parameters.AddWithValue("$a", d.AmountCents);
                cmd.Parameters.AddWithValue("$id", d.AccountId.ToString());
                break;
            case MoneyWithdrawn w:
                cmd.CommandText = "UPDATE account_views SET balance_cents = balance_cents - $a, transaction_count = transaction_count + 1 WHERE id = $id";
                cmd.Parameters.AddWithValue("$a", w.AmountCents);
                cmd.Parameters.AddWithValue("$id", w.AccountId.ToString());
                break;
            case AccountClosed c:
                cmd.CommandText = "UPDATE account_views SET status = 'Closed' WHERE id = $id";
                cmd.Parameters.AddWithValue("$id", c.AccountId.ToString());
                break;
            default:
                return; // unknown/future events are ignored by this projection
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(SqliteConnection connection, SqliteTransaction tx, string sql, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct));
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
