namespace Ledger;

/// <summary>Application service: load the aggregate from its events, run a command, save the new events.</summary>
public class AccountService(IEventStore store, int maxConflictRetries = 10)
{
    public async Task<Guid> OpenAsync(string owner, string currency, CancellationToken ct = default)
    {
        var account = Account.Open(Guid.NewGuid(), owner, currency);
        await store.AppendAsync(account.Id, expectedVersion: 0, account.UncommittedEvents, ct);
        return account.Id;
    }

    public Task DepositAsync(Guid id, long cents, CancellationToken ct = default) => ExecuteAsync(id, a => a.Deposit(cents), ct);
    public Task WithdrawAsync(Guid id, long cents, CancellationToken ct = default) => ExecuteAsync(id, a => a.Withdraw(cents), ct);
    public Task CloseAsync(Guid id, CancellationToken ct = default) => ExecuteAsync(id, a => a.Close(), ct);

    public async Task<Account> GetAsync(Guid id, CancellationToken ct = default)
    {
        var history = await store.ReadStreamAsync(id, ct);
        return history.Count == 0
            ? throw new AccountNotFoundException(id)
            : Account.Rehydrate(history.Select(h => h.Event));
    }

    /// <summary>
    /// Runs a command with optimistic concurrency. If another writer changed the account first, reload and
    /// re-run the command against the fresh state, so business rules (like "no overdraft") are always
    /// checked against the latest balance and never against a stale one.
    /// </summary>
    private async Task ExecuteAsync(Guid id, Action<Account> command, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            var account = await GetAsync(id, ct);
            command(account); // may throw a DomainException: not retried, it's a real business rejection

            try
            {
                await store.AppendAsync(id, account.LoadedVersion, account.UncommittedEvents, ct);
                account.MarkCommitted();
                return;
            }
            catch (ConcurrencyException) when (attempt < maxConflictRetries)
            {
                // Someone else got in first; loop to reload and try again.
            }
        }
    }
}
