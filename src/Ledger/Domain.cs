namespace Ledger;

public class DomainException(string message) : Exception(message);
public class InsufficientFundsException(long balance, long requested)
    : DomainException($"Insufficient funds: balance is {balance}, requested {requested} (in cents).");
public class AccountNotFoundException(Guid id) : Exception($"Account {id} was not found.");

/// <summary>Immutable facts about what happened. State is never stored, only derived from these.</summary>
public abstract record DomainEvent;

public record AccountOpened(Guid AccountId, string Owner, string Currency) : DomainEvent;
public record MoneyDeposited(Guid AccountId, long AmountCents) : DomainEvent;
public record MoneyWithdrawn(Guid AccountId, long AmountCents) : DomainEvent;
public record AccountClosed(Guid AccountId) : DomainEvent;

/// <summary>
/// The account aggregate. Commands validate business rules and raise events; events are the only thing that
/// changes state. The same <see cref="Apply"/> code runs whether an event is new or replayed from history,
/// so a rebuilt account is always identical to the one that produced the events.
/// </summary>
public class Account
{
    private readonly List<DomainEvent> _uncommitted = [];

    public Guid Id { get; private set; }
    public string Owner { get; private set; } = string.Empty;
    public string Currency { get; private set; } = string.Empty;
    public long BalanceCents { get; private set; }
    public bool IsClosed { get; private set; }

    /// <summary>Number of events applied so far (including uncommitted ones).</summary>
    public int Version { get; private set; }

    /// <summary>The stream version this instance was loaded at: what the store must still hold when we save.</summary>
    public int LoadedVersion => Version - _uncommitted.Count;

    public IReadOnlyList<DomainEvent> UncommittedEvents => _uncommitted;

    private Account() { }

    public static Account Open(Guid id, string owner, string currency)
    {
        if (string.IsNullOrWhiteSpace(owner)) throw new DomainException("Owner is required.");
        if (string.IsNullOrWhiteSpace(currency) || currency.Length != 3) throw new DomainException("Currency must be a 3-letter code.");

        var account = new Account();
        account.Raise(new AccountOpened(id, owner.Trim(), currency.ToUpperInvariant()));
        return account;
    }

    public static Account Rehydrate(IEnumerable<DomainEvent> history)
    {
        var account = new Account();
        foreach (var e in history) account.Apply(e);
        return account.Version == 0 ? throw new DomainException("An account cannot be rebuilt from an empty history.") : account;
    }

    public void Deposit(long amountCents)
    {
        EnsureOpen();
        if (amountCents <= 0) throw new DomainException("Deposit must be positive.");
        Raise(new MoneyDeposited(Id, amountCents));
    }

    public void Withdraw(long amountCents)
    {
        EnsureOpen();
        if (amountCents <= 0) throw new DomainException("Withdrawal must be positive.");
        if (amountCents > BalanceCents) throw new InsufficientFundsException(BalanceCents, amountCents);
        Raise(new MoneyWithdrawn(Id, amountCents));
    }

    public void Close()
    {
        EnsureOpen();
        if (BalanceCents != 0) throw new DomainException("An account can only be closed when its balance is zero.");
        Raise(new AccountClosed(Id));
    }

    public void MarkCommitted() => _uncommitted.Clear();

    private void EnsureOpen()
    {
        if (Version == 0) throw new DomainException("Account has not been opened.");
        if (IsClosed) throw new DomainException("Account is closed.");
    }

    private void Raise(DomainEvent e)
    {
        Apply(e);
        _uncommitted.Add(e);
    }

    private void Apply(DomainEvent e)
    {
        switch (e)
        {
            case AccountOpened o: Id = o.AccountId; Owner = o.Owner; Currency = o.Currency; break;
            case MoneyDeposited d: BalanceCents += d.AmountCents; break;
            case MoneyWithdrawn w: BalanceCents -= w.AmountCents; break;
            case AccountClosed: IsClosed = true; break;
            default: throw new InvalidOperationException($"Unknown event {e.GetType().Name}.");
        }

        Version++;
    }
}
