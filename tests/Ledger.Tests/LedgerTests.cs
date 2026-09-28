using Ledger;

namespace Ledger.Tests;

public sealed class TestEnv : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"ledger-tests-{Guid.NewGuid():N}.db");
    public string ConnectionString => $"Data Source={_path};Default Timeout=30";
    public SqliteEventStore Store { get; }
    public AccountService Service { get; }
    public BalanceProjection Projection { get; }

    public TestEnv()
    {
        Store = new SqliteEventStore(ConnectionString);
        Service = new AccountService(Store, maxConflictRetries: 50);
        Projection = new BalanceProjection(ConnectionString, Store);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { File.Delete(_path); } catch (IOException) { /* best effort */ }
    }
}

public class AccountAggregateTests
{
    private static Account Funded(long cents)
    {
        var account = Account.Open(Guid.NewGuid(), "Dana", "usd");
        account.Deposit(cents);
        return account;
    }

    [Fact]
    public void Opening_raises_an_event_and_normalises_currency()
    {
        var account = Account.Open(Guid.NewGuid(), " Dana ", "usd");

        Assert.Equal("Dana", account.Owner);
        Assert.Equal("USD", account.Currency);
        Assert.IsType<AccountOpened>(Assert.Single(account.UncommittedEvents));
    }

    [Fact]
    public void Balance_is_derived_from_events()
    {
        var account = Funded(1000);
        account.Withdraw(300);
        account.Deposit(50);

        Assert.Equal(750, account.BalanceCents);
        Assert.Equal(4, account.Version);
    }

    [Fact]
    public void Overdraft_is_rejected_and_raises_no_event()
    {
        var account = Funded(100);
        var before = account.UncommittedEvents.Count;

        Assert.Throws<InsufficientFundsException>(() => account.Withdraw(101));

        Assert.Equal(before, account.UncommittedEvents.Count);
        Assert.Equal(100, account.BalanceCents);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void Non_positive_amounts_are_rejected(long amount)
    {
        var account = Funded(100);

        Assert.Throws<DomainException>(() => account.Deposit(amount));
        Assert.Throws<DomainException>(() => account.Withdraw(amount));
    }

    [Fact]
    public void Closing_requires_a_zero_balance_and_blocks_further_activity()
    {
        var account = Funded(100);
        Assert.Throws<DomainException>(account.Close);

        account.Withdraw(100);
        account.Close();

        Assert.True(account.IsClosed);
        Assert.Throws<DomainException>(() => account.Deposit(10));
        Assert.Throws<DomainException>(account.Close);
    }

    [Fact]
    public void Rehydrating_from_history_reproduces_the_exact_state()
    {
        var original = Funded(1000);
        original.Withdraw(250);

        var rebuilt = Account.Rehydrate(original.UncommittedEvents);

        Assert.Equal(original.BalanceCents, rebuilt.BalanceCents);
        Assert.Equal(original.Version, rebuilt.Version);
        Assert.Equal(original.Id, rebuilt.Id);
        Assert.Empty(rebuilt.UncommittedEvents); // replayed events are history, not new
    }

    [Fact]
    public void Invalid_open_arguments_are_rejected()
    {
        Assert.Throws<DomainException>(() => Account.Open(Guid.NewGuid(), "", "USD"));
        Assert.Throws<DomainException>(() => Account.Open(Guid.NewGuid(), "Dana", "DOLLARS"));
    }
}

public class EventStoreTests : IDisposable
{
    private readonly TestEnv _env = new();
    public void Dispose() => _env.Dispose();

    [Fact]
    public async Task Events_round_trip_in_order_with_their_types()
    {
        var id = Guid.NewGuid();
        await _env.Store.AppendAsync(id, 0, [new AccountOpened(id, "Dana", "USD"), new MoneyDeposited(id, 500)]);
        await _env.Store.AppendAsync(id, 2, [new MoneyWithdrawn(id, 200)]);

        var history = await _env.Store.ReadStreamAsync(id);

        Assert.Equal([1, 2, 3], history.Select(h => h.Version));
        Assert.IsType<AccountOpened>(history[0].Event);
        Assert.Equal(500, ((MoneyDeposited)history[1].Event).AmountCents);
        Assert.Equal(200, ((MoneyWithdrawn)history[2].Event).AmountCents);
    }

    [Fact]
    public async Task Appending_at_a_stale_version_is_rejected()
    {
        var id = Guid.NewGuid();
        await _env.Store.AppendAsync(id, 0, [new AccountOpened(id, "Dana", "USD")]);

        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            _env.Store.AppendAsync(id, 0, [new MoneyDeposited(id, 1)])); // pretends nothing was written yet
    }

    [Fact]
    public async Task A_rejected_batch_writes_nothing()
    {
        var id = Guid.NewGuid();
        await _env.Store.AppendAsync(id, 0, [new AccountOpened(id, "Dana", "USD")]);

        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            _env.Store.AppendAsync(id, 5, [new MoneyDeposited(id, 1), new MoneyDeposited(id, 2)]));

        Assert.Single(await _env.Store.ReadStreamAsync(id));
    }

    [Fact]
    public async Task Global_read_returns_events_across_streams_after_a_sequence()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await _env.Store.AppendAsync(a, 0, [new AccountOpened(a, "A", "USD")]);
        await _env.Store.AppendAsync(b, 0, [new AccountOpened(b, "B", "USD")]);
        await _env.Store.AppendAsync(a, 1, [new MoneyDeposited(a, 10)]);

        var all = await _env.Store.ReadAllAsync(0, 100);
        var tail = await _env.Store.ReadAllAsync(all[1].Sequence, 100);

        Assert.Equal(3, all.Count);
        Assert.Equal(all.Select(e => e.Sequence).Order(), all.Select(e => e.Sequence));
        Assert.Single(tail);
    }
}

public class ConcurrencyTests : IDisposable
{
    private readonly TestEnv _env = new();
    public void Dispose() => _env.Dispose();

    [Fact]
    public async Task Two_writers_from_the_same_version_cannot_both_win()
    {
        var id = await _env.Service.OpenAsync("Dana", "USD");
        await _env.Service.DepositAsync(id, 1000);

        var first = await _env.Service.GetAsync(id);
        var second = await _env.Service.GetAsync(id);
        first.Deposit(1);
        second.Deposit(2);

        await _env.Store.AppendAsync(id, first.LoadedVersion, first.UncommittedEvents);
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            _env.Store.AppendAsync(id, second.LoadedVersion, second.UncommittedEvents));
    }

    [Fact]
    public async Task Parallel_deposits_are_never_lost()
    {
        var id = await _env.Service.OpenAsync("Dana", "USD");

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => _env.Service.DepositAsync(id, 100)));

        var account = await _env.Service.GetAsync(id);
        Assert.Equal(2000, account.BalanceCents);
        Assert.Equal(21, account.Version); // 1 open + 20 deposits, no gaps or duplicates
    }

    [Fact]
    public async Task Parallel_withdrawals_can_never_overdraw_the_account()
    {
        var id = await _env.Service.OpenAsync("Dana", "USD");
        await _env.Service.DepositAsync(id, 500);

        // 10 concurrent attempts to withdraw 100 from a balance of 500: exactly 5 may succeed.
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(async _ =>
        {
            try { await _env.Service.WithdrawAsync(id, 100); return true; }
            catch (InsufficientFundsException) { return false; }
        }));

        Assert.Equal(5, results.Count(r => r));
        Assert.Equal(0, (await _env.Service.GetAsync(id)).BalanceCents);
    }

    [Fact]
    public async Task Unknown_account_is_reported()
    {
        await Assert.ThrowsAsync<AccountNotFoundException>(() => _env.Service.GetAsync(Guid.NewGuid()));
    }
}

public class ProjectionTests : IDisposable
{
    private readonly TestEnv _env = new();
    public void Dispose() => _env.Dispose();

    [Fact]
    public async Task Read_model_reflects_the_events()
    {
        var id = await _env.Service.OpenAsync("Dana", "usd");
        await _env.Service.DepositAsync(id, 1000);
        await _env.Service.WithdrawAsync(id, 250);

        await _env.Projection.CatchUpAsync();
        var view = await _env.Projection.GetAsync(id);

        Assert.Equal(new AccountView(id, "Dana", "USD", 750, "Open", 2), view);
    }

    [Fact]
    public async Task Catching_up_is_incremental_and_idempotent()
    {
        var id = await _env.Service.OpenAsync("Dana", "USD");
        await _env.Service.DepositAsync(id, 100);

        Assert.Equal(2, await _env.Projection.CatchUpAsync());
        Assert.Equal(0, await _env.Projection.CatchUpAsync()); // nothing new, nothing double-applied

        await _env.Service.DepositAsync(id, 50);
        Assert.Equal(1, await _env.Projection.CatchUpAsync());
        Assert.Equal(150, (await _env.Projection.GetAsync(id))!.BalanceCents);
    }

    [Fact]
    public async Task Rebuilding_from_scratch_gives_the_same_read_model()
    {
        var ids = new List<Guid>();
        for (var i = 0; i < 5; i++)
        {
            var id = await _env.Service.OpenAsync($"Owner {i}", "USD");
            await _env.Service.DepositAsync(id, 1000 + i);
            await _env.Service.WithdrawAsync(id, 100);
            ids.Add(id);
        }
        await _env.Service.WithdrawAsync(ids[0], 900);
        await _env.Service.CloseAsync(ids[0]);

        await _env.Projection.CatchUpAsync();
        var incremental = await _env.Projection.ListAsync();

        await _env.Projection.RebuildAsync();
        var rebuilt = await _env.Projection.ListAsync();

        Assert.Equal(incremental, rebuilt);
        Assert.Equal("Closed", rebuilt.Single(v => v.Id == ids[0]).Status);
    }

    [Fact]
    public async Task Projection_matches_the_aggregate_state_after_concurrent_activity()
    {
        var id = await _env.Service.OpenAsync("Dana", "USD");
        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => _env.Service.DepositAsync(id, 10)));

        await _env.Projection.CatchUpAsync();

        Assert.Equal((await _env.Service.GetAsync(id)).BalanceCents, (await _env.Projection.GetAsync(id))!.BalanceCents);
    }
}
