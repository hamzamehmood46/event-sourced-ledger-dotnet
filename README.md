# Event-Sourced Ledger

[![CI](https://github.com/hamzamehmood46/event-sourced-ledger-dotnet/actions/workflows/ci.yml/badge.svg)](https://github.com/hamzamehmood46/event-sourced-ledger-dotnet/actions/workflows/ci.yml)

**Live demo: [event-sourced-ledger-dotnet.onrender.com/swagger](https://event-sourced-ledger-dotnet.onrender.com/swagger)** (Swagger UI). Hosted on Render's free tier, so the first load after idle can take about a minute, and demo data resets on restart.

Try it: `POST /api/accounts` to open an account, deposit and withdraw (amounts are in cents), then `GET /api/accounts/{id}/events` to see the immutable history the balance is rebuilt from. `GET /api/accounts` is the separate read model: it lags until you `POST /api/projection/catch-up`, and `POST /api/projection/rebuild` throws it away and replays every event. The headline guarantee is one call away: `POST /api/demo/contention` races 10 workers withdrawing $1 from a $5 balance, and exactly 5 succeed and the balance ends at 0, never negative.
A bank-account ledger in **.NET 8** built with **event sourcing and CQRS**. Instead of storing a balance and overwriting it, every change is stored as an immutable event (`MoneyDeposited`, `MoneyWithdrawn`, ...). Current state is *derived* by replaying those events, which gives a complete audit trail for free, which is exactly what financial systems need.

```
   command                      write side                           read side (CQRS)
 ┌──────────┐   load    ┌─────────────────┐  append (expected   ┌───────────────┐  catch up   ┌──────────────┐
 │ Withdraw │ ────────► │ Account         │  version) ────────► │  Event store  │ ──────────► │ account_views│
 │  $100    │           │ (rebuilt from   │  optimistic         │  (append-only)│  checkpoint │ (disposable  │
 └──────────┘           │  its events)    │  concurrency        └───────────────┘             │  read model) │
                        └─────────────────┘                                                   └──────────────┘
```

## Why this design

| Idea | What it gives you |
|---|---|
| **Events are the source of truth** | A full audit history. You can answer "what was the balance on March 3rd?" or "why is it this number?" by replaying. |
| **Aggregate enforces the rules** | `Account` validates commands (no overdraft, no deposits to a closed account, close only at zero) *before* raising events. Rejected commands write nothing. |
| **One `Apply` for new and replayed events** | A rebuilt account is guaranteed identical to the one that produced the events. |
| **Optimistic concurrency** | The `(stream_id, version)` uniqueness constraint means two writers cannot both create version N. The loser reloads and re-runs its command against fresh state. |
| **Atomic batches** | A command's events are appended in one transaction: all or none. |
| **CQRS read model** | `BalanceProjection` maintains a query-friendly table plus a checkpoint, updated in one transaction so events are never double-applied or skipped. |
| **Disposable projections** | `RebuildAsync()` drops the read model and replays history, so you can add a new report or fix a projection bug with no data migration. |

## The interesting guarantee: invariants hold under contention

Ten threads try to withdraw $1 from a $5 balance at the same instant:

```csharp
var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => service.WithdrawAsync(id, 100)));
// exactly 5 succeed, 5 get InsufficientFundsException, final balance is 0, never negative
```

Each conflicting writer is retried against the **latest** state, so the "no overdraft" rule is always checked against the real balance, not a stale read. Likewise 20 parallel deposits always total exactly 20 deposits, with no lost updates.

## Project layout

```
src/Ledger/
  Domain.cs          Account aggregate + events + business rules (no I/O)
  EventStore.cs      IEventStore + SQLite append-only implementation (ADO.NET)
  AccountService.cs  Load -> command -> save, with conflict retry
  Projection.cs      Read model with checkpoint and rebuild
src/Ledger.Demo/      Minimal API used for the live demo (Swagger UI)
tests/Ledger.Tests/  26 xUnit tests against a real SQLite database, including the demo API
```

## Try it

```csharp
var store = new SqliteEventStore("Data Source=ledger.db");
var service = new AccountService(store);
var projection = new BalanceProjection("Data Source=ledger.db", store);

var id = await service.OpenAsync("Dana", "USD");
await service.DepositAsync(id, 100_00);      // amounts are in cents; never use floating point for money
await service.WithdrawAsync(id, 25_00);

await projection.CatchUpAsync();
var view = await projection.GetAsync(id);    // AccountView { BalanceCents = 7500, Status = "Open", ... }
```

## Tests

```bash
dotnet test
```

20 tests, run repeatedly to check for flakiness. They cover the aggregate's rules, event round-tripping and ordering, stale-version rejection and atomic batches, **concurrent deposits and withdrawals**, and the projection (incremental, idempotent, **rebuild equals incremental**, and matching the aggregate after concurrent activity).

## Production notes

- Money is stored as integer cents. Never use `float`/`double` for currency.
- Add **snapshots** for long-lived streams so loading doesn't replay thousands of events.
- Run `CatchUpAsync` in a background worker (or push events over a broker via the [transactional outbox](https://github.com/hamzamehmood46/outbox-pattern-dotnet)).
- Version events carefully: old events must remain readable forever (upcasters).

## License

MIT
