using Ledger;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

var db = builder.Configuration["Ledger:Db"] ?? "Data Source=ledger-demo.db";
var store = new SqliteEventStore(db);
builder.Services.AddSingleton<IEventStore>(store);
builder.Services.AddSingleton(new AccountService(store));
builder.Services.AddSingleton(new BalanceProjection(db, store));

builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(o => o.SwaggerDoc("v1", new OpenApiInfo
{
    Title = "Event-Sourced Ledger",
    Version = "v1",
    Description = "Bank accounts stored as immutable events, with optimistic concurrency and a rebuildable read model. Amounts are in cents.",
}));

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();

// Business-rule rejections and missing accounts become clean HTTP answers.
async Task<IResult> Run(Func<Task<IResult>> action)
{
    try
    {
        return await action();
    }
    catch (AccountNotFoundException ex)
    {
        return Results.NotFound(new { error = ex.Message });
    }
    catch (DomainException ex)
    {
        return Results.UnprocessableEntity(new { error = ex.Message });
    }
}

var api = app.MapGroup("/api");

api.MapPost("/accounts", (OpenRequest body, AccountService svc) => Run(async () =>
{
    var id = await svc.OpenAsync(body.Owner ?? "", body.Currency ?? "USD");
    return Results.Created($"/api/accounts/{id}", new { id });
}));

api.MapGet("/accounts/{id:guid}", (Guid id, AccountService svc) => Run(async () =>
{
    var a = await svc.GetAsync(id);
    return Results.Ok(new { a.Id, a.Owner, a.Currency, a.BalanceCents, status = a.IsClosed ? "Closed" : "Open", version = a.Version, note = "Rebuilt right now by replaying this account's events." });
}));

api.MapPost("/accounts/{id:guid}/deposit", (Guid id, AmountRequest body, AccountService svc) => Run(async () =>
{
    await svc.DepositAsync(id, body.Cents);
    return Results.Ok(await Summary(svc, id));
}));

api.MapPost("/accounts/{id:guid}/withdraw", (Guid id, AmountRequest body, AccountService svc) => Run(async () =>
{
    await svc.WithdrawAsync(id, body.Cents);
    return Results.Ok(await Summary(svc, id));
}));

api.MapPost("/accounts/{id:guid}/close", (Guid id, AccountService svc) => Run(async () =>
{
    await svc.CloseAsync(id);
    return Results.Ok(await Summary(svc, id));
}));

api.MapGet("/accounts/{id:guid}/events", async (Guid id, IEventStore events) =>
{
    var history = await events.ReadStreamAsync(id);
    return history.Count == 0
        ? Results.NotFound(new { error = "No events for that account." })
        : Results.Ok(history.Select(e => new { e.Sequence, e.Version, type = e.Event.GetType().Name, data = (object)e.Event, e.OccurredAt }));
});

// ---- The read side (CQRS) ----
api.MapGet("/accounts", async (BalanceProjection p) => Results.Ok(new { note = "This is the read model. It only knows about events up to its last catch-up.", accounts = await p.ListAsync() }));

api.MapPost("/projection/catch-up", async (BalanceProjection p) => Results.Ok(new { applied = await p.CatchUpAsync() }));

api.MapPost("/projection/rebuild", async (BalanceProjection p) => Results.Ok(new { replayed = await p.RebuildAsync(), note = "The table was dropped and rebuilt purely from the event log." }));

// ---- The headline guarantee ----
api.MapPost("/demo/contention", async (ContentionRequest? body, AccountService svc) =>
{
    var workers = Math.Clamp(body?.Workers ?? 10, 2, 50);
    var withdraw = Math.Clamp(body?.WithdrawCents ?? 100, 1, 100_000);
    var start = Math.Clamp(body?.StartCents ?? 500, 1, 1_000_000);

    var id = await svc.OpenAsync("Contention demo", "USD");
    await svc.DepositAsync(id, start);

    var outcomes = await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
    {
        try
        {
            await svc.WithdrawAsync(id, withdraw);
            return "succeeded";
        }
        catch (InsufficientFundsException)
        {
            return "rejected";
        }
    })));

    var account = await svc.GetAsync(id);
    var succeeded = outcomes.Count(o => o == "succeeded");
    return Results.Ok(new
    {
        accountId = id,
        workers,
        startingBalanceCents = start,
        withdrawalCents = withdraw,
        succeeded,
        rejected = workers - succeeded,
        finalBalanceCents = account.BalanceCents,
        neverOverdrawn = account.BalanceCents >= 0,
        explanation = "All workers raced at once. Optimistic concurrency made losers reload and re-check the balance, so the account can never go below zero and no withdrawal is lost.",
    });
});

app.Run();

static async Task<object> Summary(AccountService svc, Guid id)
{
    var a = await svc.GetAsync(id);
    return new { a.Id, a.BalanceCents, status = a.IsClosed ? "Closed" : "Open", version = a.Version };
}

public record OpenRequest(string? Owner, string? Currency);
public record AmountRequest(long Cents);
public record ContentionRequest(int? Workers, long? WithdrawCents, long? StartCents);

public partial class Program;
