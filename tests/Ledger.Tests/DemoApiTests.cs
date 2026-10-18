using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Data.Sqlite;

namespace Ledger.Tests;

public class DemoApiTests : IDisposable
{
    private readonly string _db = Path.Combine(Path.GetTempPath(), $"ledger-demo-{Guid.NewGuid():N}.db");
    private readonly WebApplicationFactory<Program> _app;
    private readonly HttpClient _client;

    public DemoApiTests()
    {
        _app = new WebApplicationFactory<Program>().WithWebHostBuilder(b => b.UseSetting("Ledger:Db", $"Data Source={_db}"));
        _client = _app.CreateClient();
    }

    public void Dispose()
    {
        _client.Dispose();
        _app.Dispose();
        SqliteConnection.ClearAllPools();
        try { File.Delete(_db); } catch (IOException) { }
    }

    private async Task<Guid> Open(string owner = "Ava")
    {
        var response = await _client.PostAsJsonAsync("/api/accounts", new { owner, currency = "usd" });
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetGuid();
    }

    private async Task<JsonElement> Get(string url) => await _client.GetFromJsonAsync<JsonElement>(url);

    [Fact]
    public async Task An_account_is_rebuilt_from_its_events()
    {
        var id = await Open();
        await _client.PostAsJsonAsync($"/api/accounts/{id}/deposit", new { cents = 1000 });
        await _client.PostAsJsonAsync($"/api/accounts/{id}/withdraw", new { cents = 250 });

        var account = await Get($"/api/accounts/{id}");
        var events = await Get($"/api/accounts/{id}/events");

        Assert.Equal(750, account.GetProperty("balanceCents").GetInt64());
        Assert.Equal(["AccountOpened", "MoneyDeposited", "MoneyWithdrawn"], events.EnumerateArray().Select(e => e.GetProperty("type").GetString()));
    }

    [Fact]
    public async Task An_overdraft_is_refused_and_writes_nothing()
    {
        var id = await Open();
        await _client.PostAsJsonAsync($"/api/accounts/{id}/deposit", new { cents = 100 });

        var response = await _client.PostAsJsonAsync($"/api/accounts/{id}/withdraw", new { cents = 101 });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Equal(2, (await Get($"/api/accounts/{id}/events")).GetArrayLength());
    }

    [Fact]
    public async Task An_unknown_account_is_a_404()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync($"/api/accounts/{Guid.NewGuid()}")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.PostAsJsonAsync($"/api/accounts/{Guid.NewGuid()}/deposit", new { cents = 5 })).StatusCode);
    }

    [Fact]
    public async Task A_non_empty_account_cannot_be_closed_but_an_empty_one_can()
    {
        var id = await Open();
        await _client.PostAsJsonAsync($"/api/accounts/{id}/deposit", new { cents = 100 });
        Assert.Equal(HttpStatusCode.UnprocessableEntity, (await _client.PostAsync($"/api/accounts/{id}/close", null)).StatusCode);

        await _client.PostAsJsonAsync($"/api/accounts/{id}/withdraw", new { cents = 100 });
        Assert.Equal(HttpStatusCode.OK, (await _client.PostAsync($"/api/accounts/{id}/close", null)).StatusCode);
        Assert.Equal(HttpStatusCode.UnprocessableEntity, (await _client.PostAsJsonAsync($"/api/accounts/{id}/deposit", new { cents = 1 })).StatusCode);
    }

    [Fact]
    public async Task The_read_model_lags_until_it_catches_up_and_can_be_rebuilt_from_the_events()
    {
        var id = await Open("Ben");
        await _client.PostAsJsonAsync($"/api/accounts/{id}/deposit", new { cents = 4200 });

        Assert.Equal(0, (await Get("/api/accounts")).GetProperty("accounts").GetArrayLength());
        await _client.PostAsync("/api/projection/catch-up", null);
        var view = (await Get("/api/accounts")).GetProperty("accounts").EnumerateArray().Single();
        Assert.Equal(4200, view.GetProperty("balanceCents").GetInt64());

        var rebuilt = await (await _client.PostAsync("/api/projection/rebuild", null)).Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(2, rebuilt.GetProperty("replayed").GetInt32());
        Assert.Equal(4200, (await Get("/api/accounts")).GetProperty("accounts").EnumerateArray().Single().GetProperty("balanceCents").GetInt64());
    }

    [Fact]
    public async Task Ten_workers_withdrawing_from_five_dollars_never_overdraw_it()
    {
        var response = await _client.PostAsJsonAsync("/api/demo/contention", new { workers = 10, withdrawCents = 100, startCents = 500 });
        var result = await response.Content.ReadFromJsonAsync<JsonElement>();

        Assert.Equal(5, result.GetProperty("succeeded").GetInt32());
        Assert.Equal(5, result.GetProperty("rejected").GetInt32());
        Assert.Equal(0, result.GetProperty("finalBalanceCents").GetInt64());
        Assert.True(result.GetProperty("neverOverdrawn").GetBoolean());
    }
}
