using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Azure.Cosmos;

namespace Pz.Connector.CosmosDb.Tests;

[CollectionDefinition("cosmosdb")]
public sealed class CosmosCollection : ICollectionFixture<CosmosFixture>;

/// <summary>One vNext emulator per test run, over plain http in gateway mode (all the emulator
/// supports). The gateway answers before its storage extension is ready, so readiness is the log
/// line the extension prints plus one successful account read. Container names are unique per call
/// so facts never share state. Helpers talk to the emulator with the raw SDK, deliberately not
/// through the connector: a fact that used the code under test to seed and verify would prove nothing.</summary>
public sealed class CosmosFixture : IAsyncLifetime
{
    public const string Image = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest";
    public const string Database = "pz_tests";

    private IContainer? _container;
    private CosmosClient? _client;

    public string Endpoint { get; private set; } = "";

    public CosmosClient Client => _client ?? throw new InvalidOperationException("the emulator is not running");

    public Database Db => Client.GetDatabase(Database);

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        _container = new ContainerBuilder(Image)
            .WithEnvironment("PROTOCOL", "http")
            .WithPortBinding(8081, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("pgcosmos extension detected"))
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        Endpoint = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(8081)}/";
        _client = new CosmosClient(Endpoint, CosmosRedactor.EmulatorKey, new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway, LimitToEndpoint = true,
        });
        // The log line precedes the extension accepting traffic by a beat; the first account read
        // that succeeds is the readiness signal. Bounded by the SDK's own request retries.
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await _client.ReadAccountAsync().ConfigureAwait(false);
                break;
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable && attempt < 60)
            {
                await Task.Delay(500).ConfigureAwait(false);
            }
        }

        await _client.CreateDatabaseIfNotExistsAsync(Database).ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig() => new()
    {
        ["database"] = Database, ["auth"] = "account_key", ["endpoint"] = Endpoint,
        ["account_key"] = CosmosRedactor.EmulatorKey, ["connection_mode"] = "gateway",
    };

    public static string NewName(string prefix = "pz") => $"{prefix}_{Guid.NewGuid():N}"[..(prefix.Length + 13)];

    public async Task<Container> CreateContainerAsync(string name, string partitionKeyPath = "/id") =>
        (await Db.CreateContainerIfNotExistsAsync(new ContainerProperties(name, partitionKeyPath)).ConfigureAwait(false)).Container;

    /// <summary>Upserts JSON documents; the partition key value is read back out of each document
    /// at the container's own path (top-level only, which every fixture container uses).</summary>
    public async Task UpsertAsync(Container container, IEnumerable<string> documents)
    {
        var path = (await container.ReadContainerAsync().ConfigureAwait(false)).Resource.PartitionKeyPaths[0].TrimStart('/');
        foreach (var chunk in documents.Chunk(200))
        {
            var tasks = chunk.Select(async json =>
            {
                using var doc = JsonDocument.Parse(json);
                var pkValue = doc.RootElement.GetProperty(path);
                var pk = pkValue.ValueKind == JsonValueKind.Number ? new PartitionKey(pkValue.GetDouble()) : new PartitionKey(pkValue.GetString());
                using var body = new MemoryStream(Encoding.UTF8.GetBytes(json));
                using var response = await container.UpsertItemStreamAsync(body, pk).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
            });
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }

    /// <summary>Seeds a fresh container and returns its name.</summary>
    public async Task<string> SeedAsync(IEnumerable<string> documents, string prefix = "pz", string partitionKeyPath = "/id")
    {
        var name = NewName(prefix);
        var container = await CreateContainerAsync(name, partitionKeyPath).ConfigureAwait(false);
        await UpsertAsync(container, documents).ConfigureAwait(false);
        return name;
    }

    public async Task<List<JsonElement>> AllAsync(string container)
    {
        var result = new List<JsonElement>();
        using var iterator = Db.GetContainer(container).GetItemQueryStreamIterator(new QueryDefinition("SELECT * FROM c ORDER BY c._ts"));
        while (iterator.HasMoreResults)
        {
            using var page = await iterator.ReadNextAsync().ConfigureAwait(false);
            page.EnsureSuccessStatusCode();
            using var doc = await JsonDocument.ParseAsync(page.Content).ConfigureAwait(false);
            result.AddRange(doc.RootElement.GetProperty("Documents").EnumerateArray().Select(e => e.Clone()));
        }

        return result;
    }

    public async Task<long> CountAsync(string container)
    {
        using var iterator = Db.GetContainer(container).GetItemQueryStreamIterator(new QueryDefinition("SELECT VALUE COUNT(1) FROM c"));
        using var page = await iterator.ReadNextAsync().ConfigureAwait(false);
        page.EnsureSuccessStatusCode();
        using var doc = await JsonDocument.ParseAsync(page.Content).ConfigureAwait(false);
        return doc.RootElement.GetProperty("Documents")[0].GetInt64();
    }

    public async Task<bool> ExistsAsync(string container)
    {
        try
        {
            await Db.GetContainer(container).ReadContainerAsync().ConfigureAwait(false);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task DeleteContainerAsync(string container)
    {
        if (await ExistsAsync(container).ConfigureAwait(false))
        {
            await Db.GetContainer(container).DeleteContainerAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Documents <c>{id, n, name, pad}</c> for <c>n</c> in [0, rows); <c>id</c> is the
    /// decimal spelling of <c>n</c> (Cosmos ids are strings), <c>n</c> the numeric cursor.</summary>
    public static IEnumerable<string> Rows(int rows, string namePrefix = "n") =>
        Enumerable.Range(0, rows).Select(i => $$"""{"id":"{{i}}","n":{{i}},"name":"{{namePrefix}}{{i}}","pad":"{{new string('x', 80)}}"}""");
}
