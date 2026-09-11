using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

/// <summary>Against a real account: PZ_COSMOS_ENDPOINT + PZ_COSMOS_KEY. A throwaway database per
/// run, dropped in teardown. Never runs in CI. Proves what the emulator cannot: direct mode, a
/// container with several physical partitions, hierarchical partition keys, and that a real account
/// (unlike the vNext emulator) actually rejects a wrong account key.</summary>
[Trait("Category", "Live")]
public sealed class LiveCosmosFacts : IAsyncLifetime
{
    private static readonly string? Endpoint = Environment.GetEnvironmentVariable("PZ_COSMOS_ENDPOINT");
    private static readonly string? Key = Environment.GetEnvironmentVariable("PZ_COSMOS_KEY");
    private readonly string _database = "pz_live_" + Guid.NewGuid().ToString("N")[..8];
    private CosmosClient? _client;

    private static void SkipUnlessLive() => Skip.If(string.IsNullOrEmpty(Endpoint) || string.IsNullOrEmpty(Key), "PZ_COSMOS_ENDPOINT / PZ_COSMOS_KEY not set");

    public async Task InitializeAsync()
    {
        if (string.IsNullOrEmpty(Endpoint) || string.IsNullOrEmpty(Key))
        {
            return;
        }

        _client = new CosmosClient(Endpoint, Key);
        await _client.CreateDatabaseIfNotExistsAsync(_database);
    }

    public async Task DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.GetDatabase(_database).DeleteAsync();
            _client.Dispose();
        }
    }

    private Dictionary<string, object?> Config(string mode) => new()
    {
        ["database"] = _database, ["auth"] = "account_key", ["endpoint"] = Endpoint, ["account_key"] = Key, ["connection_mode"] = mode,
    };

    [SkippableFact]
    public async Task Direct_mode_reads_a_multi_partition_container_across_feed_ranges()
    {
        SkipUnlessLive();
        // 20000 RU/s provisions two physical partitions; the container is deleted with the database.
        var container = (await _client!.GetDatabase(_database).CreateContainerAsync(new ContainerProperties("multi", "/pk"), throughput: 20000)).Container;
        var docs = Enumerable.Range(0, 2000).Select(i => $$"""{"id":"{{i}}","pk":"p{{i % 50}}","n":{{i}}}""");
        var fixture = new CosmosFixture();
        await fixture.UpsertAsync(container, docs);

        ISourceConnector connector = new CosmosConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(Config("direct")), CancellationToken.None);
        var spec = new DatasetSpec("cosmosdb", "multi", new Dictionary<string, object?> { ["fields"] = new Dictionary<string, object?> { ["n"] = "int64" } });
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        Assert.True(partitions.Count >= 2, $"expected >= 2 feed ranges, got {partitions.Count}");

        var total = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                total += batch.Length;
                batch.Dispose();
            }
        }

        Assert.Equal(2000, total);

        var folded = await source.PlanReadAsync(new DatasetSpec("cosmosdb", "multi", new Dictionary<string, object?>(spec.Options) { ["partitions"] = 1 }), ReadHints.None, CancellationToken.None);
        Assert.Single(folded);
    }

    [SkippableFact]
    public async Task Hierarchical_partition_keys_write_and_read_back()
    {
        SkipUnlessLive();
        await _client!.GetDatabase(_database).CreateContainerAsync(new ContainerProperties("hpk", new List<string> { "/tenant", "/region" }));
        var ids = new Apache.Arrow.StringArray.Builder().Append("a").Append("b").Build();
        var tenants = new Apache.Arrow.StringArray.Builder().Append("t1").Append("t1").Build();
        var regions = new Apache.Arrow.StringArray.Builder().Append("eu").Append("us").Build();
        var schema = new Apache.Arrow.Schema(
        [
            new Apache.Arrow.Field("id", Apache.Arrow.Types.StringType.Default, false),
            new Apache.Arrow.Field("tenant", Apache.Arrow.Types.StringType.Default, false),
            new Apache.Arrow.Field("region", Apache.Arrow.Types.StringType.Default, false),
        ], null);
        using var batch = new Apache.Arrow.RecordBatch(schema, [ids, tenants, regions], 2);

        ISinkConnector sinkConnector = new CosmosConnector();
        await using var sink = await sinkConnector.OpenAsync(new ConnectorConfig(Config("direct")), CancellationToken.None);
        var spec = new OutputSpec("cosmosdb", "hpk", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id", "tenant", "region"] };
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        Assert.Equal(2, (await session.CommitAsync(CancellationToken.None)).RowsWritten);

        ISourceConnector sourceConnector = new CosmosConnector();
        await using var source = await sourceConnector.OpenAsync(new ConnectorConfig(Config("direct")), CancellationToken.None);
        var read = await source.PlanReadAsync(new DatasetSpec("cosmosdb", "hpk", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var rows = 0;
        foreach (var p in read)
        {
            await foreach (var b in p.ReadAsync(BatchOptions.Default, CancellationToken.None)) { rows += b.Length; b.Dispose(); }
        }

        Assert.Equal(2, rows);
    }

    /// <summary>The vNext emulator does not validate account keys at all (Task 4's Docker-side
    /// version of this fact was removed for that reason); a real account does, so this is the only
    /// place that proves a wrong key is rejected -- and that the rejection echoes neither key.</summary>
    [SkippableFact]
    public async Task Check_connection_with_a_wrong_key_fails_without_echoing_it()
    {
        SkipUnlessLive();
        const string wrongKey = "d29ybmdrZXlkb2Vzbm90bWF0Y2h0aGVyZWFsYWNjb3VudGtleXZhbHVlPT0=";
        var config = Config("gateway");
        config["account_key"] = wrongKey;

        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.DoesNotContain(wrongKey, check.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Key!, check.Message, StringComparison.Ordinal);
    }
}
