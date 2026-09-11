using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

[Collection("cosmosdb")]
[Trait("Category", "Docker")]
public sealed class CosmosConnectorFacts(CosmosFixture cosmos)
{
    [Fact]
    public async Task Validate_reports_every_config_error_at_once()
    {
        var result = await new CosmosConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "account_key" }), CancellationToken.None);
        Assert.Contains(result.Errors, e => e.Contains("'database' is required", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'endpoint' is required", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'account_key' is required", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Check_connection_reports_the_account()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(cosmos.ConnectionConfig()), CancellationToken.None);
        Assert.True(check.Ok, check.Message);
        Assert.Contains("Cosmos DB account", check.Message);
        Assert.Contains("consistency", check.Message);
    }

    [SkippableFact]
    public async Task Check_connection_names_a_missing_database()
    {
        DockerFacts.SkipUnlessDocker();
        var config = cosmos.ConnectionConfig();
        config["database"] = "no_such_db";
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("no_such_db", check.Message);
        Assert.Contains("PZCS0102", check.Message);
    }

    /// <summary>The emulator's own reported default drives the requested level, rather than assuming
    /// Session: a spike against the pinned vnext-latest image found its default is Eventual, not
    /// Session, so 'strong' (always the strongest level) is requested unless the account itself
    /// already defaults to strong.</summary>
    [SkippableFact]
    public async Task Check_connection_refuses_a_consistency_raise()
    {
        DockerFacts.SkipUnlessDocker();
        var accountDefault = (await cosmos.Client.ReadAccountAsync()).Consistency.DefaultConsistencyLevel;
        var requested = accountDefault == ConsistencyLevel.Strong ? ConsistencyLevel.BoundedStaleness : ConsistencyLevel.Strong;

        var config = cosmos.ConnectionConfig();
        config["consistency"] = ConfigName(requested);
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.Contains("PZCS0101", check.Message);
        Assert.Contains(ConfigName(requested), check.Message);
        Assert.Contains(ConfigName(accountDefault), check.Message);
    }

    private static string ConfigName(ConsistencyLevel level) => level switch
    {
        ConsistencyLevel.Eventual => "eventual",
        ConsistencyLevel.ConsistentPrefix => "consistent_prefix",
        ConsistencyLevel.Session => "session",
        ConsistencyLevel.BoundedStaleness => "bounded_staleness",
        ConsistencyLevel.Strong => "strong",
        _ => throw new ArgumentOutOfRangeException(nameof(level), level, "unknown consistency level"),
    };

    private static async Task<(Apache.Arrow.Schema Schema, List<Apache.Arrow.RecordBatch> Batches)> ReadAllAsync(CosmosFixture cosmos, DatasetSpec spec, ReadHints? hints = null)
    {
        ISourceConnector connector = new CosmosConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(cosmos.ConnectionConfig()), CancellationToken.None);
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var batches = new List<Apache.Arrow.RecordBatch>();
        foreach (var partition in await source.PlanReadAsync(spec, hints ?? ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batches.Add(batch);
            }
        }

        return (schema, batches);
    }

    [SkippableFact]
    public async Task Inferred_schema_flattens_and_reads_nested_documents()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(
        [
            """{"id":"a","n":1,"address":{"city":"Paris"},"tags":["x"],"price":"12.50"}""",
            """{"id":"b","n":2,"address":{"city":"Rome"},"tags":[],"price":"7.25"}""",
        ]);
        var (schema, batches) = await ReadAllAsync(cosmos, new DatasetSpec("cosmosdb", name, new Dictionary<string, object?>()));
        // Cosmos DB's query engine reorders a "SELECT *" result's top-level properties (scalars and
        // arrays ahead of nested objects) rather than preserving the document's own written order, so
        // "address" -- flattened to "address.city" -- surfaces after "tags"/"price" here, not between
        // "n" and "tags" as the document was written; verified against the emulator's raw response.
        Assert.Equal(["n", "tags", "price", "address.city", "_ts", "id"], schema.FieldsList.Select(f => f.Name));
        var rows = batches.Sum(b => b.Length);
        Assert.Equal(2, rows);
        foreach (var b in batches) b.Dispose();
    }

    [SkippableFact]
    public async Task Declared_fields_and_column_pruning_shape_the_batches()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(CosmosFixture.Rows(5));
        var spec = new DatasetSpec("cosmosdb", name, new Dictionary<string, object?>
        {
            ["fields"] = new Dictionary<string, object?> { ["n"] = "int64", ["name"] = "string", ["_ts"] = "timestamp" },
        });
        var (_, batches) = await ReadAllAsync(cosmos, spec, new ReadHints(Columns: ["name", "n"]));
        var batch = Assert.Single(batches);
        Assert.Equal(["name", "n"], batch.Schema.FieldsList.Select(f => f.Name));
        batch.Dispose();
    }

    [SkippableFact]
    public async Task User_query_is_honoured_and_bounds_apply_on_top()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(CosmosFixture.Rows(20));
        var spec = new DatasetSpec("cosmosdb", name, new Dictionary<string, object?>
        {
            ["query"] = "SELECT c.id, c.n FROM c WHERE c.n < 15",
            ["fields"] = new Dictionary<string, object?> { ["n"] = "int64" },
        }) { WatermarkCursor = "n", WatermarkValue = "9" };
        var (_, batches) = await ReadAllAsync(cosmos, spec);
        var values = batches.SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((Apache.Arrow.Int64Array)b.Column(0)).GetValue(i)!.Value)).Order().ToList();
        Assert.Equal([10L, 11L, 12L, 13L, 14L], values);
        foreach (var b in batches) b.Dispose();
    }

    [SkippableFact]
    public async Task Ts_is_a_usable_cursor()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(CosmosFixture.Rows(3));
        var spec = new DatasetSpec("cosmosdb", name, new Dictionary<string, object?>()) { WatermarkCursor = "_ts", WatermarkValue = "0" };
        var (_, batches) = await ReadAllAsync(cosmos, spec);
        Assert.Equal(3, batches.Sum(b => b.Length));
        foreach (var b in batches) b.Dispose();
    }

    [SkippableFact]
    public async Task Empty_container_without_fields_is_refused_and_missing_container_is_named()
    {
        DockerFacts.SkipUnlessDocker();
        var empty = CosmosFixture.NewName("empty");
        await cosmos.CreateContainerAsync(empty);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(cosmos, new DatasetSpec("cosmosdb", empty, new Dictionary<string, object?>())));
        Assert.Contains("declare the columns under fields:", ex.Message);

        var missing = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(cosmos, new DatasetSpec("cosmosdb", "no_such_container", new Dictionary<string, object?>())));
        Assert.Contains("container 'no_such_container' does not exist", missing.Message);
        Assert.False(missing.IsTransient);
    }

    [SkippableFact]
    public async Task A_lossy_value_names_the_document()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(["""{"id":"bad-one","n":1.5}"""]);
        var spec = new DatasetSpec("cosmosdb", name, new Dictionary<string, object?> { ["fields"] = new Dictionary<string, object?> { ["n"] = "int64" } });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(cosmos, spec));
        Assert.Contains("field 'n' of document bad-one", ex.Message);
    }

    [SkippableFact]
    public async Task Query_syntax_errors_are_fatal_with_the_service_reason()
    {
        DockerFacts.SkipUnlessDocker();
        var name = await cosmos.SeedAsync(CosmosFixture.Rows(1));
        var spec = new DatasetSpec("cosmosdb", name, new Dictionary<string, object?> { ["query"] = "SELECT FROM WHERE", ["fields"] = new Dictionary<string, object?> { ["n"] = "int64" } });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(cosmos, spec));
        Assert.False(ex.IsTransient);
        Assert.Contains("(400", ex.Message);
    }
}
