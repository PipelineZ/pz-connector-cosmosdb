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

    private static async Task<WriteResult> WriteAsync(CosmosFixture cosmos, OutputSpec spec, Apache.Arrow.Schema schema, params Apache.Arrow.RecordBatch[] batches)
    {
        ISinkConnector connector = new CosmosConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(cosmos.ConnectionConfig()), CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        return await session.CommitAsync(CancellationToken.None);
    }

    private static Apache.Arrow.RecordBatch Rows(params (string Id, string Region, long N)[] rows)
    {
        var ids = new Apache.Arrow.StringArray.Builder();
        var regions = new Apache.Arrow.StringArray.Builder();
        var ns = new Apache.Arrow.Int64Array.Builder();
        foreach (var (id, region, n) in rows)
        {
            ids.Append(id); regions.Append(region); ns.Append(n);
        }

        var schema = new Apache.Arrow.Schema(
        [
            new Apache.Arrow.Field("id", Apache.Arrow.Types.StringType.Default, false),
            new Apache.Arrow.Field("region", Apache.Arrow.Types.StringType.Default, false),
            new Apache.Arrow.Field("n", Apache.Arrow.Types.Int64Type.Default, true),
        ], null);
        return new Apache.Arrow.RecordBatch(schema, [ids.Build(), regions.Build(), ns.Build()], rows.Length);
    }

    [SkippableFact]
    public async Task Merge_upserts_on_partition_key_and_id()
    {
        DockerFacts.SkipUnlessDocker();
        var name = CosmosFixture.NewName("merge_pk");
        await cosmos.CreateContainerAsync(name, "/region");
        var spec = new OutputSpec("cosmosdb", name, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        using var first = Rows(("a", "eu", 1), ("b", "eu", 2));
        await WriteAsync(cosmos, spec, first.Schema, first);
        using var second = Rows(("a", "eu", 10), ("c", "us", 3));
        var result = await WriteAsync(cosmos, spec, second.Schema, second);
        Assert.Equal(2, result.RowsWritten);
        var docs = await cosmos.AllAsync(name);
        Assert.Equal(3, docs.Count);
        Assert.Equal(10, docs.Single(d => d.GetProperty("id").GetString() == "a").GetProperty("n").GetInt64());
    }

    /// <summary>Pins the same-identity chain in CosmosWriteSession: two rows sharing (partition
    /// key, id), sent as one batch through one merge session, must not race on the wire -- the
    /// second row's value is the one that survives.</summary>
    [SkippableFact]
    public async Task Merge_two_rows_of_the_same_identity_in_one_batch_resolve_last_writer_wins()
    {
        DockerFacts.SkipUnlessDocker();
        var name = CosmosFixture.NewName("merge_dup");
        await cosmos.CreateContainerAsync(name, "/region");
        var spec = new OutputSpec("cosmosdb", name, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        using var batch = Rows(("a", "eu", 1), ("a", "eu", 2));
        var result = await WriteAsync(cosmos, spec, batch.Schema, batch);
        Assert.Equal(2, result.RowsWritten);
        var docs = await cosmos.AllAsync(name);
        var doc = Assert.Single(docs);
        Assert.Equal(2, doc.GetProperty("n").GetInt64());
    }

    [SkippableFact]
    public async Task Append_conflict_on_an_explicit_id_is_fatal()
    {
        DockerFacts.SkipUnlessDocker();
        var name = CosmosFixture.NewName("conflict");
        await cosmos.CreateContainerAsync(name, "/region");
        var spec = new OutputSpec("cosmosdb", name, "append", "fail_on_change", new Dictionary<string, object?>());
        using var batch = Rows(("a", "eu", 1));
        await WriteAsync(cosmos, spec, batch.Schema, batch);
        using var again = Rows(("a", "eu", 1));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(cosmos, spec, again.Schema, again));
        Assert.False(ex.IsTransient);
        Assert.Contains("(409", ex.Message);
        Assert.Contains("'a'", ex.Message);
    }

    [SkippableFact]
    public async Task Missing_container_and_partition_key_mismatch_are_refused_up_front()
    {
        DockerFacts.SkipUnlessDocker();
        using var batch = Rows(("a", "eu", 1));
        var missing = new OutputSpec("cosmosdb", "no_such_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(cosmos, missing, batch.Schema, batch));
        Assert.Contains("PZCS0302", ex.Message);

        var name = CosmosFixture.NewName("pkmismatch");
        await cosmos.CreateContainerAsync(name, "/tenant");
        var mismatch = new OutputSpec("cosmosdb", name, "append", "fail_on_change", new Dictionary<string, object?>());
        var pk = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(cosmos, mismatch, batch.Schema, batch));
        Assert.Contains("PZCS0303", pk.Message);
        Assert.Contains("'/tenant'", pk.Message);
    }

    [SkippableFact]
    public async Task Replace_is_refused_before_touching_the_container()
    {
        DockerFacts.SkipUnlessDocker();
        using var batch = Rows(("a", "eu", 1));
        var spec = new OutputSpec("cosmosdb", "never_created", "replace", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(cosmos, spec, batch.Schema, batch));
        Assert.Contains("PZCS0301", ex.Message);
        Assert.False(await cosmos.ExistsAsync("never_created"));
    }

    [SkippableFact]
    public async Task Round_trip_through_source_and_sink_keeps_values()
    {
        DockerFacts.SkipUnlessDocker();
        var source = await cosmos.SeedAsync(["""{"id":"r1","region":"eu","n":5,"address":{"city":"Oslo"},"tags":[1,2]}"""]);
        var (_, batches) = await ReadAllAsync(cosmos, new DatasetSpec("cosmosdb", source, new Dictionary<string, object?>
        {
            ["fields"] = new Dictionary<string, object?> { ["id"] = "string", ["region"] = "string", ["n"] = "int64", ["address.city"] = "string", ["tags"] = "json" },
        }));
        var target = CosmosFixture.NewName("roundtrip");
        await cosmos.CreateContainerAsync(target, "/region");
        var spec = new OutputSpec("cosmosdb", target, "append", "fail_on_change", new Dictionary<string, object?>());
        await WriteAsync(cosmos, spec, batches[0].Schema, batches.ToArray());
        var doc = Assert.Single(await cosmos.AllAsync(target));
        Assert.Equal("Oslo", doc.GetProperty("address").GetProperty("city").GetString());
        // The "json" field carries the value's raw wire text verbatim (DocumentBatchBuilder's
        // documented no-reserialization contract), and the vNext emulator pretty-prints query
        // responses -- verified directly against its raw bytes -- so the text the source read (and
        // the sink wrote back unchanged, inside a JSON string) is "[\n  1,\n  2\n]", not the compact
        // form the seed document was written with. Parsing it back is the structural comparison
        // that survives that whitespace, matching the reordering accommodation already made for
        // this same emulator quirk elsewhere in this file.
        using var tags = System.Text.Json.JsonDocument.Parse(doc.GetProperty("tags").GetString()!);
        Assert.Equal([1, 2], tags.RootElement.EnumerateArray().Select(e => e.GetInt32()));
        foreach (var b in batches) b.Dispose();
    }
}
