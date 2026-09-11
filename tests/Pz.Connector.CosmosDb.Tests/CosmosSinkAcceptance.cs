using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.CosmosDb.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector stores each row as {"id":"<n>","name":...} in a container partitioned on /id, so
/// read-back parses the string id back into the Int64 column. Containers are created per
/// test-class instance (xunit instantiates per fact) with fresh names, so facts never see each
/// other's documents. Replace is asserted as a refusal.</summary>
[Collection("cosmosdb")]
[Trait("Category", "Docker")]
public sealed class CosmosSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly CosmosFixture _cosmos;
    private readonly string _append = CosmosFixture.NewName("append");
    private readonly string _merge = CosmosFixture.NewName("merge");

    public CosmosSinkAcceptance(CosmosFixture cosmos)
    {
        _cosmos = cosmos;
        DockerFacts.SkipUnlessDocker();
        Task.WhenAll(cosmos.CreateContainerAsync(_append), cosmos.CreateContainerAsync(_merge)).GetAwaiter().GetResult();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new CosmosConnector();

    protected override ConnectorConfig ValidConfig => new(_cosmos.ConnectionConfig());

    protected override OutputSpec SmallOutput => new("cosmosdb", _append, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("cosmosdb", _merge, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override async Task ResetMergeTargetAsync()
    {
        await _cosmos.DeleteContainerAsync(_merge);
        await _cosmos.CreateContainerAsync(_merge);
    }

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        if (!await _cosmos.ExistsAsync(spec.Output))
        {
            return [];
        }

        var docs = await _cosmos.AllAsync(spec.Output);
        if (docs.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var doc in docs)
        {
            ids.Append(long.Parse(doc.GetProperty("id").GetString()!, CultureInfo.InvariantCulture));
            names.Append(doc.GetProperty("name").GetString()!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], docs.Count)];
    }
}
