using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.CosmosDb.Tests;

/// <summary>TestKit source contract against the emulator. SmallDataset is a container of 120
/// ~100-byte documents whose declared first column is the int64 <c>n</c> (Cosmos <c>id</c> is a
/// string, so the kit's inclusive-watermark fact -- which assumes an int "id" cursor -- is
/// overridden below with the same logic on <c>n</c>). LargeDataset is 150k documents so mid-read
/// cancellation is observable. BoundedWindowDataset seeds n = 0..10. All three are seeded once per
/// fixture on first use.</summary>
[Collection("cosmosdb")]
[Trait("Category", "Docker")]
public sealed class CosmosSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _small;
    private static string? _large;
    private static string? _window;
    private readonly CosmosFixture _cosmos;

    private static readonly Dictionary<string, object?> Fields = new()
    {
        ["fields"] = new Dictionary<string, object?> { ["n"] = "int64", ["name"] = "string", ["pad"] = "string" },
    };

    public CosmosSourceAcceptance(CosmosFixture cosmos)
    {
        _cosmos = cosmos;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_cosmos.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("cosmosdb", _small!, Fields);

    protected override DatasetSpec? LargeDataset => new("cosmosdb", _large!, Fields);

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("cosmosdb", _window!, Fields) { WatermarkCursor = "n", WatermarkValue = "3", WatermarkUpperBound = "7" };

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) =>
        new("cosmosdb", _small!, new Dictionary<string, object?>(Fields) { ["partitions"] = partitions });

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new CosmosConnector();

    /// <summary>The kit's fact on cursor "id"; Cosmos ids are strings, so the same contract is proven
    /// on the int64 column <c>n</c> that leads the declared schema.</summary>
    public override async Task Inclusive_watermark_bound_returns_boundary_row()
    {
        DockerFacts.SkipUnlessDocker();
        var connector = CreateSource();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var unfiltered = await FirstColumnAsync(source, SmallDataset);
        var min = unfiltered.Min();
        var exclusive = await FirstColumnAsync(source, SmallDataset with { WatermarkCursor = "n", WatermarkValue = min.ToString(CultureInfo.InvariantCulture) });
        var inclusive = await FirstColumnAsync(source, SmallDataset with { WatermarkCursor = "n", WatermarkValue = min.ToString(CultureInfo.InvariantCulture), WatermarkLowerInclusive = true });

        Assert.DoesNotContain(min, exclusive);
        Assert.Contains(min, inclusive);
        Assert.Equal(exclusive.Count + 1, inclusive.Count);
    }

    private static async Task<List<long>> FirstColumnAsync(ISource source, DatasetSpec spec)
    {
        var values = new List<long>();
        foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var column = (Apache.Arrow.Int64Array)batch.Column(0);
                for (var i = 0; i < batch.Length; i++)
                {
                    values.Add(column.GetValue(i)!.Value);
                }

                batch.Dispose();
            }
        }

        return values;
    }

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            _small ??= await _cosmos.SeedAsync(CosmosFixture.Rows(120), "small");
            _large ??= await _cosmos.SeedAsync(CosmosFixture.Rows(150_000), "large");
            _window ??= await _cosmos.SeedAsync(CosmosFixture.Rows(11), "window");
        }
        finally
        {
            Seed.Release();
        }
    }
}
