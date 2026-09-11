using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class DatasetConfigTests
{
    private static CosmosDatasetConfig? Parse(Dictionary<string, object?> options, out List<string> errors)
    {
        errors = [];
        return CosmosDatasetConfig.Parse(new DatasetSpec("cosmos", "orders", options), errors);
    }

    [Fact]
    public void Defaults_come_from_the_entity_name()
    {
        var config = Parse(new(), out var errors);
        Assert.Empty(errors);
        Assert.Equal("orders", config!.Container);
        Assert.Null(config.Query);
        Assert.Null(config.Fields);
        Assert.Equal(CosmosDatasetConfig.DefaultSampleSize, config.SampleSize);
        Assert.Equal(CosmosDatasetConfig.DefaultPageSize, config.PageSize);
        Assert.Null(config.Partitions);
    }

    [Fact]
    public void Every_option_parses_including_integral_doubles()
    {
        var config = Parse(new()
        {
            ["container"] = "c1", ["query"] = "SELECT * FROM c WHERE c.x = 1",
            ["fields"] = new Dictionary<string, object?> { ["id"] = "string", ["total"] = "decimal", ["a.b"] = "int64" },
            ["sample_size"] = 50.0, ["page_size"] = 500.0, ["partitions"] = 3.0, ["columns"] = new Dictionary<string, object?>(),
        }, out var errors);
        Assert.Empty(errors);
        Assert.Equal("c1", config!.Container);
        Assert.Equal(50, config.SampleSize);
        Assert.Equal(500, config.PageSize);
        Assert.Equal(3, config.Partitions);
        Assert.Equal(["id", "total", "a.b"], config.Fields!.Select(f => f.Name));
        Assert.Equal(ColumnKind.Decimal, config.Fields![1].Kind);
    }

    [Fact]
    public void Partitions_auto_is_null()
    {
        Assert.Null(Parse(new() { ["partitions"] = "auto" }, out var errors)!.Partitions);
        Assert.Empty(errors);
    }

    [Fact]
    public void Bad_options_are_all_reported()
    {
        Assert.Null(Parse(new()
        {
            ["container"] = "", ["fields"] = "nope", ["sample_size"] = 0, ["page_size"] = 20000, ["partitions"] = "some", ["bogus"] = 1,
        }, out var errors));
        Assert.Contains(errors, e => e.Contains("'container' must be a non-empty string", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'fields' must be a mapping", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'sample_size' must be an integer between 1 and 100000", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'page_size' must be an integer between 1 and 10000", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'partitions' must be an integer of at least 1", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("unknown read option 'bogus'", StringComparison.Ordinal));
    }

    [Fact]
    public void Fields_with_unknown_type_or_empty_path_are_reported()
    {
        Assert.Null(Parse(new() { ["fields"] = new Dictionary<string, object?> { ["x"] = "blob", ["a..b"] = "string" } }, out var errors));
        Assert.Contains(errors, e => e.Contains("field 'x' has unknown type 'blob'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'fields' has an empty field path", StringComparison.Ordinal));
    }
}
