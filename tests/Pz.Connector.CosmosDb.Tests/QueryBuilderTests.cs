using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class QueryBuilderTests
{
    private static readonly ColumnPlan Plan = new([
        ColumnSpec.Of("n", ColumnKind.Int64), ColumnSpec.Of("address.city", ColumnKind.String), ColumnSpec.Of("_ts", ColumnKind.Timestamp),
        ColumnSpec.Of("when", ColumnKind.Timestamp), ColumnSpec.Of("day", ColumnKind.Date), ColumnSpec.Of("id", ColumnKind.String),
    ]);

    private static CosmosDatasetConfig Dataset(string? query = null) => new("orders", query, null, 1000, 1000, null);

    private static DatasetSpec Spec(string? cursor = null, string? lower = null, string? upper = null, bool inclusive = false) =>
        new DatasetSpec("cosmos", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = cursor, WatermarkValue = lower, WatermarkUpperBound = upper, WatermarkLowerInclusive = inclusive,
        };

    private static IReadOnlyDictionary<string, object> Parameters(QueryDefinition query) =>
        query.GetQueryParameters().ToDictionary(p => p.Name, p => p.Value);

    [Fact]
    public void Default_query_projects_top_level_segments()
    {
        var query = CosmosQueryBuilder.Read(Dataset(), Plan.Project(["n", "address.city"]), Spec(), CosmosRedactor.None);
        Assert.Equal("SELECT c[\"n\"], c[\"address\"], c[\"id\"] FROM c", query.QueryText);
    }

    [Fact]
    public void User_query_is_wrapped_untouched()
    {
        var query = CosmosQueryBuilder.Read(Dataset("SELECT c.id, c.n FROM c WHERE c.n > 1"), Plan, Spec(), CosmosRedactor.None);
        Assert.Equal("SELECT * FROM (SELECT c.id, c.n FROM c WHERE c.n > 1) c", query.QueryText);
    }

    [Fact]
    public void Bounds_are_parameters_typed_from_the_cursor()
    {
        var query = CosmosQueryBuilder.Read(Dataset("SELECT * FROM c"), Plan, Spec("n", "3", "7"), CosmosRedactor.None);
        Assert.Equal("SELECT * FROM (SELECT * FROM c) c WHERE c[\"n\"] > @pz_lower AND c[\"n\"] <= @pz_upper", query.QueryText);
        var parameters = Parameters(query);
        Assert.Equal(3L, parameters["@pz_lower"]);
        Assert.Equal(7L, parameters["@pz_upper"]);
    }

    [Fact]
    public void Inclusive_lower_bound_uses_gte_and_a_lone_bound_emits_one_clause()
    {
        var query = CosmosQueryBuilder.Read(Dataset(), Plan, Spec("n", "3", null, inclusive: true), CosmosRedactor.None);
        Assert.EndsWith("FROM c WHERE c[\"n\"] >= @pz_lower", query.QueryText);
        Assert.Single(query.GetQueryParameters());
    }

    [Fact]
    public void Ts_cursor_bounds_are_epoch_seconds_and_other_timestamps_are_iso_strings()
    {
        var ts = Parameters(CosmosQueryBuilder.Read(Dataset(), Plan, Spec("_ts", "2026-01-02T03:04:05.000000"), CosmosRedactor.None));
        Assert.Equal(1767323045L, ts["@pz_lower"]);
        var when = Parameters(CosmosQueryBuilder.Read(Dataset(), Plan, Spec("when", "2026-01-02T03:04:05.123456"), CosmosRedactor.None));
        Assert.Equal("2026-01-02T03:04:05.123456Z", when["@pz_lower"]);
        var day = Parameters(CosmosQueryBuilder.Read(Dataset(), Plan, Spec("day", "2026-01-02"), CosmosRedactor.None));
        Assert.Equal("2026-01-02", day["@pz_lower"]);
    }

    [Fact]
    public void Cursor_set_without_a_value_adds_no_clause()
    {
        var query = CosmosQueryBuilder.Read(Dataset(), Plan, Spec("n"), CosmosRedactor.None);
        Assert.DoesNotContain("WHERE", query.QueryText);
    }

    [Fact]
    public void Unsupported_cursor_kinds_and_unknown_cursors_are_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() => CosmosQueryBuilder.Read(Dataset(), Plan, Spec("id", "3"), CosmosRedactor.None));
        Assert.Contains("watermark cursor 'id' is a string column", ex.Message);
        var missing = Assert.Throws<PzConnectorException>(() => CosmosQueryBuilder.Read(Dataset(), Plan, Spec("nope", "3"), CosmosRedactor.None));
        Assert.Contains("watermark cursor 'nope' is not a column", missing.Message);
    }

    [Fact]
    public void Bad_bound_text_is_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() => CosmosQueryBuilder.Read(Dataset(), Plan, Spec("n", "three"), CosmosRedactor.None));
        Assert.Contains("watermark bound 'three' is not a int64 value", ex.Message);
    }

    [Fact]
    public void Sample_orders_by_ts_and_tops()
    {
        Assert.Equal("SELECT TOP 50 * FROM (SELECT * FROM c) c ORDER BY c[\"_ts\"]", CosmosQueryBuilder.Sample(Dataset(), 50).QueryText);
        Assert.Equal("SELECT TOP 5 * FROM (SELECT c.id FROM c) c ORDER BY c[\"_ts\"]", CosmosQueryBuilder.Sample(Dataset("SELECT c.id FROM c"), 5).QueryText);
    }

    [Fact]
    public void Quote_escapes()
    {
        Assert.Equal("c[\"a\\\"b\\\\\"]", CosmosQueryBuilder.Quote("a\"b\\"));
    }
}
