using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class SchemaInferenceTests
{
    private static ColumnPlan Infer(params string[] documents)
    {
        var inference = new SchemaInference("ds", CosmosRedactor.None);
        foreach (var json in documents)
        {
            using var doc = JsonDocument.Parse(json);
            inference.Observe(doc.RootElement);
        }

        return inference.Plan();
    }

    [Fact]
    public void Scalars_flatten_nested_objects_and_id_trails()
    {
        var plan = Infer("""{"id":"1","n":1,"name":"a","address":{"city":"Paris","geo":{"lat":1.5}},"_ts":1700000000,"_rid":"x","_etag":"e","_self":"s","_attachments":"a"}""");
        Assert.Equal(["n", "name", "address.city", "address.geo.lat", "_ts", "id"], plan.Columns.Select(c => c.Name));
        Assert.Equal(ColumnKind.Int64, plan.Columns[0].Kind);
        Assert.Equal(ColumnKind.String, plan.Columns[1].Kind);
        Assert.Equal(ColumnKind.Double, plan.Columns[3].Kind);
        Assert.Equal(ColumnKind.Int64, plan.Columns[4].Kind);
        Assert.Equal(ColumnKind.String, plan.Columns[5].Kind);
    }

    [Fact]
    public void Numbers_widen_and_mixed_kinds_become_json()
    {
        var plan = Infer("""{"id":"1","a":1,"b":2,"c":"x","d":[1],"e":{}}""", """{"id":"2","a":1.5,"b":3,"c":4,"d":[2],"e":{}}""");
        Assert.Equal(ColumnKind.Double, plan.Columns.Single(c => c.Name == "a").Kind);
        Assert.Equal(ColumnKind.Int64, plan.Columns.Single(c => c.Name == "b").Kind);
        Assert.Equal(ColumnKind.Json, plan.Columns.Single(c => c.Name == "c").Kind);
        Assert.Equal(ColumnKind.Json, plan.Columns.Single(c => c.Name == "d").Kind);
        Assert.Equal(ColumnKind.Json, plan.Columns.Single(c => c.Name == "e").Kind);
    }

    [Fact]
    public void Integers_beyond_2_53_infer_as_double()
    {
        var plan = Infer("""{"id":"1","big":9007199254740993}""");
        Assert.Equal(ColumnKind.Double, plan.Columns[0].Kind);
    }

    [Fact]
    public void Nulls_and_missing_do_not_count()
    {
        var plan = Infer("""{"id":"1","a":null}""", """{"id":"2","a":true}""", """{"id":"3"}""");
        Assert.Equal(ColumnKind.Boolean, plan.Columns.Single(c => c.Name == "a").Kind);
    }

    [Fact]
    public void A_field_named_with_a_dot_is_refused_naming_the_document()
    {
        var ex = Assert.Throws<PzConnectorException>(() => Infer("""{"id":"doc-7","a.b":1}"""));
        Assert.Contains("field 'a.b' of document doc-7", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Too_many_paths_are_refused()
    {
        var wide = "{\"id\":\"1\"," + string.Join(",", Enumerable.Range(0, ColumnPlan.MaxColumns + 1).Select(i => $"\"k{i}\":1")) + "}";
        var ex = Assert.Throws<PzConnectorException>(() => Infer(wide));
        Assert.Contains($"more than {ColumnPlan.MaxColumns}", ex.Message);
    }
}
