using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class OutputConfigTests
{
    private static OutputSpec Spec(string mode, Dictionary<string, object?>? options = null, params string[] keys) =>
        new OutputSpec("cosmos", "orders", mode, "fail_on_change", options ?? new Dictionary<string, object?>()) { Keys = keys };

    private static Schema Schema(params (string Name, IArrowType Type)[] fields) =>
        new(fields.Select(f => new Field(f.Name, f.Type, true)).ToList(), null);

    private static List<string> Validate(OutputSpec spec, Schema schema, params string[] pkPaths)
    {
        var errors = new List<string>();
        var output = CosmosOutputConfig.Parse(spec, errors);
        if (output is not null)
        {
            CosmosOutputConfig.ValidateSchema(spec, output, schema, pkPaths.Length == 0 ? ["/id"] : pkPaths, errors);
        }

        return errors;
    }

    [Fact]
    public void Defaults_and_integral_doubles()
    {
        var errors = new List<string>();
        var output = CosmosOutputConfig.Parse(Spec("append", new() { ["concurrency"] = 8.0, ["rate_limit_retries"] = 2.0, ["id_from"] = new List<object?> { "a", "b" } }), errors);
        Assert.Empty(errors);
        Assert.Equal("orders", output!.Container);
        Assert.Equal(8, output.Concurrency);
        Assert.Equal(2, output.RateLimitRetries);
        Assert.Equal(["a", "b"], output.IdFrom);
        var defaults = CosmosOutputConfig.Parse(Spec("append"), errors)!;
        Assert.Equal(CosmosOutputConfig.DefaultConcurrency, defaults.Concurrency);
        Assert.Equal(CosmosOutputConfig.DefaultRateLimitRetries, defaults.RateLimitRetries);
    }

    [Fact]
    public void Replace_is_refused_with_PZCS0301()
    {
        var errors = new List<string>();
        Assert.Null(CosmosOutputConfig.Parse(Spec("replace"), errors));
        var error = Assert.Single(errors);
        Assert.Contains("PZCS0301", error);
        Assert.Contains("append", error);
        Assert.Contains("merge", error);
    }

    [Fact]
    public void Append_with_int64_id_and_matching_partition_key_is_fine()
    {
        Assert.Empty(Validate(Spec("append"), Schema(("id", Int64Type.Default), ("name", StringType.Default))));
    }

    [Fact]
    public void Partition_key_paths_must_be_columns()
    {
        var errors = Validate(Spec("append"), Schema(("id", StringType.Default)), "/region", "/tenant/code");
        Assert.Contains(errors, e => e.Contains("PZCS0303", StringComparison.Ordinal) && e.Contains("'/region'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'/tenant/code'", StringComparison.Ordinal) && e.Contains("tenant.code", StringComparison.Ordinal));
        Assert.Empty(Validate(Spec("append"), Schema(("id", StringType.Default), ("tenant.code", StringType.Default)), "/tenant/code"));
    }

    [Fact]
    public void Merge_keys_must_be_the_identity()
    {
        var schema = Schema(("id", StringType.Default), ("region", StringType.Default), ("n", Int64Type.Default));
        Assert.Empty(Validate(Spec("merge", null, "id"), schema, "/region"));
        Assert.Empty(Validate(Spec("merge", null, "id", "region"), schema, "/region"));
        Assert.Contains(Validate(Spec("merge", null, "n"), schema, "/region"), e => e.Contains("PZCS0304", StringComparison.Ordinal));

        var fromKeys = Schema(("a", StringType.Default), ("b", Int64Type.Default), ("region", StringType.Default));
        var options = new Dictionary<string, object?> { ["id_from"] = new List<object?> { "a", "b" } };
        Assert.Empty(Validate(Spec("merge", options, "b", "a"), fromKeys, "/region"));
        Assert.Contains(Validate(Spec("merge", options, "a"), fromKeys, "/region"), e => e.Contains("PZCS0304", StringComparison.Ordinal));
        Assert.Contains(Validate(Spec("merge", null, "region"), Schema(("region", StringType.Default)), "/region"), e => e.Contains("needs an 'id' column or 'id_from'", StringComparison.Ordinal));
    }

    [Fact]
    public void Bad_columns_are_reported()
    {
        var errors = Validate(Spec("append"), Schema(("id", DoubleType.Default), ("_rid", StringType.Default), ("_ts", Int64Type.Default), ("blob", BinaryType.Default), ("a", StringType.Default), ("a.b", StringType.Default)));
        Assert.Contains(errors, e => e.Contains("'id' column is Double; only a varchar, int32 or int64 column can be the document id", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("column '_rid' is a Cosmos DB system property", StringComparison.Ordinal));
        Assert.DoesNotContain(errors, e => e.Contains("'_ts'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("column 'blob' is Binary", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'a' is both a value and the parent of 'a.b'", StringComparison.Ordinal));
    }

    [Fact]
    public void Id_from_must_name_text_or_integer_columns()
    {
        var errors = Validate(Spec("append", new() { ["id_from"] = new List<object?> { "x", "d" } }), Schema(("d", DoubleType.Default), ("id", StringType.Default)));
        Assert.Contains(errors, e => e.Contains("id_from entry 'x' is not a column", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("id_from entry 'd' is a Double column", StringComparison.Ordinal));
    }
}
