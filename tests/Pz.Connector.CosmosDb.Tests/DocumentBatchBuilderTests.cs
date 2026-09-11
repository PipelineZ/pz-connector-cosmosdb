using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class DocumentBatchBuilderTests
{
    private static object? Convert(ColumnKind kind, string json, string path = "v")
    {
        var plan = new ColumnPlan([ColumnSpec.Of(path, kind), ColumnSpec.Of("id", ColumnKind.String)]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "ds", CosmosRedactor.None);
        using var doc = JsonDocument.Parse(json);
        return builder.Convert(plan.Columns[0], doc.RootElement);
    }

    [Fact]
    public void Scalars_convert()
    {
        Assert.Equal("x", Convert(ColumnKind.String, """{"id":"1","v":"x"}"""));
        Assert.Equal(7L, Convert(ColumnKind.Int64, """{"id":"1","v":7}"""));
        Assert.Equal(7, Convert(ColumnKind.Int32, """{"id":"1","v":7}"""));
        Assert.Equal(7.5, Convert(ColumnKind.Double, """{"id":"1","v":7.5}"""));
        Assert.Equal(7.0, Convert(ColumnKind.Double, """{"id":"1","v":7}"""));
        Assert.Equal(true, Convert(ColumnKind.Boolean, """{"id":"1","v":true}"""));
        Assert.Equal(1.25m, Convert(ColumnKind.Decimal, """{"id":"1","v":1.25}"""));
        Assert.Equal(12345678901234567890.123456789m, Convert(ColumnKind.Decimal, """{"id":"1","v":"12345678901234567890.123456789"}"""));
        Assert.Equal("Paris", Convert(ColumnKind.String, """{"id":"1","a":{"b":"Paris"}}""", "a.b"));
    }

    [Fact]
    public void Timestamps_accept_iso_strings_and_epoch_seconds()
    {
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, 123, TimeSpan.Zero).AddTicks(4560), Convert(ColumnKind.Timestamp, """{"id":"1","v":"2026-01-02T03:04:05.123456Z"}"""));
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), Convert(ColumnKind.Timestamp, """{"id":"1","v":"2026-01-02T04:04:05+01:00"}"""));
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1767323045), Convert(ColumnKind.Timestamp, """{"id":"1","v":1767323045}"""));
        Assert.Equal(new DateOnly(2026, 1, 2), Convert(ColumnKind.Date, """{"id":"1","v":"2026-01-02"}"""));
    }

    [Fact]
    public void Json_takes_anything_raw_and_null_or_missing_is_null()
    {
        Assert.Equal("""{"a":[1,2]}""", Convert(ColumnKind.Json, """{"id":"1","v":{"a":[1,2]}}"""));
        Assert.Equal("7", Convert(ColumnKind.Json, """{"id":"1","v":7}"""));
        Assert.Null(Convert(ColumnKind.Int64, """{"id":"1","v":null}"""));
        Assert.Null(Convert(ColumnKind.Int64, """{"id":"1"}"""));
        Assert.Null(Convert(ColumnKind.String, """{"id":"1","a":"not an object"}""", "a.b"));
    }

    // ColumnKind is `internal`; InternalsVisibleTo makes it usable inside this assembly's method
    // bodies, but a `public` Theory method still cannot declare it as a parameter type (CS0051), so
    // InlineData carries the int and the body casts back -- same pattern as ColumnPlanTests.
    [Theory]
    [InlineData((int)ColumnKind.Int64, """{"id":"d9","v":1.5}""", "holds 1.5 where an integer is planned")]
    [InlineData((int)ColumnKind.Int64, """{"id":"d9","v":"7"}""", "holds \"7\" where an integer is planned")]
    [InlineData((int)ColumnKind.Int32, """{"id":"d9","v":3000000000}""", "outside the planned 32-bit range")]
    [InlineData((int)ColumnKind.Boolean, """{"id":"d9","v":1}""", "holds 1 where a boolean is planned")]
    [InlineData((int)ColumnKind.Decimal, """{"id":"d9","v":"1.0000000001"}""", "more than 9 fraction digits")]
    [InlineData((int)ColumnKind.Decimal, """{"id":"d9","v":"abc"}""", "where a decimal is planned")]
    [InlineData((int)ColumnKind.Timestamp, """{"id":"d9","v":"yesterday"}""", "where a timestamp is planned")]
    [InlineData((int)ColumnKind.Date, """{"id":"d9","v":"2026-1-2"}""", "where a date is planned")]
    [InlineData((int)ColumnKind.String, """{"id":"d9","v":5}""", "holds 5 where a string is planned")]
    public void Lossy_values_are_refused_naming_field_and_document(int kind, string json, string reason)
    {
        var ex = Assert.Throws<PzConnectorException>(() => Convert((ColumnKind)kind, json));
        Assert.Contains("field 'v' of document d9", ex.Message);
        Assert.Contains(reason, ex.Message);
        Assert.False(ex.IsTransient);
    }

    /// <summary>long.MinValue has no positive counterpart: the timestamp column's exact-integer
    /// range check must be a comparison, not Math.Abs, or the read dies of an OverflowException
    /// instead of the refusal every other out-of-range value gets. An int64 column takes it, the
    /// way it takes any other integer the wire spells.</summary>
    [Fact]
    public void Long_min_value_refuses_as_a_timestamp_and_reads_as_an_int64()
    {
        var json = $$"""{"id":"d9","v":{{long.MinValue}}}""";
        var ex = Assert.Throws<PzConnectorException>(() => Convert(ColumnKind.Timestamp, json));
        Assert.Contains("field 'v' of document d9", ex.Message);
        Assert.Contains("where a timestamp is planned", ex.Message);
        Assert.Equal(long.MinValue, Convert(ColumnKind.Int64, json));
    }

    [Fact]
    public void Batches_are_cut_by_row_ceiling_and_flushed()
    {
        var plan = new ColumnPlan([ColumnSpec.Of("n", ColumnKind.Int64), ColumnSpec.Of("id", ColumnKind.String)]);
        var builder = new DocumentBatchBuilder(plan, new BatchOptions(MaxRowsPerBatch: 3), "ds", CosmosRedactor.None);
        var taken = new List<RecordBatch>();
        for (var i = 0; i < 7; i++)
        {
            using var doc = JsonDocument.Parse($$"""{"id":"{{i}}","n":{{i}}}""");
            builder.Append(doc.RootElement);
            if (builder.TryTakeBatch(out var batch))
            {
                taken.Add(batch!);
            }
        }

        var tail = builder.Flush();
        Assert.Equal([3, 3], taken.Select(b => b.Length));
        Assert.Equal(1, tail!.Length);
        Assert.Equal(6L, ((Int64Array)tail.Column(0)).GetValue(0));
        foreach (var b in taken.Append(tail)) b.Dispose();
    }
}
