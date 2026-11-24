using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class RowDocumentWriterTests
{
    private static readonly CosmosOutputConfig Output = new("orders", null, 32, 9);

    private static RecordBatch Batch(params (string Name, IArrowArray Array)[] columns)
    {
        var schema = new Schema(columns.Select(c => new Field(c.Name, c.Array.Data.DataType, true)).ToList(), null);
        return new RecordBatch(schema, columns.Select(c => c.Array).ToList(), columns[0].Array.Length);
    }

    private static RowDocument Write(RecordBatch batch, int row = 0, CosmosOutputConfig? output = null, params string[] pkPaths) =>
        new RowDocumentWriter(batch.Schema, output ?? Output, pkPaths.Length == 0 ? ["/id"] : pkPaths, "orders", CosmosRedactor.None, () => "generated-guid").Write(batch, row, row + 1);

    private static JsonElement Parse(RowDocument doc) => JsonDocument.Parse(doc.Json).RootElement.Clone();

    [Fact]
    public void Every_type_is_spelled_and_nested_paths_nest()
    {
        var ts = new TimestampArray.Builder(TimeUnit.Microsecond, "UTC").Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(1234560)).Build();
        var batch = Batch(
            ("id", new StringArray.Builder().Append("k1").Build()),
            ("i32", new Int32Array.Builder().Append(7).Build()),
            ("i64", new Int64Array.Builder().Append(9007199254740992).Build()),
            ("d", new DoubleArray.Builder().Append(1.5).Build()),
            ("m", new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(12345678901234567890.123456789m).Build()),
            ("b", new BooleanArray.Builder().Append(true).Build()),
            ("day", new Date32Array.Builder().Append(new DateOnly(2026, 1, 2)).Build()),
            ("ts", ts),
            ("address.city", new StringArray.Builder().Append("Paris").Build()),
            ("_ts", new Int64Array.Builder().Append(1).Build()));
        var doc = Parse(Write(batch));
        Assert.Equal("k1", doc.GetProperty("id").GetString());
        Assert.Equal(7, doc.GetProperty("i32").GetInt32());
        Assert.Equal(9007199254740992, doc.GetProperty("i64").GetInt64());
        Assert.Equal(1.5, doc.GetProperty("d").GetDouble());
        Assert.Equal("12345678901234567890.123456789", doc.GetProperty("m").GetString());
        Assert.True(doc.GetProperty("b").GetBoolean());
        Assert.Equal("2026-01-02", doc.GetProperty("day").GetString());
        Assert.Equal("2026-01-02T03:04:05.123456Z", doc.GetProperty("ts").GetString());
        Assert.Equal("Paris", doc.GetProperty("address").GetProperty("city").GetString());
        Assert.False(doc.TryGetProperty("_ts", out _));
    }

    [Fact]
    public void Id_resolves_from_column_then_id_from_then_generator()
    {
        var fromInt = Write(Batch(("id", new Int64Array.Builder().Append(42).Build())));
        Assert.Equal("42", fromInt.Id);
        Assert.Equal(new PartitionKey("42"), fromInt.PartitionKey);

        var keyed = new CosmosOutputConfig("orders", ["a", "b"], 32, 9);
        var fromKeys = Write(Batch(("a", new StringArray.Builder().Append("x").Build()), ("b", new Int32Array.Builder().Append(3).Build()), ("pk", new StringArray.Builder().Append("p").Build())), 0, keyed, "/pk");
        Assert.Equal("x|3", fromKeys.Id);
        Assert.Equal("x|3", Parse(fromKeys).GetProperty("id").GetString());

        var generated = Write(Batch(("pk", new StringArray.Builder().Append("p").Build())), 0, Output, "/pk");
        Assert.Equal("generated-guid", generated.Id);
        Assert.Equal(new PartitionKey("p"), generated.PartitionKey);
    }

    [Fact]
    public void Partition_keys_are_typed_and_hierarchical()
    {
        var batch = Batch(("id", new StringArray.Builder().Append("k").Build()), ("tenant", new StringArray.Builder().Append("t1").Build()), ("n", new Int64Array.Builder().Append(5).Build()));
        var doc = Write(batch, 0, Output, "/tenant", "/n");
        Assert.Equal(new PartitionKeyBuilder().Add("t1").Add(5.0).Build(), doc.PartitionKey);
    }

    [Fact]
    public void Nulls_are_written_explicitly_except_identity_and_partition_key()
    {
        var doc = Parse(Write(Batch(("id", new StringArray.Builder().Append("k").Build()), ("v", new Int64Array.Builder().AppendNull().Build()))));
        Assert.Equal(JsonValueKind.Null, doc.GetProperty("v").ValueKind);

        var nullId = Batch(("id", new StringArray.Builder().AppendNull().Build()));
        var ex = Assert.Throws<PzConnectorException>(() => Write(nullId));
        Assert.Contains("'id' is null in row 1", ex.Message);

        var nullPk = Batch(("id", new StringArray.Builder().Append("k").Build()), ("pk", new StringArray.Builder().AppendNull().Build()));
        Assert.Contains("partition key column 'pk' is null in row 1", Assert.Throws<PzConnectorException>(() => Write(nullPk, 0, Output, "/pk")).Message);
    }

    [Theory]
    [InlineData("bad/id")]
    [InlineData("bad\\id")]
    [InlineData("bad?id")]
    [InlineData("bad#id")]
    [InlineData("")]
    public void Invalid_ids_are_refused(string id)
    {
        var ex = Assert.Throws<PzConnectorException>(() => Write(Batch(("id", new StringArray.Builder().Append(id).Build()))));
        Assert.Contains("is not a valid Cosmos DB id", ex.Message);
    }

    [Fact]
    public void Id_from_values_with_separator_or_reserved_characters_fail_the_row()
    {
        var idFrom = new CosmosOutputConfig("orders", ["a"], 32, 9);

        var pipe = Batch(("a", new StringArray.Builder().Append("x|y").Build()));
        Assert.Contains("column 'a'", Assert.Throws<PzConnectorException>(() => Write(pipe, 0, idFrom)).Message);

        var slash = Batch(("a", new StringArray.Builder().Append("x/y").Build()));
        Assert.Contains("column 'a'", Assert.Throws<PzConnectorException>(() => Write(slash, 0, idFrom)).Message);
    }

    [Fact]
    public void Composed_id_over_255_characters_is_refused()
    {
        var idFrom = new CosmosOutputConfig("orders", ["a"], 32, 9);
        var batch = Batch(("a", new StringArray.Builder().Append(new string('x', 256)).Build()));
        var ex = Assert.Throws<PzConnectorException>(() => Write(batch, 0, idFrom));
        Assert.Contains("is not a valid Cosmos DB id", ex.Message);
    }

    [Fact]
    public void Integers_beyond_2_53_and_non_finite_doubles_are_refused()
    {
        var big = Batch(("id", new StringArray.Builder().Append("k").Build()), ("v", new Int64Array.Builder().Append(9007199254740993).Build()));
        Assert.Contains("PZCS0305", Assert.Throws<PzConnectorException>(() => Write(big)).Message);
        var nan = Batch(("id", new StringArray.Builder().Append("k").Build()), ("v", new DoubleArray.Builder().Append(double.NaN).Build()));
        Assert.Contains("NaN", Assert.Throws<PzConnectorException>(() => Write(nan)).Message);
    }

    /// <summary>long.MinValue has no positive counterpart: the magnitude check must be a
    /// comparison, not Math.Abs, or the write dies of an OverflowException instead of PZCS0305.</summary>
    [Fact]
    public void Long_min_value_is_refused_as_PZCS0305()
    {
        var batch = Batch(("id", new StringArray.Builder().Append("k").Build()), ("v", new Int64Array.Builder().Append(long.MinValue).Build()));
        var ex = Assert.Throws<PzConnectorException>(() => Write(batch));
        Assert.Contains("PZCS0305", ex.Message);
        Assert.Contains(long.MinValue.ToString(System.Globalization.CultureInfo.InvariantCulture), ex.Message);
    }

    [Fact]
    public void Oversized_documents_are_refused_naming_the_id()
    {
        var huge = Batch(("id", new StringArray.Builder().Append("big-one").Build()), ("v", new StringArray.Builder().Append(new string('x', RowDocumentWriter.MaxDocumentBytes + 1)).Build()));
        var ex = Assert.Throws<PzConnectorException>(() => Write(huge));
        Assert.Contains("PZCS0306", ex.Message);
        Assert.Contains("big-one", ex.Message);
    }

    private static RecordBatch NestedBatch()
    {
        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        var recType = new StructType([new Field("n", Int64Type.Default, true), new Field("tags", new ListType(new Field("item", StringType.Default, true)), true)]);
        var rec = new StructArray(recType, 1, [new Int64Array.Builder().Append(7).Build(), tags.Build()], new ArrowBuffer.BitmapBuilder().Append(true).Build(), 0);
        var map = new MapArray.Builder(new MapType(StringType.Default, new Decimal128Type(38, 9)));
        map.Append();
        ((StringArray.Builder)map.KeyBuilder).Append("k1");
        ((Decimal128Array.Builder)map.ValueBuilder).Append(1.5m);
        var pair = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
        var pairArray = new FixedSizeListArray(new ArrayData(pair, 1, 0, 0, [ArrowBuffer.Empty], [new Int32Array.Builder().Append(1).Append(2).Build().Data]));
        var ts = new TimestampArray.Builder(TimeUnit.Microsecond, "UTC").Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)).Build();
        var tsList = new ListArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        tsList.Append();
        ((TimestampArray.Builder)tsList.ValueBuilder).Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero));
        return Batch(("id", new StringArray.Builder().Append("k1").Build()), ("rec", rec), ("m", map.Build()), ("pair", pairArray), ("when", tsList.Build()));
    }

    [Fact]
    public void Nested_columns_are_json_objects_and_arrays_with_flat_leaf_spelling()
    {
        using var batch = NestedBatch();
        var doc = Parse(Write(batch));
        Assert.Equal("""{"n":7,"tags":["a",null]}""", doc.GetProperty("rec").GetRawText());
        Assert.Equal("""{"k1":"1.500000000"}""", doc.GetProperty("m").GetRawText());
        Assert.Equal("[1,2]", doc.GetProperty("pair").GetRawText());
        Assert.Equal("""["2026-01-02T03:04:05.000000Z"]""", doc.GetProperty("when").GetRawText());
    }

    [Fact]
    public void A_nested_int64_beyond_2_pow_53_is_refused()
    {
        var list = new ListArray.Builder(Int64Type.Default);
        list.Append();
        ((Int64Array.Builder)list.ValueBuilder).Append(9007199254740993);
        using var batch = Batch(("id", new StringArray.Builder().Append("k1").Build()), ("xs", list.Build()));
        var ex = Assert.Throws<PzConnectorException>(() => Write(batch));
        Assert.Contains("PZCS0305", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_nested_column_is_accepted_but_a_nested_partition_key_or_unsupported_leaf_is_not()
    {
        var list = new ListType(new Field("item", StringType.Default, true));
        var schema = new Schema([new Field("id", StringType.Default, true), new Field("xs", list, true)], null);
        var errors = new List<string>();
        CosmosOutputConfig.ValidateSchema(new OutputSpec("cosmos", "o", "append", "fail_on_change", new Dictionary<string, object?>()), Output, schema, ["/id"], errors);
        Assert.Empty(errors);

        errors.Clear();
        CosmosOutputConfig.ValidateSchema(new OutputSpec("cosmos", "o", "append", "fail_on_change", new Dictionary<string, object?>()), Output, schema, ["/xs"], errors);
        Assert.Contains(errors, e => e.Contains("partition key column 'xs'", StringComparison.Ordinal));

        var floats = new Schema([new Field("id", StringType.Default, true), new Field("fs", new ListType(new Field("item", FloatType.Default, true)), true)], null);
        errors.Clear();
        CosmosOutputConfig.ValidateSchema(new OutputSpec("cosmos", "o", "append", "fail_on_change", new Dictionary<string, object?>()), Output, floats, ["/id"], errors);
        Assert.Contains(errors, e => e.Contains("column 'fs' is Float", StringComparison.Ordinal));
    }

    [Fact]
    public void The_connector_declares_nested_types()
    {
        Assert.True(new CosmosConnector().Capabilities.HasFlag(ConnectorCapabilities.NestedTypes));
    }

    [Fact]
    public void Nested_id_id_from_and_merge_key_columns_are_refused()
    {
        var st = new StructType([new Field("a", StringType.Default, true)]);
        var schema = new Schema([new Field("id", st, true), new Field("k", new ListType(new Field("item", StringType.Default, true)), true)], null);
        var spec = new OutputSpec("cosmos", "o", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id", "k"] };
        var errors = new List<string>();
        CosmosOutputConfig.ValidateSchema(spec, new CosmosOutputConfig("o", ["k"], 32, 9), schema, ["/id"], errors);
        Assert.Contains(errors, e => e.Contains("merge key column 'k'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("merge key column 'id'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("the 'id' column is Struct", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("partition key column 'id'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("id_from entry 'k'", StringComparison.Ordinal));
    }
}
