using System.Buffers;
using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>What one row becomes: its document id, its partition key, and the document bytes.</summary>
internal sealed record RowDocument(string Id, PartitionKey PartitionKey, ReadOnlyMemory<byte> Json);

/// <summary>One row of an Arrow batch as a JSON document: property order = column order, a dotted
/// column name nested into sub-objects (the inverse of the source's flattening), and every value
/// in its JSON spelling. Hand-written over the v0 type matrix -- the choices are a documented
/// contract: Cosmos DB numbers are IEEE doubles, so an integer beyond ±2^53 is refused rather than
/// silently rounded and a Decimal128 keeps every digit as a string; a timestamp is fixed-width UTC
/// ISO-8601 with microseconds; a date is <c>yyyy-MM-dd</c>. The id comes from the <c>id</c> column,
/// else from <c>id_from</c>, else from the generator; the partition key from the container's own
/// paths. <c>_ts</c> is dropped: the service owns it.</summary>
internal sealed class RowDocumentWriter
{
    public const int MaxDocumentBytes = 2 * 1024 * 1024;
    private const long MaxExactInteger = 9007199254740992;

    private readonly string[][] _paths;
    private readonly bool[] _skip;
    private readonly int _idColumn;
    private readonly int[] _idFromColumns;
    private readonly int[] _partitionKeyColumns;
    private readonly string[] _partitionKeyNames;
    private readonly string _output;
    private readonly CosmosRedactor _redactor;
    private readonly Func<string> _newId;
    private readonly ArrayBufferWriter<byte> _buffer = new(1024);

    public RowDocumentWriter(Schema schema, CosmosOutputConfig output, IReadOnlyList<string> partitionKeyPaths, string outputName, CosmosRedactor redactor, Func<string> newId)
    {
        var names = schema.FieldsList.Select(f => f.Name).ToList();
        _paths = names.Select(n => n.Split('.')).ToArray();
        _skip = names.Select(n => n == "_ts").ToArray();
        _idColumn = names.IndexOf("id");
        _idFromColumns = _idColumn < 0 && output.IdFrom is { } from ? from.Select(n => names.IndexOf(n)).ToArray() : [];
        _partitionKeyNames = partitionKeyPaths.Select(p => p.TrimStart('/').Replace('/', '.')).ToArray();
        _partitionKeyColumns = _partitionKeyNames.Select(n => names.IndexOf(n)).ToArray();
        _output = outputName;
        _redactor = redactor;
        _newId = newId;
    }

    public RowDocument Write(RecordBatch batch, int row, long rowNumber)
    {
        var id = ResolveId(batch, row, rowNumber);
        var partitionKey = ResolvePartitionKey(batch, row, rowNumber);

        _buffer.Clear();
        using (var writer = new Utf8JsonWriter(_buffer))
        {
            writer.WriteStartObject();
            var open = new List<string>();
            if (_idColumn < 0)
            {
                writer.WriteString("id", id);
            }

            for (var c = 0; c < _paths.Length; c++)
            {
                if (_skip[c])
                {
                    continue;
                }

                var path = _paths[c];
                // Close the nested objects this path leaves and open the ones it enters. JSON
                // forbids a repeated key, so the columns of one nested object must be adjacent in
                // the schema -- CosmosOutputConfig.ValidateSchema refuses a schema where they are not.
                var common = 0;
                while (common < open.Count && common < path.Length - 1 && open[common] == path[common])
                {
                    common++;
                }

                for (var i = open.Count; i > common; i--)
                {
                    writer.WriteEndObject();
                    open.RemoveAt(i - 1);
                }

                for (var i = common; i < path.Length - 1; i++)
                {
                    writer.WriteStartObject(path[i]);
                    open.Add(path[i]);
                }

                writer.WritePropertyName(path[^1]);
                if (c == _idColumn)
                {
                    writer.WriteStringValue(id);
                }
                else
                {
                    WriteValue(writer, batch.Column(c), row, _paths[c], rowNumber, id);
                }
            }

            for (var i = open.Count; i > 0; i--)
            {
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        if (_buffer.WrittenCount > MaxDocumentBytes)
        {
            throw CosmosErrors.Fatal($"output '{_output}': document '{id}' (row {rowNumber}) is {_buffer.WrittenCount} bytes, over the 2 MB Cosmos DB limit (PZCS0306)", _redactor);
        }

        return new RowDocument(id, partitionKey, _buffer.WrittenMemory.ToArray());
    }

    private string ResolveId(RecordBatch batch, int row, long rowNumber)
    {
        if (_idColumn >= 0)
        {
            var id = Text(batch.Column(_idColumn), row) ?? throw CosmosErrors.Fatal($"output '{_output}': 'id' is null in row {rowNumber}; every document needs an id", _redactor);
            return ValidateId(id, rowNumber);
        }

        if (_idFromColumns.Length > 0)
        {
            var parts = new string[_idFromColumns.Length];
            for (var i = 0; i < parts.Length; i++)
            {
                var column = string.Join(".", _paths[_idFromColumns[i]]);
                var value = Text(batch.Column(_idFromColumns[i]), row)
                    ?? throw CosmosErrors.Fatal($"output '{_output}': id_from column '{column}' is null in row {rowNumber}", _redactor);
                if (value.IndexOfAny(['|', '/', '\\', '?', '#']) >= 0)
                {
                    throw CosmosErrors.Fatal($"output '{_output}': id_from column '{column}' holds '{value}' (row {rowNumber}), which contains a reserved character; id_from values must not contain | / \\ ? #", _redactor);
                }

                parts[i] = value;
            }

            // Every part is already free of the reserved characters, so joining with '|' cannot
            // create an ambiguous id -- two different key tuples never produce the same string.
            return ValidateId(string.Join("|", parts), rowNumber);
        }

        return _newId();
    }

    /// <summary>The one rule Cosmos DB itself enforces on every id, whatever produced it: 1-255
    /// characters, none of the four that make an id unusable as a URI segment.</summary>
    private string ValidateId(string id, long rowNumber)
    {
        if (id.Length == 0 || id.Length > 255 || id.IndexOfAny(['/', '\\', '?', '#']) >= 0)
        {
            throw CosmosErrors.Fatal($"output '{_output}': '{id}' (row {rowNumber}) is not a valid Cosmos DB id: 1-255 characters, none of / \\ ? #", _redactor);
        }

        return id;
    }

    private PartitionKey ResolvePartitionKey(RecordBatch batch, int row, long rowNumber)
    {
        var builder = new PartitionKeyBuilder();
        for (var i = 0; i < _partitionKeyColumns.Length; i++)
        {
            var column = batch.Column(_partitionKeyColumns[i]);
            if (column.IsNull(row))
            {
                throw CosmosErrors.Fatal($"output '{_output}': partition key column '{_partitionKeyNames[i]}' is null in row {rowNumber}; every document needs its partition key", _redactor);
            }

            if (_partitionKeyColumns[i] == _idColumn)
            {
                // The id column feeds the partition key as the same text the document carries,
                // whatever its underlying Arrow type -- Cosmos DB's own id is always a string.
                builder.Add(ResolveId(batch, row, rowNumber));
                continue;
            }

            switch (column)
            {
                case BooleanArray b: builder.Add(b.GetValue(row)!.Value); break;
                case Int32Array a: builder.Add((double)a.GetValue(row)!.Value); break;
                case Int64Array a: builder.Add((double)a.GetValue(row)!.Value); break;
                case DoubleArray a: builder.Add(a.GetValue(row)!.Value); break;
                default: builder.Add(Text(column, row)!); break;
            }
        }

        return builder.Build();
    }

    private static string? Text(IArrowArray column, int row) => column.IsNull(row) ? null : column switch
    {
        StringArray a => a.GetString(row),
        Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        DoubleArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
        BooleanArray a => a.GetValue(row)!.Value ? "true" : "false",
        Decimal128Array a => a.GetSqlDecimal(row)!.Value.ToString(),
        Date32Array a => a.GetDateOnly(row)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimestampArray a => a.GetTimestamp(row)!.Value.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
        _ => throw new NotSupportedException($"column type {column.Data.DataType.TypeId} is outside pz's type matrix"),
    };

    private void WriteValue(Utf8JsonWriter writer, IArrowArray column, int row, string[] path, long rowNumber, string id)
    {
        if (column.IsNull(row))
        {
            writer.WriteNullValue();
            return;
        }

        switch (column)
        {
            case Int32Array a:
                writer.WriteNumberValue(a.GetValue(row)!.Value);
                break;
            case Int64Array a:
                var l = a.GetValue(row)!.Value;
                // Range-compared, never Math.Abs: long.MinValue has no positive counterpart and
                // would throw an OverflowException instead of the refusal below.
                if (l < -MaxExactInteger || l > MaxExactInteger)
                {
                    throw CosmosErrors.Fatal($"output '{_output}': column '{string.Join(".", path)}' of document '{id}' (row {rowNumber}) holds {l}, beyond ±2^53, which a Cosmos DB number cannot hold exactly (PZCS0305); cast it to varchar", _redactor);
                }

                writer.WriteNumberValue(l);
                break;
            case DoubleArray a:
                var d = a.GetValue(row)!.Value;
                if (!double.IsFinite(d))
                {
                    throw CosmosErrors.Fatal($"output '{_output}': column '{string.Join(".", path)}' of document '{id}' (row {rowNumber}) holds {d} (NaN or infinity), which JSON cannot spell", _redactor);
                }

                writer.WriteNumberValue(d);
                break;
            case BooleanArray a:
                writer.WriteBooleanValue(a.GetValue(row)!.Value);
                break;
            default:
                // Decimal, Date32, Timestamp, String: their text spelling is the contract.
                writer.WriteStringValue(Text(column, row));
                break;
        }
    }
}
