using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.CosmosDb;

/// <summary>Documents in, Arrow batches out, one column per <see cref="ColumnSpec"/>. Values are
/// read off the document by path and converted per the column's kind; a value the kind cannot hold
/// losslessly -- a fraction in an integer column, a string where a number is planned, a decimal with
/// more fraction digits than the column's scale -- fails the read naming the field and the document,
/// because a silently stringified or truncated column is worse than a stopped run. Batches come from
/// the ABI's pooled builder, so every yielded batch is a fresh instance the engine owns outright.</summary>
internal sealed class DocumentBatchBuilder
{
    private const long MaxExactInteger = 9007199254740992; // 2^53: what a Cosmos double holds exactly.
    private static readonly decimal DecimalScaleFactor = 1_000_000_000m;

    private readonly ColumnPlan _plan;
    private readonly ArrowBatchBuilder _inner;
    private readonly string _dataset;
    private readonly CosmosRedactor _redactor;
    private readonly object?[] _row;

    public DocumentBatchBuilder(ColumnPlan plan, BatchOptions options, string dataset, CosmosRedactor redactor)
    {
        _plan = plan;
        _inner = new ArrowBatchBuilder(plan.Schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        _dataset = dataset;
        _redactor = redactor;
        _row = new object?[plan.Columns.Count];
    }

    public int PendingRows => _inner.PendingRows;

    public void Append(JsonElement document)
    {
        var columns = _plan.Columns;
        for (var c = 0; c < columns.Count; c++)
        {
            _row[c] = Convert(columns[c], document);
        }

        _inner.AppendRow(_row);
    }

    public bool TryTakeBatch(out RecordBatch? batch) => _inner.TryTakeBatch(out batch);

    public RecordBatch? Flush() => _inner.Flush();

    internal object? Convert(ColumnSpec column, JsonElement document)
    {
        if (!TryGetPath(document, column.Path, out var value) || value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return null;
        }

        return column.Kind switch
        {
            ColumnKind.Json => value.GetRawText(),
            ColumnKind.String => value.ValueKind == JsonValueKind.String ? value.GetString() : throw Refuse(column, document, $"holds {Describe(value)} where a string is planned"),
            ColumnKind.Int32 => ToInt32(column, value, document),
            ColumnKind.Int64 => ToInt64(column, value, document),
            ColumnKind.Double => value.ValueKind == JsonValueKind.Number ? value.GetDouble() : throw Refuse(column, document, $"holds {Describe(value)} where a double is planned"),
            ColumnKind.Decimal => ToDecimal(column, value, document),
            ColumnKind.Boolean => value.ValueKind is JsonValueKind.True or JsonValueKind.False ? value.GetBoolean() : throw Refuse(column, document, $"holds {Describe(value)} where a boolean is planned"),
            ColumnKind.Timestamp => ToTimestamp(column, value, document),
            ColumnKind.Date => value.ValueKind == JsonValueKind.String && DateOnly.TryParseExact(value.GetString(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day)
                ? day
                : throw Refuse(column, document, $"holds {Describe(value)} where a date is planned (yyyy-MM-dd)"),
            _ => throw new InvalidOperationException($"unexpected column kind {column.Kind}"),
        };
    }

    /// <summary>Walks <paramref name="path"/> into nested objects. A step through anything that is
    /// not an object is a missing value: the plan was made from documents shaped one way and this one
    /// is shaped another, which a nullable column absorbs.</summary>
    internal static bool TryGetPath(JsonElement document, string[] path, out JsonElement value)
    {
        var current = document;
        foreach (var segment in path)
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out var next))
            {
                value = default;
                return false;
            }

            current = next;
        }

        value = current;
        return true;
    }

    private int ToInt32(ColumnSpec column, JsonElement value, JsonElement document)
    {
        var l = ToInt64(column, value, document);
        if (l is < int.MinValue or > int.MaxValue)
        {
            throw Refuse(column, document, $"holds {l}, outside the planned 32-bit range");
        }

        return (int)l;
    }

    private long ToInt64(ColumnSpec column, JsonElement value, JsonElement document)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var l))
        {
            return l;
        }

        throw Refuse(column, document, $"holds {Describe(value)} where an integer is planned");
    }

    /// <summary>A number, or the string spelling the sink writes decimals as; must fit
    /// decimal(38,9) exactly.</summary>
    private decimal ToDecimal(ColumnSpec column, JsonElement value, JsonElement document)
    {
        decimal result;
        if (value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var m))
        {
            result = m;
        }
        else if (value.ValueKind == JsonValueKind.String
                 && decimal.TryParse(value.GetString(), NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed))
        {
            result = parsed;
        }
        else
        {
            throw Refuse(column, document, $"holds {Describe(value)} where a decimal is planned");
        }

        if (result.Scale > ColumnPlan.DecimalScale)
        {
            var truncated = decimal.Truncate(result * DecimalScaleFactor) / DecimalScaleFactor;
            if (truncated != result)
            {
                throw Refuse(column, document, $"holds {Describe(value)}, more than {ColumnPlan.DecimalScale} fraction digits; declare the field as string or double under fields:");
            }

            result = truncated;
        }

        return result;
    }

    /// <summary>An ISO-8601 string (any offset, normalised to UTC) or an integral number of epoch
    /// seconds, which is how <c>_ts</c> arrives.</summary>
    private DateTimeOffset ToTimestamp(ColumnSpec column, JsonElement value, JsonElement document)
    {
        if (value.ValueKind == JsonValueKind.String
            && DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
        {
            return ts;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var seconds) && Math.Abs(seconds) <= MaxExactInteger)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds(seconds);
            }
            catch (ArgumentOutOfRangeException)
            {
                throw Refuse(column, document, $"holds {seconds}, outside the timestamp range");
            }
        }

        throw Refuse(column, document, $"holds {Describe(value)} where a timestamp is planned");
    }

    /// <summary>A value as a reader would write it: strings quoted, everything else as its JSON.</summary>
    internal static string Describe(JsonElement value) => value.ValueKind == JsonValueKind.String ? $"\"{value.GetString()}\"" : value.GetRawText();

    private PzConnectorException Refuse(ColumnSpec column, JsonElement document, string what) =>
        CosmosErrors.Fatal($"dataset '{_dataset}': field '{column.Name}' of document {SchemaInference.IdOf(document)} {what}", _redactor);
}
