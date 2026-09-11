using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.CosmosDb;

/// <summary>How a column's value is read out of a document and spelled in Arrow. <see cref="Json"/>
/// takes any JSON value as its raw text.</summary>
internal enum ColumnKind { String, Int32, Int64, Double, Decimal, Boolean, Timestamp, Date, Json }

/// <summary>One column: its name (the dotted JSON path), the path inside the document, and its kind.</summary>
internal sealed record ColumnSpec(string Name, string[] Path, ColumnKind Kind)
{
    public static ColumnSpec Of(string path, ColumnKind kind) => new(path, path.Split('.'), kind);
}

/// <summary>The dataset's Arrow schema and, per column, how to fill it. <c>id</c> is the one
/// property Cosmos DB guarantees on every document, hence the one non-nullable field.</summary>
internal sealed class ColumnPlan
{
    public const int DecimalPrecision = 38;
    public const int DecimalScale = 9;

    /// <summary>More columns than this is not a table but a map with dynamic keys; such a field
    /// belongs under <c>fields:</c> as <c>json</c>.</summary>
    public const int MaxColumns = 2000;

    public const string KindNames = "string, int32, int64, double, decimal, bool, timestamp, date, json";

    /// <summary>Service-owned properties never inferred and never projected by default. <c>_ts</c>
    /// is deliberately absent: it is the natural "changed since" cursor.</summary>
    public static readonly string[] SystemProperties = ["_rid", "_self", "_etag", "_attachments"];

    public ColumnPlan(IReadOnlyList<ColumnSpec> columns)
    {
        Columns = columns;
        Schema = new Schema(columns.Select(c => new Field(c.Name, ArrowType(c.Kind), c.Name != "id")).ToList(), null);
    }

    public IReadOnlyList<ColumnSpec> Columns { get; }

    public Schema Schema { get; }

    /// <summary>The top-level properties a query must return for every column to be resolvable:
    /// the distinct first path segments, plus <c>id</c> even when no column wants it, so a refusal
    /// can still name the document.</summary>
    public IReadOnlyList<string> TopLevelProjection()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (var column in Columns)
        {
            if (seen.Add(column.Path[0]))
            {
                result.Add(column.Path[0]);
            }
        }

        if (seen.Add("id"))
        {
            result.Add("id");
        }

        return result;
    }

    /// <summary>Narrows to the named columns, in the hint's order -- the engine narrows its staging
    /// table on the declared capability and refuses a batch shaped otherwise. A name this plan does
    /// not have means the hint is unusable, and the full plan is returned -- the engine then drops
    /// the hint the same way and the pipeline's SQL reports the unknown column.</summary>
    public ColumnPlan Project(IReadOnlyList<string>? columns)
    {
        if (columns is not { Count: > 0 })
        {
            return this;
        }

        var kept = new List<ColumnSpec>(columns.Count);
        foreach (var name in columns)
        {
            var column = Columns.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            if (column is null)
            {
                return this;
            }

            kept.Add(column);
        }

        return new ColumnPlan(kept);
    }

    public static IArrowType ArrowType(ColumnKind kind) => kind switch
    {
        ColumnKind.Int32 => Int32Type.Default,
        ColumnKind.Int64 => Int64Type.Default,
        ColumnKind.Double => DoubleType.Default,
        ColumnKind.Decimal => new Decimal128Type(DecimalPrecision, DecimalScale),
        ColumnKind.Boolean => BooleanType.Default,
        ColumnKind.Timestamp => new TimestampType(TimeUnit.Microsecond, "UTC"),
        ColumnKind.Date => Date32Type.Default,
        _ => StringType.Default,
    };

    /// <summary>The <c>fields:</c> spellings, with the SQL-flavoured aliases a pz author reaches for.</summary>
    public static bool TryParseKind(string text, out ColumnKind kind)
    {
        switch (text.Trim().ToLowerInvariant())
        {
            case "string" or "varchar" or "text": kind = ColumnKind.String; return true;
            case "int32" or "int" or "integer": kind = ColumnKind.Int32; return true;
            case "int64" or "bigint" or "long": kind = ColumnKind.Int64; return true;
            case "double" or "float" or "float8": kind = ColumnKind.Double; return true;
            case "decimal" or "decimal128" or "numeric": kind = ColumnKind.Decimal; return true;
            case "bool" or "boolean": kind = ColumnKind.Boolean; return true;
            case "timestamp" or "datetime": kind = ColumnKind.Timestamp; return true;
            case "date": kind = ColumnKind.Date; return true;
            case "json": kind = ColumnKind.Json; return true;
            default: kind = default; return false;
        }
    }

    /// <summary>Column names are JSON paths and DuckDB identifiers at once: one must not be a
    /// prefix path of another (the sink could not nest them), and two must not differ only by case
    /// (DuckDB would fold them together).</summary>
    public static void ValidateNames(IReadOnlyList<string> names, string prefix, List<string> errors)
    {
        if (names.Count > MaxColumns)
        {
            errors.Add($"{prefix}: {names.Count} columns is more than the {MaxColumns} a dataset may have; a field with dynamic keys should be declared as json under fields:");
            return;
        }

        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var exact = new HashSet<string>(names, StringComparer.Ordinal);
        foreach (var name in names)
        {
            if (seen.TryGetValue(name, out var other) && !string.Equals(other, name, StringComparison.Ordinal))
            {
                errors.Add($"{prefix}: fields '{other}' and '{name}' differ only by case, which SQL cannot tell apart; declare one of them under fields: with another name or drop it");
            }

            seen.TryAdd(name, name);
            for (var dot = name.IndexOf('.'); dot > 0; dot = name.IndexOf('.', dot + 1))
            {
                var parent = name[..dot];
                if (exact.Contains(parent))
                {
                    errors.Add($"{prefix}: field '{parent}' is both a value and the parent of '{name}'");
                    break;
                }
            }
        }
    }
}
