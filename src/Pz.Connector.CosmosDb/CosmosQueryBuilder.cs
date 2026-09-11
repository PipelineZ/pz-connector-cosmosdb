using System.Globalization;
using System.Text;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>The one place Cosmos DB SQL is assembled. A user's <c>query</c> is never edited: it is
/// wrapped as a subquery and the engine's watermark bounds are applied on the outside as typed
/// parameters. With no user query, the projection the engine asks for becomes the SELECT list (the
/// top-level segments, bracket-quoted so a reserved word or an odd name works). Sampling wraps the
/// same way, with <c>ORDER BY c["_ts"]</c> -- one property, because a composite order needs a
/// composite index the default policy lacks.</summary>
internal static class CosmosQueryBuilder
{
    public const string LowerParameter = "@pz_lower";
    public const string UpperParameter = "@pz_upper";

    private static readonly ColumnKind[] CursorKinds =
        [ColumnKind.Int32, ColumnKind.Int64, ColumnKind.Double, ColumnKind.Timestamp, ColumnKind.Date];

    public static QueryDefinition Read(CosmosDatasetConfig dataset, ColumnPlan projected, DatasetSpec spec, CosmosRedactor redactor)
    {
        var sql = new StringBuilder();
        if (dataset.Query is { } user)
        {
            sql.Append("SELECT * FROM (").Append(user.Trim()).Append(") c");
        }
        else
        {
            sql.Append("SELECT ").Append(string.Join(", ", projected.TopLevelProjection().Select(Quote))).Append(" FROM c");
        }

        var query = new QueryDefinition(sql.ToString());
        if (spec.WatermarkCursor is null || (spec.WatermarkValue is null && spec.WatermarkUpperBound is null))
        {
            if (spec.WatermarkCursor is not null)
            {
                // A cursor name with no value yet is still the same misconfiguration when wrong.
                ValidateCursor(projected, spec, redactor);
            }

            return query;
        }

        var column = ValidateCursor(projected, spec, redactor);
        var clauses = new List<string>();
        if (spec.WatermarkValue is { } lower)
        {
            clauses.Add($"{Quote(column.Name)} {(spec.WatermarkLowerInclusive ? ">=" : ">")} {LowerParameter}");
            query = query.WithParameter(LowerParameter, TypedBound(column, lower, spec, redactor));
        }

        if (spec.WatermarkUpperBound is { } upper)
        {
            clauses.Add($"{Quote(column.Name)} <= {UpperParameter}");
            query = query.WithParameter(UpperParameter, TypedBound(column, upper, spec, redactor));
        }

        return new QueryDefinition(sql.Append(" WHERE ").Append(string.Join(" AND ", clauses)).ToString())
            .CopyParametersFrom(query);
    }

    public static QueryDefinition Sample(CosmosDatasetConfig dataset, int sampleSize)
    {
        var inner = dataset.Query?.Trim() ?? "SELECT * FROM c";
        return new QueryDefinition($"SELECT TOP {sampleSize.ToString(CultureInfo.InvariantCulture)} * FROM ({inner}) c ORDER BY {Quote("_ts")}");
    }

    /// <summary>A watermark cursor must be a column a range can bound: numeric, date, or
    /// timestamp. <c>decimal</c> is excluded because its wire form is a string, which the service
    /// would compare ordinally. Nested paths are refused: a bound on <c>c["a"]["b"]</c> would work,
    /// but pz stores watermarks by column name, and a dotted cursor never reaches the engine intact.</summary>
    public static ColumnSpec ValidateCursor(ColumnPlan plan, DatasetSpec spec, CosmosRedactor redactor)
    {
        var cursor = spec.WatermarkCursor ?? throw new InvalidOperationException("no cursor");
        var column = plan.Columns.FirstOrDefault(c => string.Equals(c.Name, cursor, StringComparison.Ordinal))
            ?? throw CosmosErrors.Fatal($"dataset '{spec.Dataset}': watermark cursor '{cursor}' is not a column of the dataset", redactor);
        if (!CursorKinds.Contains(column.Kind))
        {
            throw CosmosErrors.Fatal(
                $"dataset '{spec.Dataset}': watermark cursor '{cursor}' is a {column.Kind.ToString().ToLowerInvariant()} column, which a range cannot bound; use a numeric, date, or timestamp field",
                redactor);
        }

        if (column.Path.Length > 1)
        {
            throw CosmosErrors.Fatal($"dataset '{spec.Dataset}': watermark cursor '{cursor}' is a nested path; cursors must be top-level properties", redactor);
        }

        return column;
    }

    /// <summary>The bound in the cursor's wire kind: numbers as numbers; <c>_ts</c> as epoch
    /// seconds; other timestamps and dates as the fixed-width UTC strings the service compares
    /// ordinally.</summary>
    internal static object TypedBound(ColumnSpec column, string text, DatasetSpec spec, CosmosRedactor redactor)
    {
        switch (column.Kind)
        {
            case ColumnKind.Int32 or ColumnKind.Int64:
                if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
                {
                    return l;
                }

                break;
            case ColumnKind.Double:
                if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                {
                    return d;
                }

                break;
            case ColumnKind.Timestamp:
                if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
                {
                    return column.Name == "_ts" ? ts.ToUnixTimeSeconds() : ts.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
                }

                break;
            case ColumnKind.Date:
                if (DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day))
                {
                    return day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                }

                break;
        }

        throw CosmosErrors.Fatal(
            $"dataset '{spec.Dataset}': watermark bound '{text}' is not a {column.Kind.ToString().ToLowerInvariant()} value for cursor '{column.Name}'", redactor);
    }

    /// <summary>A property reference that survives any name: reserved words, dots, quotes.</summary>
    public static string Quote(string property) =>
        "c[\"" + property.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal) + "\"]";

    /// <summary>The rendered query text, for logging and tests -- callers that only need the text
    /// should not have to hold a <see cref="QueryDefinition"/> alive.</summary>
    internal static string Text(QueryDefinition query) => query.QueryText;

    private static QueryDefinition CopyParametersFrom(this QueryDefinition target, QueryDefinition source)
    {
        foreach (var (name, value) in source.GetQueryParameters())
        {
            target = target.WithParameter(name, value);
        }

        return target;
    }
}
