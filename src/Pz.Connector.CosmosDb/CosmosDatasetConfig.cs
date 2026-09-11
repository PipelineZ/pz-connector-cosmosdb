using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Per-dataset read options: <c>container</c> (defaults to the entity name), <c>query</c>
/// (a Cosmos DB SQL query; default <c>SELECT * FROM c</c>), <c>fields</c> (a declared schema, path to
/// type), <c>sample_size</c> (documents inspected when inferring), <c>page_size</c> (items per query
/// page), and <c>partitions</c> (<c>auto</c> = one per feed range, or a count). A declared schema
/// replaces inference entirely.</summary>
internal sealed record CosmosDatasetConfig(
    string Container, string? Query, IReadOnlyList<ColumnSpec>? Fields, int SampleSize, int PageSize, int? Partitions)
{
    public const int DefaultSampleSize = 1000;
    public const int MaxSampleSize = 100_000;
    public const int DefaultPageSize = 1000;
    public const int MaxPageSize = 10_000;

    // "columns" is the engine's own: it stamps a dataset's declared columns: contract into the
    // options of every spec. The plan itself still comes from fields: or inference.
    private static readonly string[] KnownKeys = ["container", "query", "fields", "sample_size", "page_size", "partitions", "columns"];

    public static CosmosDatasetConfig? Parse(DatasetSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"dataset '{spec.Dataset}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown read option '{key}'; known: {string.Join(", ", KnownKeys.Where(k => k != "columns"))}");
        }

        var container = spec.Dataset;
        if (spec.Options.TryGetValue("container", out var containerRaw))
        {
            container = containerRaw?.ToString() ?? "";
            if (container.Length == 0)
            {
                errors.Add($"{prefix}: 'container' must be a non-empty string");
            }
        }

        string? query = null;
        if (spec.Options.TryGetValue("query", out var queryRaw) && queryRaw is not null)
        {
            query = queryRaw.ToString();
            if (string.IsNullOrWhiteSpace(query))
            {
                errors.Add($"{prefix}: 'query' must be a non-empty SQL string");
            }
        }

        IReadOnlyList<ColumnSpec>? fields = null;
        if (spec.Options.TryGetValue("fields", out var fieldsRaw) && fieldsRaw is not null)
        {
            fields = ParseFields(fieldsRaw, prefix, errors);
        }

        var sampleSize = Options.Int(spec.Options, "sample_size", DefaultSampleSize, 1, MaxSampleSize, prefix, errors);
        var pageSize = Options.Int(spec.Options, "page_size", DefaultPageSize, 1, MaxPageSize, prefix, errors);

        int? partitions = null;
        if (spec.Options.TryGetValue("partitions", out var partitionsRaw) && partitionsRaw is not null)
        {
            if (partitionsRaw is string text && text.Trim().Equals("auto", StringComparison.OrdinalIgnoreCase))
            {
                partitions = null;
            }
            else
            {
                partitions = Options.Int(spec.Options, "partitions", 1, 1, int.MaxValue, prefix, errors);
            }
        }

        return errors.Count == start ? new CosmosDatasetConfig(container, query, fields, sampleSize, pageSize, partitions) : null;
    }

    private static IReadOnlyList<ColumnSpec>? ParseFields(object raw, string prefix, List<string> errors)
    {
        if (raw is not IEnumerable<KeyValuePair<string, object?>> map)
        {
            errors.Add($"{prefix}: 'fields' must be a mapping of field path to type ({ColumnPlan.KindNames})");
            return null;
        }

        var start = errors.Count;
        var columns = new List<ColumnSpec>();
        foreach (var (name, typeRaw) in map)
        {
            if (string.IsNullOrEmpty(name) || name.Split('.').Any(segment => segment.Length == 0))
            {
                errors.Add($"{prefix}: 'fields' has an empty field path");
                continue;
            }

            var typeText = typeRaw?.ToString() ?? "";
            if (!ColumnPlan.TryParseKind(typeText, out var kind))
            {
                errors.Add($"{prefix}: field '{name}' has unknown type '{typeText}'; known: {ColumnPlan.KindNames}");
                continue;
            }

            columns.Add(ColumnSpec.Of(name, kind));
        }

        if (columns.Count == 0 && errors.Count == start)
        {
            errors.Add($"{prefix}: 'fields' must declare at least one field");
        }

        ColumnPlan.ValidateNames(columns.Select(c => c.Name).ToList(), prefix, errors);
        return errors.Count == start ? columns : null;
    }
}
