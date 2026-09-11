using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Per-output write options: <c>container</c> (defaults to the entity name), <c>id_from</c>
/// (columns whose values form the document id when the row has no <c>id</c> column),
/// <c>concurrency</c> (in-flight requests) and <c>rate_limit_retries</c> (the SDK's 429 backoff
/// budget). Column checks against the real schema and the container's partition key happen at
/// BeginWriteAsync (<see cref="ValidateSchema"/>); Parse only knows the option shapes.</summary>
internal sealed record CosmosOutputConfig(string Container, IReadOnlyList<string>? IdFrom, int Concurrency, int RateLimitRetries)
{
    public const int DefaultConcurrency = 32;
    public const int MaxConcurrency = 256;
    public const int DefaultRateLimitRetries = 9;
    public const int MaxRateLimitRetries = 100;

    private static readonly string[] KnownKeys = ["container", "id_from", "concurrency", "rate_limit_retries"];
    private static readonly string[] Modes = ["append", "merge"];

    /// <summary>Exactly what <see cref="RowDocumentWriter"/> can spell -- pz's v0 type matrix.</summary>
    private static readonly ArrowTypeId[] DocumentTypes =
    [
        ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double,
        ArrowTypeId.Decimal128, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp,
    ];

    private static readonly ArrowTypeId[] IdTypes = [ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64];

    public static CosmosOutputConfig? Parse(OutputSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"output '{spec.Output}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown write option '{key}'; known: {string.Join(", ", KnownKeys)}");
        }

        if (spec.Mode == "replace")
        {
            errors.Add($"{prefix}: mode replace is not supported (PZCS0301): Cosmos DB has no container rename and no atomic truncate, so a replace cannot be staged and swapped; use append or merge");
        }
        else if (!Modes.Contains(spec.Mode, StringComparer.Ordinal))
        {
            errors.Add($"{prefix}: mode '{spec.Mode}' is not supported; cosmosdb supports append and merge");
        }

        var container = spec.Output;
        if (spec.Options.TryGetValue("container", out var containerRaw))
        {
            container = containerRaw?.ToString() ?? "";
            if (container.Length == 0)
            {
                errors.Add($"{prefix}: 'container' must be a non-empty string");
            }
        }

        var idFrom = Options.Strings(spec.Options, "id_from", prefix, errors);
        if (idFrom is { Count: 0 })
        {
            errors.Add($"{prefix}: 'id_from' must name at least one column");
        }

        var concurrency = Options.Int(spec.Options, "concurrency", DefaultConcurrency, 1, MaxConcurrency, prefix, errors);
        var retries = Options.Int(spec.Options, "rate_limit_retries", DefaultRateLimitRetries, 0, MaxRateLimitRetries, prefix, errors);

        return errors.Count == start ? new CosmosOutputConfig(container, idFrom, concurrency, retries) : null;
    }

    /// <summary>Every column must be spellable as JSON and nameable as a property; the partition key
    /// paths must be columns; the id must be resolvable the way the mode needs; and a merge's keys
    /// must be the identity Cosmos DB upserts on.</summary>
    public static void ValidateSchema(OutputSpec spec, CosmosOutputConfig output, Schema schema, IReadOnlyList<string> partitionKeyPaths, List<string> errors)
    {
        var prefix = $"output '{spec.Output}'";
        // First-wins rather than ToDictionary: a schema carrying the same name twice must be
        // reported by ValidateNames alongside every other error, not thrown out of validation.
        var columns = new Dictionary<string, Field>(StringComparer.Ordinal);
        foreach (var field in schema.FieldsList)
        {
            columns.TryAdd(field.Name, field);
        }

        foreach (var field in schema.FieldsList)
        {
            if (!DocumentTypes.Contains(field.DataType.TypeId))
            {
                errors.Add($"{prefix}: column '{field.Name}' is {field.DataType.TypeId}, which a JSON document cannot carry; allowed: {string.Join(", ", DocumentTypes)}. Drop it from the pipeline's projection or cast it");
            }

            if (field.Name.Length == 0 || field.Name.Split('.').Any(s => s.Length == 0))
            {
                errors.Add($"{prefix}: column '{field.Name}' is not a property name Cosmos DB accepts (no empty segments)");
            }
            else if (field.Name.StartsWith('_') && field.Name != "_ts")
            {
                errors.Add($"{prefix}: column '{field.Name}' is a Cosmos DB system property name (the '_' prefix is reserved); rename it in the pipeline");
            }
        }

        if (columns.TryGetValue("id", out var id) && !IdTypes.Contains(id.DataType.TypeId))
        {
            errors.Add($"{prefix}: the 'id' column is {id.DataType.TypeId}; only a varchar, int32 or int64 column can be the document id");
        }

        if (output.IdFrom is { } idFrom)
        {
            foreach (var name in idFrom)
            {
                if (!columns.TryGetValue(name, out var column))
                {
                    errors.Add($"{prefix}: id_from entry '{name}' is not a column of the pipeline's output");
                }
                else if (!IdTypes.Contains(column.DataType.TypeId))
                {
                    errors.Add($"{prefix}: id_from entry '{name}' is a {column.DataType.TypeId} column; only varchar, int32 or int64 columns can form the id");
                }
            }
        }

        ColumnPlan.ValidateNames(schema.FieldsList.Select(f => f.Name).ToList(), prefix, errors);

        // Nested columns of one parent must be adjacent: the writer streams objects in column
        // order and JSON forbids a repeated key. Every proper prefix of a dotted name is a parent;
        // a parent seen, left, and seen again is the refusal.
        var lastParents = new HashSet<string>(StringComparer.Ordinal);
        var closedParents = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in schema.FieldsList.Select(f => f.Name))
        {
            var parents = new HashSet<string>(StringComparer.Ordinal);
            for (var dot = name.IndexOf('.'); dot > 0; dot = name.IndexOf('.', dot + 1))
            {
                parents.Add(name[..dot]);
            }

            foreach (var left in lastParents.Where(p => !parents.Contains(p)))
            {
                closedParents.Add(left);
            }

            var reopened = parents.FirstOrDefault(closedParents.Contains);
            if (reopened is not null)
            {
                errors.Add($"{prefix}: nested columns of '{reopened}' are not adjacent ('{name}' comes after another parent); keep the columns of one nested object together in the pipeline's SELECT list");
            }

            lastParents = parents;
        }

        var pkColumns = new List<string>();
        foreach (var path in partitionKeyPaths)
        {
            var column = path.TrimStart('/').Replace('/', '.');
            pkColumns.Add(column);
            if (!columns.ContainsKey(column))
            {
                errors.Add($"{prefix}: the container's partition key path '{path}' needs a column named '{column}' in the pipeline's output (PZCS0303); columns present: {string.Join(", ", columns.Keys)}");
            }
        }

        if (spec.Mode == "merge")
        {
            var identity = columns.ContainsKey("id") ? new HashSet<string>(["id"], StringComparer.Ordinal)
                : output.IdFrom is { } from ? new HashSet<string>(from, StringComparer.Ordinal)
                : null;
            if (identity is null)
            {
                errors.Add($"{prefix}: mode merge needs an 'id' column or 'id_from' -- Cosmos DB upserts on (partition key, id)");
                return;
            }

            var keys = new HashSet<string>(spec.Keys, StringComparer.Ordinal);
            var withPk = new HashSet<string>(identity.Concat(pkColumns), StringComparer.Ordinal);
            if (!keys.SetEquals(identity) && !keys.SetEquals(withPk))
            {
                errors.Add($"{prefix}: merge keys [{string.Join(", ", spec.Keys)}] are not the document identity (PZCS0304); Cosmos DB upserts on (partition key, id), so keys must be [{string.Join(", ", identity)}] or [{string.Join(", ", withPk)}]");
            }
        }
    }
}
