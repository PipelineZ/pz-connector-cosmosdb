using System.Text.Json;

namespace Pz.Connector.CosmosDb;

/// <summary>Turns a sample of documents into a <see cref="ColumnPlan"/>: every scalar leaf is a
/// column named by its dotted path, in first-seen order; nested objects flatten; arrays, values of
/// no scalar kind, and fields whose kind varies across the sample land as JSON text; numbers widen
/// (integral within ±2^53 -> int64, anything else -> double); <c>id</c> is the one trailing column;
/// the service-owned <c>_rid</c>/<c>_self</c>/<c>_etag</c>/<c>_attachments</c> are skipped. Null and
/// missing values carry no kind. A field whose own name contains a dot is refused: the column named
/// <c>a.b</c> is the path into a nested object, and a literal <c>"a.b"</c> field would share its
/// name and lose its values silently.</summary>
internal sealed class SchemaInference(string datasetName, CosmosRedactor redactor)
{
    [Flags]
    internal enum Seen { None = 0, String = 1, Int64 = 2, Double = 4, Boolean = 8, Object = 16, Array = 32 }

    private const Seen Numeric = Seen.Int64 | Seen.Double;

    private readonly Dictionary<string, Seen> _seen = new(StringComparer.Ordinal);
    private readonly List<string> _order = [];
    private readonly HashSet<string> _parents = new(StringComparer.Ordinal);

    public int Documents { get; private set; }

    public void Observe(JsonElement document)
    {
        Documents++;
        foreach (var property in document.EnumerateObject())
        {
            if (property.Name == "id" || ColumnPlan.SystemProperties.Contains(property.Name, StringComparer.Ordinal))
            {
                continue;
            }

            Walk(null, property.Name, property.Value, document);
        }
    }

    /// <summary>The document's <c>id</c> as a refusal names it.</summary>
    public static string IdOf(JsonElement document) =>
        document.ValueKind == JsonValueKind.Object && document.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String
            ? id.GetString()!
            : "?";

    private void Walk(string? parent, string name, JsonElement value, JsonElement document)
    {
        if (name.Contains('.'))
        {
            throw CosmosErrors.Fatal(
                $"dataset '{datasetName}': field '{name}' of document {IdOf(document)} has a dot in its own name, which a column path cannot tell from a nested field; declare its parent as json under fields:",
                redactor);
        }

        var kind = KindOf(value);
        if (kind == Seen.None)
        {
            return;
        }

        var path = parent is null ? name : parent + "." + name;
        if (!_seen.TryGetValue(path, out var seen))
        {
            _order.Add(path);
            if (_order.Count > ColumnPlan.MaxColumns)
            {
                throw CosmosErrors.Fatal(
                    $"dataset '{datasetName}': more than {ColumnPlan.MaxColumns} distinct field paths in the sample; a field with dynamic keys should be declared as json under fields:",
                    redactor);
            }

            if (parent is not null)
            {
                _parents.Add(parent);
            }
        }

        _seen[path] = seen | kind;
        if (kind == Seen.Object)
        {
            foreach (var property in value.EnumerateObject())
            {
                Walk(path, property.Name, property.Value, document);
            }
        }
    }

    public ColumnPlan Plan()
    {
        // A path lands as JSON when it is ever an array, or a mix of an object and something else;
        // an object-only path is not a column -- its leaves are -- unless it never had any.
        var json = new HashSet<string>(StringComparer.Ordinal);
        var columns = new List<ColumnSpec>();
        foreach (var path in _order)
        {
            if (HasJsonAncestor(path, json))
            {
                continue;
            }

            var seen = _seen[path];
            if (seen == Seen.Object)
            {
                if (!_parents.Contains(path))
                {
                    columns.Add(ColumnSpec.Of(path, ColumnKind.Json));
                }

                continue;
            }

            var kind = Resolve(seen);
            if (kind == ColumnKind.Json)
            {
                json.Add(path);
            }

            columns.Add(ColumnSpec.Of(path, kind));
        }

        columns.Add(ColumnSpec.Of("id", ColumnKind.String));

        var errors = new List<string>();
        ColumnPlan.ValidateNames(columns.Select(c => c.Name).ToList(), $"dataset '{datasetName}'", errors);
        if (errors.Count > 0)
        {
            throw CosmosErrors.Fatal(string.Join("; ", errors), redactor);
        }

        return new ColumnPlan(columns);
    }

    private static bool HasJsonAncestor(string path, HashSet<string> json)
    {
        for (var dot = path.IndexOf('.'); dot > 0; dot = path.IndexOf('.', dot + 1))
        {
            if (json.Contains(path[..dot]))
            {
                return true;
            }
        }

        return false;
    }

    internal static ColumnKind Resolve(Seen seen)
    {
        if ((seen & (Seen.Array | Seen.Object)) != 0)
        {
            return ColumnKind.Json;
        }

        if ((seen & ~Numeric) == 0)
        {
            return (seen & Seen.Double) != 0 ? ColumnKind.Double : ColumnKind.Int64;
        }

        return seen switch
        {
            Seen.String => ColumnKind.String,
            Seen.Boolean => ColumnKind.Boolean,
            _ => ColumnKind.Json,
        };
    }

    /// <summary>JSON numbers are untyped: integral and within ±2^53 (what a Cosmos double holds
    /// exactly) reads as int64, anything else as double.</summary>
    internal static Seen KindOf(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => Seen.None,
        JsonValueKind.String => Seen.String,
        JsonValueKind.True or JsonValueKind.False => Seen.Boolean,
        JsonValueKind.Number => value.TryGetInt64(out var l) && l is >= -9007199254740992 and <= 9007199254740992 ? Seen.Int64 : Seen.Double,
        JsonValueKind.Object => Seen.Object,
        JsonValueKind.Array => Seen.Array,
        _ => Seen.None,
    };
}
