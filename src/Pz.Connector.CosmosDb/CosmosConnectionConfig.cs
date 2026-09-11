using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>The typed connection surface. <c>auth</c> selects one of five credential shapes, each
/// with its own required fields (checked offline and all at once); <c>endpoint</c> is the account
/// URL for every method except <c>connection_string</c>. Every credential value is registered with
/// the redactor by content.</summary>
internal sealed record CosmosConnectionConfig(
    string Database,
    string Auth,
    string? Endpoint,
    string? AccountKey,
    string? ConnectionString,
    string? TenantId,
    string? ClientId,
    string? ClientSecret,
    ConnectionMode ConnectionMode,
    ConsistencyLevel? Consistency,
    int TimeoutSeconds,
    CosmosRedactor Redactor)
{
    public const int DefaultTimeoutSeconds = 30;
    public const int MaxTimeoutSeconds = 600;

    public static readonly string[] AuthMethods =
        ["connection_string", "account_key", "service_principal", "credential_chain", "managed_identity"];

    private static readonly string[] KnownKeys =
    [
        "database", "auth", "endpoint", "account_key", "connection_string", "tenant_id", "client_id", "client_secret",
        "connection_mode", "consistency", "timeout",
    ];

    private static readonly string[] ConsistencyNames = ["eventual", "consistent_prefix", "session", "bounded_staleness", "strong"];

    public static CosmosConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys)}");
        }

        var database = config.GetString("database");
        if (string.IsNullOrWhiteSpace(database))
        {
            errors.Add("'database' is required");
        }

        var auth = config.GetString("auth") ?? "";
        if (!AuthMethods.Contains(auth, StringComparer.Ordinal))
        {
            errors.Add(auth.Length == 0
                ? $"'auth' is required (one of: {string.Join(", ", AuthMethods)})"
                : $"'auth' must be one of {string.Join(", ", AuthMethods)} (got '{auth}')");
        }
        else
        {
            foreach (var field in RequiredFields(auth).Where(f => string.IsNullOrEmpty(config.GetString(f))))
            {
                errors.Add($"'{field}' is required for auth '{auth}'");
            }
        }

        var endpoint = config.GetString("endpoint");
        if (!string.IsNullOrEmpty(endpoint)
            && (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || (uri.Scheme != "https" && uri.Scheme != "http")))
        {
            errors.Add("'endpoint' is not an absolute http(s) URL");
        }

        foreach (var secret in new[] { "account_key", "connection_string", "client_secret" }.Select(config.GetString))
        {
            if (!string.IsNullOrEmpty(secret))
            {
                secrets.Add(secret);
            }
        }

        var mode = ConnectionMode.Direct;
        var modeText = config.GetString("connection_mode");
        if (!string.IsNullOrEmpty(modeText))
        {
            switch (modeText)
            {
                case "direct": mode = ConnectionMode.Direct; break;
                case "gateway": mode = ConnectionMode.Gateway; break;
                default: errors.Add($"'connection_mode' must be one of direct, gateway (got '{modeText}')"); break;
            }
        }

        ConsistencyLevel? consistency = null;
        var consistencyText = config.GetString("consistency");
        if (!string.IsNullOrEmpty(consistencyText))
        {
            consistency = consistencyText switch
            {
                "eventual" => ConsistencyLevel.Eventual,
                "consistent_prefix" => ConsistencyLevel.ConsistentPrefix,
                "session" => ConsistencyLevel.Session,
                "bounded_staleness" => ConsistencyLevel.BoundedStaleness,
                "strong" => ConsistencyLevel.Strong,
                _ => null,
            };
            if (consistency is null)
            {
                errors.Add($"'consistency' must be one of {string.Join(", ", ConsistencyNames)} (got '{consistencyText}')");
            }
        }

        var timeout = Options.Int(config.Values, "timeout", DefaultTimeoutSeconds, 1, MaxTimeoutSeconds, "", errors);

        return errors.Count == start
            ? new CosmosConnectionConfig(database!, auth, endpoint, config.GetString("account_key"), config.GetString("connection_string"),
                config.GetString("tenant_id"), config.GetString("client_id"), config.GetString("client_secret"),
                mode, consistency, timeout, new CosmosRedactor(secrets))
            : null;
    }

    private static string[] RequiredFields(string auth) => auth switch
    {
        "connection_string" => ["connection_string"],
        "account_key" => ["endpoint", "account_key"],
        "service_principal" => ["endpoint", "tenant_id", "client_id", "client_secret"],
        "credential_chain" => ["endpoint"],
        "managed_identity" => ["endpoint"],
        _ => [],
    };
}
