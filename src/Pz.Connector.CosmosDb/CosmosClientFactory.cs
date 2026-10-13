using Azure.Identity;
using Microsoft.Azure.Cosmos;

namespace Pz.Connector.CosmosDb;

/// <summary>One config, one client. Every auth method maps to exactly one <see cref="CosmosClient"/>
/// constructor; the client is heavyweight and owned by whichever source/sink created it. Bulk mode
/// and the 429 backoff budget are sink concerns, passed in by the sink; the source leaves both at
/// the defaults. Client telemetry to Azure is off: a batch tool has nothing to phone home about.
/// The library's own distributed-tracing spans stay on; they cost nothing until a listener (the
/// host's OTLP export) subscribes to the source.</summary>
internal static class CosmosClientFactory
{
    // The library emits its "Azure.Cosmos.Operation" spans through Azure.Core, which keeps them off
    // until this switch is set; it is read once, so it must be set before the first client exists.
    static CosmosClientFactory() => AppContext.SetSwitch("Azure.Experimental.EnableActivitySource", true);

    public static CosmosClient Create(CosmosConnectionConfig connection, bool bulk = false, int rateLimitRetries = 9)
    {
        var options = new CosmosClientOptions
        {
            ConnectionMode = connection.ConnectionMode,
            RequestTimeout = TimeSpan.FromSeconds(connection.TimeoutSeconds),
            AllowBulkExecution = bulk,
            MaxRetryAttemptsOnRateLimitedRequests = rateLimitRetries,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
            CosmosClientTelemetryOptions = new CosmosClientTelemetryOptions { DisableSendingMetricsToService = true, DisableDistributedTracing = false },
        };
        if (connection.Consistency is { } level)
        {
            options.ConsistencyLevel = level;
        }

        return connection.Auth switch
        {
            "connection_string" => new CosmosClient(connection.ConnectionString, options),
            "account_key" => new CosmosClient(connection.Endpoint, connection.AccountKey, options),
            "service_principal" => new CosmosClient(connection.Endpoint,
                new ClientSecretCredential(connection.TenantId, connection.ClientId, connection.ClientSecret), options),
            "credential_chain" => new CosmosClient(connection.Endpoint, new DefaultAzureCredential(), options),
            "managed_identity" => new CosmosClient(connection.Endpoint,
                new ManagedIdentityCredential(string.IsNullOrEmpty(connection.ClientId)
                    ? ManagedIdentityId.SystemAssigned
                    : ManagedIdentityId.FromUserAssignedClientId(connection.ClientId)), options),
            _ => throw new InvalidOperationException($"auth '{connection.Auth}' passed validation"),
        };
    }
}
