using Azure.Identity;
using Microsoft.Azure.Cosmos;

namespace Pz.Connector.CosmosDb;

/// <summary>One config, one client. Every auth method maps to exactly one <see cref="CosmosClient"/>
/// constructor; the client is heavyweight and owned by whichever source/sink created it. Bulk mode
/// and the 429 backoff budget are sink concerns, passed in by the sink; the source leaves both at
/// the defaults. Client telemetry to Azure is off: a batch tool has nothing to phone home about.</summary>
internal static class CosmosClientFactory
{
    public static CosmosClient Create(CosmosConnectionConfig connection, bool bulk = false, int rateLimitRetries = 9)
    {
        var options = new CosmosClientOptions
        {
            ConnectionMode = connection.ConnectionMode,
            RequestTimeout = TimeSpan.FromSeconds(connection.TimeoutSeconds),
            AllowBulkExecution = bulk,
            MaxRetryAttemptsOnRateLimitedRequests = rateLimitRetries,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
            CosmosClientTelemetryOptions = new CosmosClientTelemetryOptions { DisableSendingMetricsToService = true },
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
