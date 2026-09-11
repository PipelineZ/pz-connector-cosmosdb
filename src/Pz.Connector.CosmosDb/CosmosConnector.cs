using System.Reflection;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Azure Cosmos DB (NoSQL API) for pz: a container is a table-shaped dataset typed from a
/// declared or sampled schema and read one feed range per partition; a sink output creates or
/// upserts each row as one document under the container's own (partition key, id) identity.</summary>
public sealed class CosmosConnector : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory;

    public CosmosConnector(ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    public ConnectorInfo Info { get; } = new(
        "cosmosdb",
        typeof(CosmosConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound
        | ConnectorCapabilities.Merge;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["database", "auth"], "properties": {
            "database": { "type": "string" },
            "auth": { "type": "string", "enum": ["connection_string", "account_key", "service_principal", "credential_chain", "managed_identity"] },
            "endpoint": { "type": "string" },
            "account_key": { "type": "string" },
            "connection_string": { "type": "string" },
            "tenant_id": { "type": "string" },
            "client_id": { "type": "string" },
            "client_secret": { "type": "string" },
            "connection_mode": { "type": "string", "enum": ["direct", "gateway"] },
            "consistency": { "type": "string", "enum": ["eventual", "consistent_prefix", "session", "bounded_staleness", "strong"] },
            "timeout": { "type": "integer", "minimum": 1, "maximum": 600 } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "container": { "type": "string" },
            "query": { "type": "string" },
            "fields": { "type": "object", "additionalProperties": { "type": "string" } },
            "sample_size": { "type": "integer", "minimum": 1, "maximum": 100000 },
            "page_size": { "type": "integer", "minimum": 1, "maximum": 10000 },
            "partitions": { "type": ["string", "integer"] },
            "columns": { "type": "object", "additionalProperties": { "type": "string" } } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        CosmosConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : new ValidationResult(errors));
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = CosmosConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        try
        {
            // The probe client never applies the requested consistency itself: baking a raised level
            // into the client makes the SDK reject the very first request (including the account
            // read below) with its own uncoded exception, before this method gets a chance to name
            // both levels. The comparison below checks the requested level against the account's
            // actual default directly, which is the same fact a data-plane client would be refused
            // over -- so CosmosErrors.Wrap's ArgumentException mapping still stands as the backstop
            // for a client actually opened at that level (source/sink reads).
            using var client = CosmosClientFactory.Create(connection with { Consistency = null });
            // The account read proves the credential; the database read proves the name.
            var account = await client.ReadAccountAsync().ConfigureAwait(false);
            if (connection.Consistency is { } requested
                && ConsistencyRank(requested) > ConsistencyRank(account.Consistency.DefaultConsistencyLevel))
            {
                return new ConnectionCheck(false, CosmosErrors.Message(connection.Redactor,
                    $"PZCS0101: consistency '{ConsistencyName(requested)}' is stronger than the account default "
                    + $"'{ConsistencyName(account.Consistency.DefaultConsistencyLevel)}'; consistency may only lower the account's default"));
            }

            try
            {
                await client.GetDatabase(connection.Database).ReadAsync(cancellationToken: ct).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return new ConnectionCheck(false, CosmosErrors.Message(connection.Redactor,
                    $"database '{connection.Database}' does not exist in account '{account.Id}' (PZCS0102); create it or fix 'database:'"));
            }

            return new ConnectionCheck(true, $"Cosmos DB account '{account.Id}', consistency {account.Consistency.DefaultConsistencyLevel}, database '{connection.Database}'");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Every failure is a failed probe, never a crash. Cancellation is not a probe result.
            return new ConnectionCheck(false, CosmosErrors.Wrap(ex, connection.Redactor, "checking the connection").Message);
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        return ValueTask.FromResult<ISource>(new CosmosSource(connection, CosmosClientFactory.Create(connection), _loggerFactory.CreateLogger<CosmosSource>()));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        return ValueTask.FromResult<ISink>(new CosmosSink(connection,
            output => CosmosClientFactory.Create(connection, bulk: true, rateLimitRetries: output.RateLimitRetries),
            _loggerFactory.CreateLogger<CosmosSink>()));
    }

    internal static CosmosConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return CosmosConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException("cosmosdb: invalid connection config: " + string.Join("; ", errors), isTransient: false);
    }

    // The SDK enum's declaration order (Strong=0 .. ConsistentPrefix=4) is not a strength order;
    // this is the actual weakest-to-strongest ranking Cosmos DB documents.
    private static int ConsistencyRank(ConsistencyLevel level) => level switch
    {
        ConsistencyLevel.Eventual => 0,
        ConsistencyLevel.ConsistentPrefix => 1,
        ConsistencyLevel.Session => 2,
        ConsistencyLevel.BoundedStaleness => 3,
        ConsistencyLevel.Strong => 4,
        _ => throw new ArgumentOutOfRangeException(nameof(level), level, "unknown consistency level"),
    };

    private static string ConsistencyName(ConsistencyLevel level) => level switch
    {
        ConsistencyLevel.Eventual => "eventual",
        ConsistencyLevel.ConsistentPrefix => "consistent_prefix",
        ConsistencyLevel.Session => "session",
        ConsistencyLevel.BoundedStaleness => "bounded_staleness",
        ConsistencyLevel.Strong => "strong",
        _ => level.ToString(),
    };
}
