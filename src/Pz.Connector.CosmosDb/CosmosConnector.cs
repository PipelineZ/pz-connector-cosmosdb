using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Azure Cosmos DB (NoSQL API) for pz: a container is a table-shaped dataset typed from a
/// declared or sampled schema and read one feed range per partition; a sink output creates or
/// upserts each row as one document under the container's own (partition key, id) identity.</summary>
public sealed class CosmosConnector : IConnector
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

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(false, "not implemented"));
}
