using System.Diagnostics.CodeAnalysis;
using System.Net;
using Apache.Arrow;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Document writes. <c>append</c> creates, <c>merge</c> upserts each row as a whole
/// document under the container's (partition key, id) identity; <c>replace</c> is refused because
/// Cosmos DB has no container rename and no atomic truncate. The container must exist -- its
/// partition key and throughput are ops decisions -- and its partition key paths are read once per
/// session and checked against the schema before a single row is sent. No native copy, and
/// <see cref="AbortSemantics.BestEffort"/>: requests already acknowledged cannot be unsent.</summary>
internal sealed class CosmosSink(CosmosConnectionConfig connection, Func<CosmosOutputConfig, CosmosClient> clientFactory, ILogger logger) : ISink
{
    private readonly List<CosmosClient> _clients = [];

    public AbortSemantics AbortSemantics => AbortSemantics.BestEffort;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var errors = new List<string>();
        var output = CosmosOutputConfig.Parse(spec, errors)
            ?? throw CosmosErrors.Fatal(string.Join("; ", errors), connection.Redactor);

        var client = clientFactory(output);
        _clients.Add(client);
        var container = client.GetDatabase(connection.Database).GetContainer(output.Container);
        IReadOnlyList<string> partitionKeyPaths;
        try
        {
            partitionKeyPaths = (await container.ReadContainerAsync(cancellationToken: ct).ConfigureAwait(false)).Resource.PartitionKeyPaths;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw CosmosErrors.Fatal(
                $"output '{spec.Output}': container '{output.Container}' does not exist in database '{connection.Database}' (PZCS0302); create it with its partition key, or fix 'container:'",
                connection.Redactor, ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw CosmosErrors.Wrap(ex, connection.Redactor, $"output '{spec.Output}': reading container '{output.Container}'");
        }

        CosmosOutputConfig.ValidateSchema(spec, output, schema, partitionKeyPaths, errors);
        if (errors.Count > 0)
        {
            throw CosmosErrors.Fatal(string.Join("; ", errors), connection.Redactor);
        }

        logger.LogDebug("cosmosdb: output {Output}: {Mode} into '{Container}' (partition key {Paths}, concurrency {Concurrency})",
            spec.Output, spec.Mode, output.Container, string.Join(",", partitionKeyPaths), output.Concurrency);
        return new CosmosWriteSession(connection, container, output, schema, partitionKeyPaths, spec.Mode == "merge", spec.Output, logger);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var client in _clients)
        {
            client.Dispose();
        }

        return ValueTask.CompletedTask;
    }
}
