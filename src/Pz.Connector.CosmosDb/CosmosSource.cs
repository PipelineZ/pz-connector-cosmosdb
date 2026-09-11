using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>A container as one table-shaped dataset: the schema is declared under <c>fields:</c> or
/// inferred from a sample, the engine's watermark bounds become a range on the cursor, pruning
/// narrows the projection, and the container's feed ranges become the partitions. No native scan:
/// DuckDB cannot speak the Cosmos DB protocol.</summary>
internal sealed class CosmosSource(CosmosConnectionConfig connection, CosmosClient client, ILogger logger) : ISource
{
    private readonly Database _database = client.GetDatabase(connection.Database);
    private readonly Dictionary<string, ColumnPlan> _plans = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _planning = new(1, 1);

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        return new DatasetSchema(plan.Schema);
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        var projected = plan.Project(hints.Columns);
        var query = CosmosQueryBuilder.Read(dataset, projected, spec, connection.Redactor);
        var container = _database.GetContainer(dataset.Container);
        var ranges = await FeedRangesAsync(container, dataset, spec, ct).ConfigureAwait(false);

        // auto = one pz partition per feed range; N below the count folds ranges round-robin.
        var count = dataset.Partitions is { } n && n < ranges.Count ? n : ranges.Count;
        var groups = Enumerable.Range(0, count).Select(_ => new List<FeedRange>()).ToList();
        for (var i = 0; i < ranges.Count; i++)
        {
            groups[i % count].Add(ranges[i]);
        }

        logger.LogDebug("cosmosdb: dataset {Dataset}: {Columns} of {Total} columns, {Partitions} partition(s) over {Ranges} feed range(s)",
            spec.Dataset, projected.Columns.Count, plan.Columns.Count, count, ranges.Count);
        return groups.Select(g => (IDatasetPartition)new CosmosPartition(connection, container, dataset, projected, query, g, spec, logger)).ToList();
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        _planning.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<IReadOnlyList<FeedRange>> FeedRangesAsync(Container container, CosmosDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        try
        {
            return await container.GetFeedRangesAsync(ct).ConfigureAwait(false);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            throw MissingContainer(dataset, spec, ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw CosmosErrors.Wrap(ex, connection.Redactor, $"dataset '{spec.Dataset}': listing the feed ranges of '{dataset.Container}'");
        }
    }

    /// <summary>A declared schema is the plan. Otherwise the first <c>sample_size</c> documents of
    /// the user's query (not the watermark bounds, so an incremental read plans the same columns as
    /// a full one), in <c>_ts</c> order, are inferred from; an empty sample is a refusal, because a
    /// dataset with no columns is a misconfiguration, not an empty table. One sample serves both the
    /// schema call and the read that follows it.</summary>
    private async Task<ColumnPlan> PlanAsync(CosmosDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        if (dataset.Fields is { } fields)
        {
            return new ColumnPlan(fields);
        }

        var key = $"{spec.Dataset}\n{dataset.Container}\n{dataset.SampleSize}\n{dataset.Query}";
        await _planning.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_plans.TryGetValue(key, out var plan))
            {
                plan = await InferAsync(dataset, spec, ct).ConfigureAwait(false);
                _plans[key] = plan;
            }

            return plan;
        }
        finally
        {
            _planning.Release();
        }
    }

    private async Task<ColumnPlan> InferAsync(CosmosDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"dataset '{spec.Dataset}': sampling '{dataset.Container}'";
        // The sample is ordered by _ts, which a dataset's own query must therefore project: a
        // projection without it is either sampled as nothing (the service drops from an ORDER BY
        // every item whose ordering property is undefined) or rejected outright as an unmappable
        // ordering, and neither outcome names _ts on its own. Every non-transient sampling failure
        // of a dataset that carries a query says so; a transient one is about the service, not the
        // query, and keeps the plain context.
        var refusal = dataset.Query is null
            ? context
            : context + " (the sample is ordered by _ts, so the dataset's own query must project c._ts, or the dataset must declare fields:)";
        var container = _database.GetContainer(dataset.Container);
        var inference = new SchemaInference(spec.Dataset, redactor);
        try
        {
            using var iterator = container.GetItemQueryStreamIterator(CosmosQueryBuilder.Sample(dataset, dataset.SampleSize), null,
                new QueryRequestOptions { MaxItemCount = Math.Min(dataset.PageSize, dataset.SampleSize) });
            while (iterator.HasMoreResults && inference.Documents < dataset.SampleSize)
            {
                using var page = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
                if (page.StatusCode == HttpStatusCode.NotFound)
                {
                    throw MissingContainer(dataset, spec, null);
                }

                if (!page.IsSuccessStatusCode)
                {
                    throw CosmosErrors.FromResponse(page, redactor, CosmosErrors.IsTransientStatus(page.StatusCode) ? context : refusal);
                }

                using var document = await JsonDocument.ParseAsync(page.Content, cancellationToken: ct).ConfigureAwait(false);
                if (!document.RootElement.TryGetProperty("Documents", out var documents))
                {
                    throw CosmosErrors.Fatal($"{context}: the query response carried no 'Documents' array", redactor);
                }

                foreach (var item in documents.EnumerateArray())
                {
                    inference.Observe(item);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Classified once, then re-issued against the refusal context when the outcome is
            // fatal; an exception that is already ours (a refusal thrown above) passes through
            // either call unchanged, so it is never decorated twice.
            var failure = CosmosErrors.Wrap(ex, redactor, context);
            throw dataset.Query is not null && failure is PzConnectorException { IsTransient: false }
                ? CosmosErrors.Wrap(ex, redactor, refusal)
                : failure;
        }

        if (inference.Documents == 0)
        {
            // With a user query the empty sample is almost never an empty container: the sample is
            // ordered by _ts, and Cosmos DB drops from an ORDER BY every item whose ordering
            // property is undefined -- which a projection that does not carry _ts makes every item.
            // Naming the container alone would send the author looking for missing data.
            throw CosmosErrors.Fatal(
                dataset.Query is null
                    ? $"dataset '{spec.Dataset}': '{dataset.Container}' has no document matching the query to infer a schema from; declare the columns under fields:"
                    : $"dataset '{spec.Dataset}': the 'query:' of '{dataset.Container}' returned no document to infer a schema from; the sample is ordered by '_ts', which a query must project (add c._ts to its SELECT list) for its items to be sampled at all -- otherwise declare the columns under fields:",
                redactor);
        }

        var plan = inference.Plan();
        logger.LogDebug("cosmosdb: dataset {Dataset}: inferred {Columns} columns from {Documents} documents", spec.Dataset, plan.Columns.Count, inference.Documents);
        return plan;
    }

    private PzConnectorException MissingContainer(CosmosDatasetConfig dataset, DatasetSpec spec, Exception? inner) =>
        CosmosErrors.Fatal(
            $"dataset '{spec.Dataset}': container '{dataset.Container}' does not exist in database '{connection.Database}' (PZCS0302); create it or fix 'container:'",
            connection.Redactor, inner);

    private CosmosDatasetConfig ParseDataset(DatasetSpec spec)
    {
        var errors = new List<string>();
        return CosmosDatasetConfig.Parse(spec, errors) ?? throw CosmosErrors.Fatal(string.Join("; ", errors), connection.Redactor);
    }
}
