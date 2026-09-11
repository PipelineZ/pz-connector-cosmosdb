using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>One pz partition: one or more feed ranges read one after another, each through the
/// query's own continuation. Every page is parsed straight from the response stream; the
/// <c>Documents</c> array is the only thing read. A non-success page is classified by status
/// (410 -- the range split or moved -- is transient, and the engine's retry re-plans the ranges).
/// Cancellation is observed per page; a cancelled call is rethrown as cancellation, never
/// classified as a transient failure.</summary>
internal sealed class CosmosPartition(
    CosmosConnectionConfig connection, Container container, CosmosDatasetConfig dataset, ColumnPlan plan,
    QueryDefinition query, IReadOnlyList<FeedRange> ranges, DatasetSpec spec, ILogger logger) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var context = $"dataset '{spec.Dataset}': reading '{dataset.Container}'";
        var builder = new DocumentBatchBuilder(plan, options, spec.Dataset, connection.Redactor);
        var requestOptions = new QueryRequestOptions { MaxItemCount = dataset.PageSize };
        var pages = 0L;
        var rows = 0L;
        foreach (var range in ranges)
        {
            using var iterator = container.GetItemQueryStreamIterator(range, query, null, requestOptions);
            while (iterator.HasMoreResults)
            {
                ct.ThrowIfCancellationRequested();
                using var page = await NextPageAsync(iterator, context, ct).ConfigureAwait(false);
                pages++;
                using var document = await ParsePageAsync(page, context, ct).ConfigureAwait(false);
                if (!document.RootElement.TryGetProperty("Documents", out var documents))
                {
                    throw CosmosErrors.Fatal($"{context}: the query response carried no 'Documents' array", connection.Redactor);
                }

                foreach (var item in documents.EnumerateArray())
                {
                    builder.Append(item);
                    rows++;
                    if (builder.TryTakeBatch(out var batch))
                    {
                        yield return batch!;
                    }
                }
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }

        logger.LogDebug("cosmosdb: dataset {Dataset}: {Rows} documents in {Pages} pages over {Ranges} feed range(s)", spec.Dataset, rows, pages, ranges.Count);
    }

    /// <summary>The page body, classified like every other failure: a truncated or non-JSON
    /// response is the service's text reaching us, so it goes through the redactor and the
    /// connector prefix rather than out as a raw <see cref="JsonException"/>.</summary>
    private async Task<JsonDocument> ParsePageAsync(ResponseMessage page, string context, CancellationToken ct)
    {
        try
        {
            return await JsonDocument.ParseAsync(page.Content, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw CosmosErrors.Wrap(ex, connection.Redactor, context);
        }
    }

    private async Task<ResponseMessage> NextPageAsync(FeedIterator iterator, string context, CancellationToken ct)
    {
        ResponseMessage page;
        try
        {
            page = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw CosmosErrors.Wrap(ex, connection.Redactor, context);
        }

        if (!page.IsSuccessStatusCode)
        {
            var failure = CosmosErrors.FromResponse(page, connection.Redactor, context);
            page.Dispose();
            throw failure;
        }

        return page;
    }
}
