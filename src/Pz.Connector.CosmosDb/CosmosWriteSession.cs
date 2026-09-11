using Apache.Arrow;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>One output's write: every row becomes one create (append) or upsert (merge) request,
/// issued through the SDK's bulk mode -- which batches them per partition on the wire -- with at
/// most <c>concurrency</c> in flight. The first failed request fails the session with its status:
/// a 409 on append is a duplicate id (non-transient), an exhausted 429 is transient with the
/// service's retry-after. <see cref="AbortSemantics.BestEffort"/>: requests already acknowledged
/// cannot be unsent.</summary>
internal sealed class CosmosWriteSession : ISinkWriteSession
{
    private readonly CosmosRedactor _redactor;
    private readonly Container _container;
    private readonly CosmosOutputConfig _output;
    private readonly bool _upsert;
    private readonly string _outputName;
    private readonly ILogger _logger;
    private readonly RowDocumentWriter _writer;
    private readonly SemaphoreSlim _inFlight;
    private readonly List<Task> _pending = [];
    // Duplicate identities within one merge session (same partition key + id, written twice as
    // two rows) are otherwise sent to the wire concurrently: bulk mode gives no ordering guarantee
    // across requests issued that way, so which one "wins" the upsert would be a race rather than
    // the mandated last-writer-wins. Chaining a repeated identity's send after its predecessor's
    // completion is what actually makes submission order the outcome. Append mode never chains:
    // every generated id is distinct and a duplicate explicit id is a 409 either way, so tracking
    // identities there would only ever grow the map. Entries are pruned as their task completes
    // (DrainCompletedAsync) and the map is cleared once the session finishes.
    private readonly Dictionary<string, Task> _lastByIdentity = new(StringComparer.Ordinal);
    private readonly ItemRequestOptions _requestOptions = new() { EnableContentResponseOnWrite = false };
    private long _rows;
    private long _batches;
    private bool _committed;
    private bool _aborted;

    public CosmosWriteSession(CosmosConnectionConfig connection, Container container, CosmosOutputConfig output, Schema schema,
        IReadOnlyList<string> partitionKeyPaths, bool upsert, string outputName, ILogger logger)
    {
        _redactor = connection.Redactor;
        _container = container;
        _output = output;
        _upsert = upsert;
        _outputName = outputName;
        _logger = logger;
        _writer = new RowDocumentWriter(schema, output, partitionKeyPaths, outputName, connection.Redactor, () => Guid.NewGuid().ToString("N"));
        _inFlight = new SemaphoreSlim(output.Concurrency, output.Concurrency);
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();
        for (var row = 0; row < batch.Length; row++)
        {
            ct.ThrowIfCancellationRequested();
            // Serialised now, while the batch is still engine-owned; the bytes are our own copy.
            var document = _writer.Write(batch, row, _rows + 1);
            _rows++;
            await _inFlight.WaitAsync(ct).ConfigureAwait(false);
            Task? previous = null;
            string? identity = null;
            if (_upsert)
            {
                identity = IdentityOf(document);
                _lastByIdentity.TryGetValue(identity, out previous);
            }

            var send = SendAsync(document, previous, ct);
            if (identity is not null)
            {
                _lastByIdentity[identity] = send;
            }

            _pending.Add(send);
            if (_pending.Count >= _output.Concurrency * 2)
            {
                await DrainCompletedAsync().ConfigureAwait(false);
            }
        }

        _batches++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        await Task.WhenAll(_pending).ConfigureAwait(false);
        _pending.Clear();
        _lastByIdentity.Clear();
        _logger.LogDebug("cosmosdb: output {Output}: committed {Rows} rows in {Batches} batches", _outputName, _rows, _batches);
        return new WriteResult(_rows, _batches);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        _aborted = true;
        // Requests already sent land or fail on their own; waiting keeps the client's bulk
        // dispatcher from being disposed under them. Their outcomes no longer matter.
        try
        {
            await Task.WhenAll(_pending).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug("cosmosdb: output {Output}: a request failed during abort ({Reason})", _outputName, _redactor.Redact(ex.Message));
        }

        _pending.Clear();
        _lastByIdentity.Clear();
    }

    public ValueTask DisposeAsync()
    {
        _inFlight.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>The (partition key, id) tuple the destination upserts on, as a stable dictionary
    /// key -- <see cref="PartitionKey"/> itself is not usable as one.</summary>
    private static string IdentityOf(RowDocument document) => document.PartitionKey + " " + document.Id;

    private async Task SendAsync(RowDocument document, Task? previous, CancellationToken ct)
    {
        try
        {
            if (previous is not null)
            {
                // The earlier request's own outcome is already tracked by its own entry in
                // _pending; swallowing it here only means this later, superseding write for the
                // same identity still gets attempted even when the one before it failed.
                try
                {
                    await previous.ConfigureAwait(false);
                }
                catch
                {
                    // Reported through the earlier task's own result, not here.
                }
            }

            using var body = new MemoryStream(document.Json.ToArray(), writable: false);
            using var response = _upsert
                ? await _container.UpsertItemStreamAsync(body, document.PartitionKey, _requestOptions, ct).ConfigureAwait(false)
                : await _container.CreateItemStreamAsync(body, document.PartitionKey, _requestOptions, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                var what = _upsert ? "upserting" : "creating";
                throw CosmosErrors.FromResponse(response, _redactor, $"output '{_outputName}': {what} document '{document.Id}' in '{_output.Container}'");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not PzConnectorException)
        {
            throw CosmosErrors.Wrap(ex, _redactor, $"output '{_outputName}': writing document '{document.Id}' in '{_output.Container}'");
        }
        finally
        {
            _inFlight.Release();
        }
    }

    /// <summary>Surfaces the first failure among finished requests and forgets the successes, so
    /// a long write neither hides an error until commit nor keeps every task alive. Walks
    /// backwards and removes each completed task the moment it is captured, rather than filtering
    /// then re-testing IsCompleted in a separate RemoveAll pass -- a send that completes in that
    /// gap would otherwise be dropped from _pending without ever being awaited, hiding a fault.</summary>
    private async Task DrainCompletedAsync()
    {
        var finished = new List<Task>();
        for (var i = _pending.Count - 1; i >= 0; i--)
        {
            if (_pending[i].IsCompleted)
            {
                finished.Add(_pending[i]);
                _pending.RemoveAt(i);
            }
        }

        if (_upsert)
        {
            foreach (var key in _lastByIdentity.Where(kv => kv.Value.IsCompleted).Select(kv => kv.Key).ToList())
            {
                _lastByIdentity.Remove(key);
            }
        }

        await Task.WhenAll(finished).ConfigureAwait(false);
    }

    private void ThrowIfFinished()
    {
        if (_committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }
    }
}
