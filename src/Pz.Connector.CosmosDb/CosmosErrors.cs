using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Turns SDK outcomes into the engine's exception, classified for retry by HTTP status.
/// Transient = the service or the network may recover on its own: throttling (429, with the
/// service's own retry-after), retry-with (449), unavailable (503), request timeout (408), a
/// partition split or migration (410), and any transport failure. Credentials, authorization,
/// missing databases/containers, malformed queries, id conflicts and oversized documents are not.
/// Unmapped statuses are non-transient: an unknown failure retried is a failure hidden. Messages
/// always pass the redactor.</summary>
internal static class CosmosErrors
{
    private static readonly HashSet<HttpStatusCode> TransientStatuses =
    [
        HttpStatusCode.TooManyRequests, (HttpStatusCode)449, HttpStatusCode.ServiceUnavailable,
        HttpStatusCode.RequestTimeout, HttpStatusCode.Gone,
    ];

    public static bool IsTransientStatus(HttpStatusCode status) => TransientStatuses.Contains(status);

    /// <summary>Classifies and wraps any exception raised by an SDK call. Cancellation is not a
    /// failure and is never wrapped; an exception that is already ours passes through.</summary>
    public static Exception Wrap(Exception ex, CosmosRedactor redactor, string context)
    {
        switch (ex)
        {
            case OperationCanceledException:
            case PzConnectorException:
                return ex;
            case CosmosException cosmos:
                return FromStatus(cosmos.StatusCode, cosmos.SubStatusCode.ToString(CultureInfo.InvariantCulture), FirstLine(cosmos.Message),
                    cosmos.RetryAfter, redactor, context, ex);
            case HttpRequestException or SocketException or IOException or TimeoutException:
                return Transient($"{context}: {ex.Message}", redactor, null, ex);
            default:
                return Fatal($"{context}: {ex.Message}", redactor, ex);
        }
    }

    /// <summary>A stream API response that is not a success: same classification as an exception,
    /// with the service's retry-after header when it sent one.</summary>
    public static PzConnectorException FromResponse(ResponseMessage response, CosmosRedactor redactor, string context) =>
        FromStatus(response.StatusCode, response.Headers.GetHeaderValue<string>("x-ms-substatus"), FirstLine(response.ErrorMessage),
            RetryAfterOf(response), redactor, context);

    public static TimeSpan? RetryAfterOf(ResponseMessage response)
    {
        var text = response.Headers.GetHeaderValue<string>("x-ms-retry-after-ms");
        return long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms) && ms > 0
            ? TimeSpan.FromMilliseconds(ms)
            : null;
    }

    /// <summary>The one message shape for a service error:
    /// <c>cosmosdb: &lt;context&gt;: &lt;message&gt; (&lt;status&gt;[ substatus n][; hint])</c>.</summary>
    public static PzConnectorException FromStatus(HttpStatusCode status, string? subStatus, string? message, TimeSpan? retryAfter,
        CosmosRedactor redactor, string context, Exception? original = null)
    {
        var hint = (int)status switch
        {
            401 or 403 => "; check 'auth' and the credential's Cosmos DB data-plane role",
            404 => "; check 'database' and the container name",
            409 => "; a document with this id already exists in the partition -- use merge, or drop the duplicate",
            413 => "; the document is larger than the 2 MB Cosmos DB limit",
            429 => "; the container's provisioned throughput was exceeded",
            _ => "",
        };
        var sub = string.IsNullOrEmpty(subStatus) || subStatus == "0" ? "" : $" substatus {subStatus}";
        var text = $"{context}: {message ?? "no message"} ({(int)status}{sub}{hint})";
        return IsTransientStatus(status) ? Transient(text, redactor, retryAfter, original) : Fatal(text, redactor, original);
    }

    public static PzConnectorException Fatal(string message, CosmosRedactor redactor, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: false, innerException: inner);

    public static PzConnectorException Transient(string message, CosmosRedactor redactor, TimeSpan? retryAfter = null, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: true, retryAfter: retryAfter, innerException: inner);

    /// <summary>The connector prefix goes on after redaction: a secret that happens to be a
    /// substring of "cosmosdb" must not shred the one part of the message that is ours.</summary>
    public static string Message(CosmosRedactor redactor, string text) => "cosmosdb: " + redactor.Redact(text);

    /// <summary>The SDK appends multi-line diagnostics to every message; the first line is the reason.</summary>
    private static string? FirstLine(string? message)
    {
        if (message is null)
        {
            return null;
        }

        var end = message.IndexOfAny(['\r', '\n']);
        return end < 0 ? message : message[..end];
    }
}
