using System.Globalization;
using System.Net;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

/// <summary>Turns what Cosmos DB reports about a request into connector stats. Everything here is
/// read off a response or exception the SDK already returned -- nothing is fetched for telemetry.
/// The cost is the request charge in request units (<c>ru</c>), which the service meters and bills;
/// <c>remoteMs</c> is the service's own <c>x-ms-request-duration-ms</c>, whole milliseconds, only
/// when it sent one. A caller on a continuation thread must pass the handle it captured when the
/// operation started: <see cref="ConnectorStats.Current"/> is not reliable there.
/// The SDK retries a 429 internally (up to the sink's <c>rate_limit_retries</c> budget) and those
/// attempts never reach this code, so <c>requests</c> and <c>throttled</c> count only what the SDK
/// surfaced: both are lower bounds on a throttled container.</summary>
internal static class CosmosStats
{
    private const string DurationHeader = "x-ms-request-duration-ms";
    private const string ItemCountHeader = "x-ms-item-count";

    /// <summary>One request answered: its charge, the service-side time, the bytes both ways, the
    /// page's document count when it is a read page, and a throttle when the answer is a 429.
    /// <paramref name="requestBodyBytes"/> is the payload the caller sent (0 for a query).</summary>
    public static void Report(ResponseMessage response, long requestBodyBytes, IConnectorStats stats)
    {
        stats.AddRequests(1);
        var charge = response.Headers.RequestCharge;
        if (charge > 0)
        {
            stats.AddCost(charge, "ru");
        }

        var bytes = requestBodyBytes + (response.Content is { CanSeek: true } body ? body.Length : 0);
        if (bytes > 0)
        {
            stats.AddWireBytes(bytes);
        }

        if (TryParseHeader(response.Headers.GetHeaderValue<string>(DurationHeader), out var ms))
        {
            stats.AddRemoteMs((long)ms);
        }

        if (TryParseHeader(response.Headers.GetHeaderValue<string>(ItemCountHeader), out var items))
        {
            stats.AddExtra("cosmosdb.documents", items);
        }

        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            stats.AddThrottled(1);
        }
    }

    /// <summary>A request that surfaced as an exception: it still counts, and a 429 still carries
    /// the charge the service billed for it.</summary>
    public static void ReportThrottle(CosmosException ex, IConnectorStats stats)
    {
        stats.AddRequests(1);
        if (ex.StatusCode == HttpStatusCode.TooManyRequests)
        {
            stats.AddThrottled(1);
        }

        if (ex.RequestCharge > 0)
        {
            stats.AddCost(ex.RequestCharge, "ru");
        }
    }

    /// <summary>A typed metadata response (container read): one request and its charge.</summary>
    public static void ReportCharge(double requestCharge, IConnectorStats stats)
    {
        stats.AddRequests(1);
        if (requestCharge > 0)
        {
            stats.AddCost(requestCharge, "ru");
        }
    }

    private static bool TryParseHeader(string? text, out double value) =>
        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value) && value >= 0 && double.IsFinite(value);
}
