using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

[Collection("cosmosdb")]
public sealed class CosmosStatsTests(CosmosFixture cosmos)
{
    [Fact]
    public void A_response_reports_one_request_its_charge_in_ru_and_its_bytes()
    {
        var acc = new ConnectorStatsAccumulator();
        using var response = new ResponseMessage(HttpStatusCode.OK) { Content = new MemoryStream(new byte[300]) };
        response.Headers.Add("x-ms-request-charge", "2.5");
        response.Headers.Add("x-ms-request-duration-ms", "1.8");
        CosmosStats.Report(response, requestBodyBytes: 120, acc);
        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(2.5, s.CostUnits);
        Assert.Equal("ru", s.CostUnit);
        Assert.Equal(420, s.WireBytes);
        Assert.Equal(1, s.RemoteMs);
    }

    [Fact]
    public void A_response_without_a_charge_or_duration_reports_only_the_request_and_bytes()
    {
        var acc = new ConnectorStatsAccumulator();
        using var response = new ResponseMessage(HttpStatusCode.OK);
        CosmosStats.Report(response, requestBodyBytes: 7, acc);
        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(7, s.WireBytes);
        Assert.Null(s.CostUnits);
        Assert.Equal(0, s.RemoteMs);
    }

    [Fact]
    public void A_429_counts_as_throttled_and_its_charge_still_counts()
    {
        var acc = new ConnectorStatsAccumulator();
        var ex = new CosmosException("throttled", (HttpStatusCode)429, 0, "a", 1.0);
        CosmosStats.ReportThrottle(ex, acc);
        var s = acc.Snapshot();
        Assert.Equal(1, s.Throttled);
        Assert.Equal(1.0, s.CostUnits);
        Assert.Equal(1, s.Requests);
    }

    [Fact]
    public void A_non_throttle_exception_is_not_counted_as_throttled()
    {
        var acc = new ConnectorStatsAccumulator();
        CosmosStats.ReportThrottle(new CosmosException("gone", HttpStatusCode.NotFound, 0, "a", 0.0), acc);
        Assert.Equal(0, acc.Snapshot().Throttled);
    }

    [SkippableFact]
    [Trait("Category", "Docker")]
    public async Task The_cosmos_operation_activity_source_name_is_what_we_opt_in_to()
    {
        DockerFacts.SkipUnlessDocker();
        var seen = new ConcurrentBag<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s =>
            {
                if (s.Name.StartsWith("Azure.Cosmos", StringComparison.Ordinal))
                {
                    seen.Add(s.Name);
                }

                return true;
            },
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        var errors = new List<string>();
        var connection = CosmosConnectionConfig.Parse(new ConnectorConfig(cosmos.ConnectionConfig()), errors)!;
        using var client = CosmosClientFactory.Create(connection);
        await client.ReadAccountAsync();
        await client.GetDatabase(connection.Database).ReadAsync();

        Assert.Contains("Azure.Cosmos.Operation", seen);
    }
}
