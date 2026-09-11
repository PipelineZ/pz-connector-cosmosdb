using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

[Collection("cosmosdb")]
[Trait("Category", "Docker")]
public sealed class CosmosConnectorFacts(CosmosFixture cosmos)
{
    [Fact]
    public async Task Validate_reports_every_config_error_at_once()
    {
        var result = await new CosmosConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "account_key" }), CancellationToken.None);
        Assert.Contains(result.Errors, e => e.Contains("'database' is required", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'endpoint' is required", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'account_key' is required", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Check_connection_reports_the_account()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(cosmos.ConnectionConfig()), CancellationToken.None);
        Assert.True(check.Ok, check.Message);
        Assert.Contains("Cosmos DB account", check.Message);
        Assert.Contains("consistency", check.Message);
    }

    [SkippableFact]
    public async Task Check_connection_names_a_missing_database()
    {
        DockerFacts.SkipUnlessDocker();
        var config = cosmos.ConnectionConfig();
        config["database"] = "no_such_db";
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("no_such_db", check.Message);
        Assert.Contains("PZCS0102", check.Message);
    }

    /// <summary>The emulator's own reported default drives the requested level, rather than assuming
    /// Session: a spike against the pinned vnext-latest image found its default is Eventual, not
    /// Session, so 'strong' (always the strongest level) is requested unless the account itself
    /// already defaults to strong.</summary>
    [SkippableFact]
    public async Task Check_connection_refuses_a_consistency_raise()
    {
        DockerFacts.SkipUnlessDocker();
        var accountDefault = (await cosmos.Client.ReadAccountAsync()).Consistency.DefaultConsistencyLevel;
        var requested = accountDefault == ConsistencyLevel.Strong ? ConsistencyLevel.BoundedStaleness : ConsistencyLevel.Strong;

        var config = cosmos.ConnectionConfig();
        config["consistency"] = ConfigName(requested);
        var check = await new CosmosConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.Contains("PZCS0101", check.Message);
        Assert.Contains(ConfigName(requested), check.Message);
        Assert.Contains(ConfigName(accountDefault), check.Message);
    }

    private static string ConfigName(ConsistencyLevel level) => level switch
    {
        ConsistencyLevel.Eventual => "eventual",
        ConsistencyLevel.ConsistentPrefix => "consistent_prefix",
        ConsistencyLevel.Session => "session",
        ConsistencyLevel.BoundedStaleness => "bounded_staleness",
        ConsistencyLevel.Strong => "strong",
        _ => throw new ArgumentOutOfRangeException(nameof(level), level, "unknown consistency level"),
    };
}
