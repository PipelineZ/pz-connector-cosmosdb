using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class ConnectionConfigTests
{
    private static CosmosConnectionConfig? Parse(Dictionary<string, object?> values, out List<string> errors)
    {
        errors = [];
        return CosmosConnectionConfig.Parse(new ConnectorConfig(values), errors);
    }

    [Fact]
    public void Account_key_needs_endpoint_and_key()
    {
        var config = Parse(new() { ["database"] = "shop", ["auth"] = "account_key", ["endpoint"] = "https://a.documents.azure.com:443/", ["account_key"] = "k3y-value-long" }, out var errors);
        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal("shop", config.Database);
        Assert.Equal(ConnectionMode.Direct, config.ConnectionMode);
        Assert.Null(config.Consistency);
        Assert.Equal(CosmosConnectionConfig.DefaultTimeoutSeconds, config.TimeoutSeconds);
        Assert.Equal("***", config.Redactor.Redact("k3y-value-long"));
    }

    [Fact]
    public void Missing_required_fields_are_all_reported()
    {
        Assert.Null(Parse(new() { ["auth"] = "service_principal" }, out var errors));
        Assert.Contains(errors, e => e.Contains("'database' is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'endpoint'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'tenant_id'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'client_id'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'client_secret'", StringComparison.Ordinal));
    }

    [Fact]
    public void Unknown_auth_names_the_allowed_set()
    {
        Assert.Null(Parse(new() { ["database"] = "d", ["auth"] = "magic" }, out var errors));
        Assert.Contains(errors, e => e.Contains("connection_string, account_key, service_principal, credential_chain, managed_identity", StringComparison.Ordinal));
    }

    [Fact]
    public void Connection_string_is_a_secret_and_needs_no_endpoint()
    {
        var cs = "AccountEndpoint=https://a.documents.azure.com:443/;AccountKey=abc123def456==;";
        var config = Parse(new() { ["database"] = "d", ["auth"] = "connection_string", ["connection_string"] = cs }, out var errors);
        Assert.Empty(errors);
        Assert.Equal("cosmosdb: *** failed", CosmosErrors.Message(config!.Redactor, cs + " failed"));
    }

    [Fact]
    public void Connection_mode_consistency_and_timeout_parse()
    {
        var config = Parse(new()
        {
            ["database"] = "d", ["auth"] = "credential_chain", ["endpoint"] = "https://a.documents.azure.com:443/",
            ["connection_mode"] = "gateway", ["consistency"] = "bounded_staleness", ["timeout"] = 45.0,
        }, out var errors);
        Assert.Empty(errors);
        Assert.Equal(ConnectionMode.Gateway, config!.ConnectionMode);
        Assert.Equal(ConsistencyLevel.BoundedStaleness, config.Consistency);
        Assert.Equal(45, config.TimeoutSeconds);
    }

    [Fact]
    public void Bad_enum_values_and_out_of_range_timeout_are_reported()
    {
        Assert.Null(Parse(new()
        {
            ["database"] = "d", ["auth"] = "managed_identity", ["endpoint"] = "https://a.documents.azure.com:443/",
            ["connection_mode"] = "fast", ["consistency"] = "sometimes", ["timeout"] = 0,
        }, out var errors));
        Assert.Contains(errors, e => e.Contains("'connection_mode' must be one of direct, gateway", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'consistency' must be one of", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'timeout' must be an integer between 1 and 600", StringComparison.Ordinal));
    }

    [Fact]
    public void Malformed_endpoint_and_unknown_keys_are_reported()
    {
        Assert.Null(Parse(new() { ["database"] = "d", ["auth"] = "credential_chain", ["endpoint"] = "not a url", ["colour"] = "blue" }, out var errors));
        Assert.Contains(errors, e => e.Contains("'endpoint' is not an absolute http(s) URL", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.StartsWith("unknown connection key 'colour'", StringComparison.Ordinal));
    }
}
