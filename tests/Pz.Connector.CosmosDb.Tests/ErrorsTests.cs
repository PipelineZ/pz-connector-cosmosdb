using System.Net;
using System.Net.Sockets;
using Microsoft.Azure.Cosmos;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb.Tests;

public sealed class ErrorsTests
{
    private static readonly CosmosRedactor Redactor = new(["top-secret-key"]);

    [Theory]
    [InlineData(429, true)]
    [InlineData(449, true)]
    [InlineData(503, true)]
    [InlineData(408, true)]
    [InlineData(410, true)]
    [InlineData(401, false)]
    [InlineData(403, false)]
    [InlineData(404, false)]
    [InlineData(400, false)]
    [InlineData(409, false)]
    [InlineData(413, false)]
    [InlineData(500, false)]
    public void Status_codes_classify(int status, bool transient)
    {
        var ex = CosmosErrors.FromStatus((HttpStatusCode)status, "0", "boom", null, Redactor, "ctx");
        Assert.Equal(transient, ex.IsTransient);
        Assert.StartsWith("cosmosdb: ctx: ", ex.Message);
        Assert.Contains($"({status}", ex.Message);
    }

    [Fact]
    public void Retry_after_is_carried_on_429()
    {
        var ex = CosmosErrors.FromStatus(HttpStatusCode.TooManyRequests, "3200", "throttled", TimeSpan.FromMilliseconds(250), Redactor, "ctx");
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromMilliseconds(250), ex.RetryAfter);
        Assert.Contains("substatus 3200", ex.Message);
    }

    [Fact]
    public void Auth_failures_name_the_auth_field()
    {
        var ex = CosmosErrors.FromStatus(HttpStatusCode.Unauthorized, null, "bad sig top-secret-key", null, Redactor, "ctx");
        Assert.False(ex.IsTransient);
        Assert.Contains("check 'auth'", ex.Message);
        Assert.DoesNotContain("top-secret-key", ex.Message);
    }

    [Fact]
    public void Transport_failures_are_transient()
    {
        Assert.True(((PzConnectorException)CosmosErrors.Wrap(new HttpRequestException("reset"), Redactor, "ctx")).IsTransient);
        Assert.True(((PzConnectorException)CosmosErrors.Wrap(new SocketException(), Redactor, "ctx")).IsTransient);
        Assert.True(((PzConnectorException)CosmosErrors.Wrap(new IOException("eof"), Redactor, "ctx")).IsTransient);
    }

    [Fact]
    public void Cosmos_exception_classifies_by_status()
    {
        var wrapped = (PzConnectorException)CosmosErrors.Wrap(new CosmosException("too many", HttpStatusCode.TooManyRequests, 3200, "act", 1.0), Redactor, "ctx");
        Assert.True(wrapped.IsTransient);
        var fatal = (PzConnectorException)CosmosErrors.Wrap(new CosmosException("nope", HttpStatusCode.BadRequest, 0, "act", 1.0), Redactor, "ctx");
        Assert.False(fatal.IsTransient);
    }

    [Fact]
    public void Cancellation_and_our_own_exceptions_pass_through()
    {
        var oce = new OperationCanceledException();
        Assert.Same(oce, CosmosErrors.Wrap(oce, Redactor, "ctx"));
        var own = CosmosErrors.Fatal("x", Redactor);
        Assert.Same(own, CosmosErrors.Wrap(own, Redactor, "ctx"));
    }

    [Fact]
    public void Unknown_exceptions_are_fatal_and_redacted()
    {
        var ex = (PzConnectorException)CosmosErrors.Wrap(new InvalidOperationException("uses top-secret-key"), Redactor, "ctx");
        Assert.False(ex.IsTransient);
        Assert.Equal("cosmosdb: ctx: uses ***", ex.Message);
    }

    [Fact]
    public void A_consistency_raise_is_reported_as_PZCS0101()
    {
        var raise = new ArgumentException(
            "ConsistencyLevel Strong specified in the request is invalid when service is configured with consistency level Session. "
            + "Ensure the request consistency level is not stronger than the service consistency level.");
        var ex = (PzConnectorException)CosmosErrors.Wrap(raise, Redactor, "ctx");
        Assert.False(ex.IsTransient);
        Assert.StartsWith("cosmosdb: PZCS0101", ex.Message);
    }
}
