using Pz.Connectors.Abstractions;

namespace Pz.Connector.CosmosDb;

internal static class CosmosErrors
{
    public static PzConnectorException Fatal(string message, CosmosRedactor redactor, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: false, innerException: inner);

    public static PzConnectorException Transient(string message, CosmosRedactor redactor, TimeSpan? retryAfter = null, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: true, retryAfter: retryAfter, innerException: inner);

    /// <summary>The connector prefix goes on after redaction: a secret that happens to be a
    /// substring of "cosmosdb" must not shred the one part of the message that is ours.</summary>
    public static string Message(CosmosRedactor redactor, string text) => "cosmosdb: " + redactor.Redact(text);
}
