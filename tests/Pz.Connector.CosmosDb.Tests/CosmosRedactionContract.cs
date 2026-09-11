using Pz.Connectors.TestKit;

namespace Pz.Connector.CosmosDb.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the account key.</summary>
public sealed class CosmosRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new CosmosRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
