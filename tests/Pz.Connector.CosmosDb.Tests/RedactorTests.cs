namespace Pz.Connector.CosmosDb.Tests;

public sealed class RedactorTests
{
    [Fact]
    public void Configured_secrets_are_masked_wherever_they_occur()
    {
        var redactor = new CosmosRedactor(["s3cret-key-value=="]);
        Assert.Equal("key *** rejected (***)", redactor.Redact("key s3cret-key-value== rejected (s3cret-key-value==)"));
    }

    [Fact]
    public void AccountKey_shape_is_masked_even_when_the_value_is_not_ours()
    {
        var text = "AccountEndpoint=https://a.documents.azure.com:443/;AccountKey=abcDEF123+/==;Database=x";
        Assert.Equal("AccountEndpoint=https://a.documents.azure.com:443/;AccountKey=***;Database=x", CosmosRedactor.None.Redact(text));
    }

    [Fact]
    public void Credential_pairs_are_masked()
    {
        Assert.Equal("client_secret=*** and password=***", CosmosRedactor.None.Redact("client_secret=abc.def and password=\"q u\""));
    }

    [Fact]
    public void The_emulator_key_is_always_masked()
    {
        Assert.Equal("key ***", CosmosRedactor.None.Redact("key " + CosmosRedactor.EmulatorKey));
    }

    [Fact]
    public void Short_secrets_are_not_matched()
    {
        Assert.Equal("ab in text", new CosmosRedactor(["ab"]).Redact("ab in text"));
    }
}
