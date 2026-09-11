using System.Text.RegularExpressions;

namespace Pz.Connector.CosmosDb;

/// <summary>Strips credentials from any text that may reach a PzConnectorException message, a log
/// line, or a ConnectionCheck: every configured secret value is replaced wherever it occurs (the SDK
/// echoes request details into its messages), the <c>AccountKey=</c> connection-string shape and
/// <c>key=value</c> credential pairs are rewritten even when the value is not one of ours, and the
/// emulator's well-known key is always masked. Secrets shorter than 3 characters are not matched;
/// replacing them would shred unrelated text.</summary>
internal sealed partial class CosmosRedactor
{
    public const string Mask = "***";

    /// <summary>The Azure Cosmos DB emulator's published, well-known account key.</summary>
    public const string EmulatorKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    public static readonly CosmosRedactor None = new([]);

    private readonly string[] _secrets;

    public CosmosRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Append(EmulatorKey).Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
            var escaped = Uri.EscapeDataString(secret);
            if (!string.Equals(escaped, secret, StringComparison.Ordinal))
            {
                text = text.Replace(escaped, Mask, StringComparison.Ordinal);
            }
        }

        text = AccountKey().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
        return CredentialPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    // AccountKey=<base64> inside a connection string; the value runs to the next ';' or whitespace.
    [GeneratedRegex("""(?<key>\bAccountKey)=[^;\s]+""", RegexOptions.IgnoreCase)]
    private static partial Regex AccountKey();

    // key=value / key="quoted value" for the credential names the config and Azure print.
    [GeneratedRegex("""(?<key>\b(?:password|passwd|pwd|client_secret|account_key|access_key|secret_key)\b)=(?:"[^"]*"|[^\s;,&]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex CredentialPair();
}
