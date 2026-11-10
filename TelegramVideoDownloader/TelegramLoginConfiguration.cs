using System.Text.Json;

internal static class TelegramLoginConfiguration
{
    private static readonly string CredentialsPath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "TelegramVideoDownloader", "telegram-credentials.json");
    private static readonly Lazy<Dictionary<string, string>> Saved = new(ReadCredentials);

    public static string? Get(string name, Func<string, string> prompt) => name switch
    {
        "api_id" => Required(name, "TELEGRAM_API_ID"),
        "api_hash" => Required(name, "TELEGRAM_API_HASH"),
        "phone_number" => Required(name, "TELEGRAM_PHONE_NUMBER"),
        "verification_code" => prompt("Telegram verification code: "),
        "password" => prompt("Telegram 2FA password: "),
        _ => null
    };

    private static string Required(string name, string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        if (string.IsNullOrWhiteSpace(value)) Saved.Value.TryGetValue(name, out value);
        if (!string.IsNullOrWhiteSpace(value)) return value.Trim();
        throw new InvalidOperationException(
            $"Missing Telegram setting '{name}'. Set {variable} or add it to {CredentialsPath}. See the README for setup.");
    }

    private static Dictionary<string, string> ReadCredentials()
    {
        if (!File.Exists(CredentialsPath)) return new();
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText(CredentialsPath))
                ?? throw new JsonException();
        }
        catch (JsonException)
        {
            throw new InvalidOperationException(
                $"Invalid Telegram configuration at {CredentialsPath}. Expected a JSON object with string values.");
        }
    }
}
