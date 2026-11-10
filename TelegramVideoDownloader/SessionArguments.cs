internal sealed record SessionArguments(int? ChannelNumber, string? Destination, bool ConfirmStart)
{
    public const string Usage = "Usage: TelegramVideoDownloader [--channel-number <n>] [--destination <path>] [--yes]";

    public static SessionArguments Parse(string[] args)
    {
        int? channel = null;
        string? destination = null;
        var yes = false;
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            var option = args[i];
            if (!seen.Add(option))
                throw new ArgumentException($"Option supplied twice: {option}.");

            switch (option.ToLowerInvariant())
            {
                case "--yes":
                    yes = true;
                    break;
                case "--channel-number":
                    var text = ReadValue(args, ref i);
                    if (!int.TryParse(text, out var number) || number < 1)
                        throw new ArgumentException("--channel-number requires a positive integer.");
                    channel = number;
                    break;
                case "--destination":
                    destination = Path.GetFullPath(ReadValue(args, ref i));
                    if (File.Exists(destination))
                        throw new ArgumentException("--destination must name a directory.");
                    break;
                default:
                    throw new ArgumentException($"Unknown option: {option}.");
            }
        }
        return new SessionArguments(channel, destination, yes);
    }

    private static string ReadValue(string[] args, ref int index)
    {
        var option = args[index];
        if (++index == args.Length || string.IsNullOrWhiteSpace(args[index]) ||
            args[index].StartsWith("--", StringComparison.Ordinal))
            throw new ArgumentException($"Missing value for {option}.");
        return args[index];
    }
}
