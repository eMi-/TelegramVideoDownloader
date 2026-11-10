using System.Text.RegularExpressions;
using System.Windows.Forms;
using TL;

internal static partial class FileDiscovery
{
    [GeneratedRegex(@"(?<!\d)\d{5,20}(?!\d)", RegexOptions.CultureInvariant)]
    private static partial Regex NumberTokenRegex();

    public static Dictionary<long, List<FileInfo>> BuildExistingFileMap(string folder)
    {
        var result = new Dictionary<long, List<FileInfo>>();
        foreach (var path in Directory.EnumerateFiles(folder, "*", SearchOption.TopDirectoryOnly))
        {
            var name = Path.GetFileName(path);
            if (name.EndsWith(".part", StringComparison.OrdinalIgnoreCase) ||
                name.EndsWith(".part.state.json", StringComparison.OrdinalIgnoreCase) ||
                name.StartsWith(".telegram-video-downloader-", StringComparison.OrdinalIgnoreCase))
                continue;

            FileInfo info;
            try
            {
                info = new FileInfo(path);
                if (!info.Exists || info.Length <= 0)
                    continue;
            }
            catch
            {
                continue;
            }

            foreach (Match match in NumberTokenRegex().Matches(name))
            {
                if (!long.TryParse(match.Value, out var token))
                    continue;
                if (!result.TryGetValue(token, out var files))
                    result[token] = files = [];
                if (!files.Any(file => string.Equals(
                        file.FullName,
                        info.FullName,
                        StringComparison.OrdinalIgnoreCase)))
                    files.Add(info);
            }
        }

        return result;
    }

    public static bool IsAlreadyDownloaded(
        VideoItem video,
        DownloadIndex index,
        string folder,
        Dictionary<long, List<FileInfo>> existingFiles,
        out bool indexChanged)
    {
        indexChanged = false;
        if (index.Videos.TryGetValue(video.Key, out var entry))
        {
            var indexedPath = Path.Combine(folder, entry.FileName);
            if (IsCompleteFile(indexedPath, video.Size))
                return true;
            index.Videos.TryRemove(video.Key, out _);
            indexChanged = true;
        }

        if (existingFiles.TryGetValue(video.DocumentId, out var candidates))
        {
            foreach (var candidate in candidates)
            {
                try
                {
                    candidate.Refresh();
                    if (!candidate.Exists || !HasExpectedSize(candidate.Length, video.Size))
                        continue;
                    AddExistingFileToIndex(video, candidate.FullName, index);
                    indexChanged = true;
                    return true;
                }
                catch
                {
                }
            }
        }

        foreach (var name in new[] { video.ExpectedFileName, video.LegacyFileName })
        {
            var candidatePath = Path.Combine(folder, name);
            if (!IsCompleteFile(candidatePath, video.Size))
                continue;
            AddExistingFileToIndex(video, candidatePath, index);
            AddFileToMemoryMap(existingFiles, video.DocumentId, candidatePath);
            indexChanged = true;
            return true;
        }

        return false;
    }

    public static bool IsCompleteFile(string path, long expectedSize)
    {
        try
        {
            return File.Exists(path) &&
                   HasExpectedSize(new FileInfo(path).Length, expectedSize);
        }
        catch
        {
            return false;
        }
    }

    public static void MarkCompleted(VideoItem video, string path, DownloadIndex index)
    {
        index.Videos[video.Key] = new DownloadIndexEntry
        {
            DocumentId = video.DocumentId,
            MessageId = video.MessageId,
            Size = video.Size,
            FileName = Path.GetFileName(path),
            DownloadedUtc = DateTime.UtcNow
        };
    }

    private static bool HasExpectedSize(long actualSize, long expectedSize) =>
        expectedSize > 0 ? actualSize == expectedSize : actualSize > 0;

    private static void AddExistingFileToIndex(
        VideoItem video,
        string path,
        DownloadIndex index) => MarkCompleted(video, path, index);

    private static void AddFileToMemoryMap(
        Dictionary<long, List<FileInfo>> map,
        long documentId,
        string path)
    {
        if (!map.TryGetValue(documentId, out var files))
            map[documentId] = files = [];
        if (!files.Any(file => string.Equals(file.FullName, path, StringComparison.OrdinalIgnoreCase)))
            files.Add(new FileInfo(path));
    }
}

internal static class FileNames
{
    public static string GetOriginalFileName(Document document)
    {
        var name = document.Filename;
        if (string.IsNullOrWhiteSpace(name))
            name = $"video_{document.id}{GetExtension(document.mime_type)}";
        return LimitFilenameLength(SanitizeFilename(name), 160);
    }

    public static string BuildFileName(int messageId, long documentId, string originalName) =>
        SanitizeFilename($"{messageId}_{documentId}_{originalName}");

    public static string BuildLegacyFileName(int messageId, string originalName) =>
        SanitizeFilename($"{messageId}_{originalName}");

    public static string BuildVideoKey(long chatId, long documentId, long size) =>
        $"{chatId}:{documentId}:{size}";

    public static string SanitizeFilename(string fileName)
    {
        foreach (var invalid in Path.GetInvalidFileNameChars())
            fileName = fileName.Replace(invalid, '_');
        fileName = fileName.Trim().TrimEnd('.');
        return string.IsNullOrWhiteSpace(fileName) ? "video" : fileName;
    }

    public static string GetNonOverwritingPath(string folder, string fileName)
    {
        var path = Path.Combine(folder, fileName);
        if (!File.Exists(path))
            return path;

        var extension = Path.GetExtension(fileName);
        var name = Path.GetFileNameWithoutExtension(fileName);
        for (var suffix = 1; ; suffix++)
        {
            var candidate = Path.Combine(folder, $"{name}_new{suffix}{extension}");
            if (!File.Exists(candidate))
                return candidate;
        }
    }

    private static string LimitFilenameLength(string fileName, int maximumLength)
    {
        if (fileName.Length <= maximumLength)
            return fileName;
        var extension = Path.GetExtension(fileName);
        var name = Path.GetFileNameWithoutExtension(fileName);
        var allowed = Math.Max(1, maximumLength - extension.Length);
        return name[..Math.Min(name.Length, allowed)] + extension;
    }

    private static string GetExtension(string? mimeType) => mimeType?.ToLowerInvariant() switch
    {
        "video/mp4" => ".mp4",
        "video/webm" => ".webm",
        "video/quicktime" => ".mov",
        "video/x-matroska" => ".mkv",
        "video/mpeg" => ".mpeg",
        "video/x-msvideo" => ".avi",
        _ => ".video"
    };
}

internal static class Ui
{
    private static readonly object ConsoleLock = new();

    public static void Print(string message)
    {
        lock (ConsoleLock)
            Console.WriteLine(message);
    }

    public static void PrintHeader()
    {
        Console.WriteLine("============================================");
        Console.WriteLine("       TELEGRAM VIDEO DOWNLOADER");
        Console.WriteLine("============================================");
        Console.WriteLine();
    }

    public static string GetChatTitle(ChatBase chat) => chat switch
    {
        Channel channel => channel.title,
        Chat basicChat => basicChat.title,
        _ => chat.ToString() ?? "Unknown Chat"
    };

    public static int ReadNumber(string prompt, int minimum, int maximum)
    {
        while (true)
        {
            Console.Write(prompt);
            if (int.TryParse(Console.ReadLine(), out var number) &&
                number >= minimum && number <= maximum)
                return number;
            Console.WriteLine($"Please enter a number between {minimum} and {maximum}.");
        }
    }

    public static string Prompt(string message)
    {
        Console.Write(message);
        return Console.ReadLine() ?? "";
    }

    public static string? ChooseDownloadFolder()
    {
        string? selected = null;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                using var dialog = new FolderBrowserDialog
                {
                    Description = "Select folder for Telegram videos",
                    ShowNewFolderButton = true
                };
                if (dialog.ShowDialog() == DialogResult.OK)
                    selected = dialog.SelectedPath;
            }
            catch (Exception ex)
            {
                error = ex;
            }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        return error is null
            ? selected
            : throw new InvalidOperationException("Could not open folder picker.", error);
    }

    public static async Task ShowStatusLoopAsync(
        TransferStats stats,
        System.Diagnostics.Stopwatch watch,
        Func<string> describeMediaWaits,
        CancellationToken cancellationToken)
    {
        long lastNetworkBytes = 0;
        var lastTime = watch.Elapsed;
        var recentSpeeds = new Queue<double>();
        var lastProgressTime = watch.Elapsed;
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            var networkBytes = Interlocked.Read(ref stats.NetworkBytes);
            var logicalBytes = Interlocked.Read(ref stats.LogicalBytes);
            var totalBytes = Interlocked.Read(ref stats.TotalBytes);
            var now = watch.Elapsed;
            var seconds = Math.Max(0.001, (now - lastTime).TotalSeconds);
            var currentSpeed = Math.Max(0, networkBytes - lastNetworkBytes) / seconds;
            if (networkBytes > lastNetworkBytes) lastProgressTime = now;
            lastNetworkBytes = networkBytes;
            lastTime = now;
            recentSpeeds.Enqueue(currentSpeed);
            while (recentSpeeds.Count > 5)
                recentSpeeds.Dequeue();
            var rollingSpeed = recentSpeeds.Count == 0 ? 0 : recentSpeeds.Average();
            var remaining = Math.Max(0, totalBytes - logicalBytes);
            var eta = FormatEta(remaining, rollingSpeed);
            var idle = now - lastProgressTime;
            var floodRemaining = stats.FloodCooldown.Remaining;
            var dcStatus = describeMediaWaits();
            var percent = totalBytes > 0 ? logicalBytes * 100.0 / totalBytes : 0;
            var completed = Volatile.Read(ref stats.Completed);
            var failed = Volatile.Read(ref stats.Failed);
            var active = Volatile.Read(ref stats.Active);
            var totalVideos = Volatile.Read(ref stats.TotalVideos);
            var queued = Math.Max(0, totalVideos - completed - failed - active);

            lock (ConsoleLock)
            {
                Console.WriteLine();
                Console.WriteLine(
                    $"[STATUS] {completed}/{totalVideos} downloaded | {active} active | {queued} queued | " +
                    $"{Volatile.Read(ref stats.AlreadyDownloaded)} already | " +
                    $"{Volatile.Read(ref stats.DuplicateReferences)} duplicates | {failed} failed | " +
                    $"parts {Volatile.Read(ref stats.ActivePartRequests)}/" +
                    $"{Volatile.Read(ref stats.ActiveTransferLimit)} | " +
                    $"waiting for DC {Volatile.Read(ref stats.WaitingPartRequests)} | " +
                    $"part retries {Volatile.Read(ref stats.PartRetries)} | " +
                    $"DC reconnects {Volatile.Read(ref stats.DcReconnects)} | " +
                    $"ref refreshes {Volatile.Read(ref stats.FileReferenceRefreshes)} | " +
                    $"{percent:0.00}% | " +
                    $"{FormatBytes(logicalBytes)} / {FormatBytes(totalBytes)} | " +
                    $"NOW {FormatBytes((long)currentSpeed)}/s | AVG15 {FormatBytes((long)rollingSpeed)}/s | " +
                    $"ETA {eta}" +
                    (floodRemaining > TimeSpan.Zero
                        ? $" | TELEGRAM WAIT {FormatDuration(floodRemaining)} (resumes automatically)"
                        : dcStatus.Length > 0
                        ? $" | {dcStatus}"
                        : active > 0 && idle.TotalSeconds >= 15
                        ? $" | no data {FormatDuration(idle)} (automatic recovery enabled)" : ""));
            }
        }
    }

    internal static string FormatEta(long remainingBytes, double speed) =>
        remainingBytes <= 0 ? "00:00:00" :
        speed <= 0 ? "--:--:--" :
        FormatDuration(TimeSpan.FromSeconds(Math.Min(
            remainingBytes / speed, TimeSpan.MaxValue.TotalSeconds / 2)));

    public static void PrintDownloadPlan(
        ScanResult scan,
        int catalogReferences,
        long catalogBytes,
        int uniqueVideos,
        TransferStats stats)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine();
            Console.WriteLine("============================================");
            Console.WriteLine("SCAN COMPLETE / DOWNLOAD PLAN");
            Console.WriteLine("============================================");
            Console.WriteLine();
            Console.WriteLine($"Scan type           : {(scan.WasFullScan ? "server-filtered full" : "incremental")}");
            Console.WriteLine($"Messages inspected  : {scan.MessagesScanned:N0}");
            Console.WriteLine($"Video references    : {catalogReferences:N0}");
            Console.WriteLine($"Channel video size  : {FormatBytes(catalogBytes)}");
            Console.WriteLine($"Unique videos       : {uniqueVideos:N0}");
            Console.WriteLine($"Already downloaded  : {Volatile.Read(ref stats.AlreadyDownloaded):N0}");
            Console.WriteLine($"Duplicate references: {Volatile.Read(ref stats.DuplicateReferences):N0}");
            Console.WriteLine($"Need downloading    : {Volatile.Read(ref stats.TotalVideos):N0}");
            Console.WriteLine($"Remaining data      : {FormatBytes(Interlocked.Read(ref stats.TotalBytes))}");
            Console.WriteLine($"Completed during scan: {Volatile.Read(ref stats.Completed):N0}");
            Console.WriteLine($"Currently active    : {Volatile.Read(ref stats.Active):N0}");
            Console.WriteLine();
            Console.WriteLine("Downloads continue while this summary is displayed.");
        }
    }

    public static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB", "TB"];
        double size = Math.Max(0, bytes);
        var suffix = 0;
        while (size >= 1024 && suffix < suffixes.Length - 1)
        {
            size /= 1024;
            suffix++;
        }
        return $"{size:0.##} {suffixes[suffix]}";
    }

    public static string FormatDuration(TimeSpan duration)
    {
        if (duration < TimeSpan.Zero)
            duration = TimeSpan.Zero;
        return duration.TotalDays >= 1
            ? $"{(int)duration.TotalDays}d {duration.Hours:00}:{duration.Minutes:00}:{duration.Seconds:00}"
            : $"{(int)duration.TotalHours:00}:{duration.Minutes:00}:{duration.Seconds:00}";
    }
}
