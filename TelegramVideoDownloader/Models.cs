using System.Collections.Concurrent;
using TL;

internal static class AppConstants
{
    public const string IndexFileName = ".telegram-video-downloader-index.json";
    public const string CatalogFileName = ".telegram-video-downloader-catalog.json";
    public const string SettingsFileName = ".telegram-video-downloader-settings.json";
    public const int FloodRetryThresholdSeconds = 120;
    public const int MaxDownloadRetries = 3;
    public const int MaximumTransferOperations = 16;
    // Persist at most every 64 MB. Frequent forced disk flushes can otherwise
    // starve the network workers on large, fast downloads.
    public const int PartialCheckpointChunks = 128;

    public static readonly TimeSpan LoginTimeout = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan TelegramRequestTimeout = TimeSpan.FromSeconds(90);
    public static readonly TimeSpan PartRequestTimeout = TimeSpan.FromSeconds(45);
    public static readonly TimeSpan FullScanInterval = TimeSpan.FromDays(7);
    public static readonly TimeSpan PartialCheckpointInterval = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan BatchedSaveInterval = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan AutoTuneSampleInterval = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan FileReferenceMaximumAge = TimeSpan.FromMinutes(30);
}

internal sealed record AppPaths(string Folder)
{
    public string IndexPath => Path.Combine(Folder, AppConstants.IndexFileName);
    public string CatalogPath => Path.Combine(Folder, AppConstants.CatalogFileName);
    public string SettingsPath => Path.Combine(Folder, AppConstants.SettingsFileName);
}

internal sealed class PerformanceSettings
{
    public string ProfileName { get; set; } = "Auto";
    public bool AutoTune { get; set; } = true;
    public int ParallelTransfers { get; set; } = 8;
    public int PartSizeKilobytes { get; set; } = 512;
    public int MaxConcurrentVideos { get; set; } = 4;
    public double LastSessionAverageBytesPerSecond { get; set; }
    public DateTime LastUpdatedUtc { get; set; }

    public int PartSizeBytes => PartSizeKilobytes * 1024;

    public static PerformanceSettings CreateDefault() => new();

    public void Normalize()
    {
        ParallelTransfers = Math.Clamp(ParallelTransfers, 1, AppConstants.MaximumTransferOperations);
        MaxConcurrentVideos = Math.Clamp(MaxConcurrentVideos, 1, 8);
        PartSizeKilobytes = PartSizeKilobytes switch
        {
            <= 256 => 256,
            <= 512 => 512,
            _ => 1024
        };
    }

    public void ApplyEnvironmentOverrides()
    {
        ApplyNamedProfile(Environment.GetEnvironmentVariable("TVD_PROFILE"));
        if (int.TryParse(Environment.GetEnvironmentVariable("TVD_TRANSFERS"), out var transfers))
            ParallelTransfers = transfers;
        if (int.TryParse(Environment.GetEnvironmentVariable("TVD_PART_SIZE_KB"), out var partSize))
            PartSizeKilobytes = partSize;
        if (int.TryParse(Environment.GetEnvironmentVariable("TVD_VIDEO_SLOTS"), out var slots))
            MaxConcurrentVideos = slots;
        if (bool.TryParse(Environment.GetEnvironmentVariable("TVD_AUTOTUNE"), out var autoTune))
            AutoTune = autoTune;
    }

    private void ApplyNamedProfile(string? profile)
    {
        switch (profile?.Trim().ToLowerInvariant())
        {
            case "auto":
                ProfileName = "Auto";
                AutoTune = true;
                ParallelTransfers = 8;
                PartSizeKilobytes = 512;
                MaxConcurrentVideos = 4;
                break;
            case "conservative":
                ProfileName = "Conservative";
                AutoTune = false;
                ParallelTransfers = 4;
                PartSizeKilobytes = 512;
                MaxConcurrentVideos = 2;
                break;
            case "balanced":
                ProfileName = "Balanced";
                AutoTune = false;
                ParallelTransfers = 8;
                PartSizeKilobytes = 512;
                MaxConcurrentVideos = 3;
                break;
            case "fast":
                ProfileName = "Fast";
                AutoTune = false;
                ParallelTransfers = 12;
                PartSizeKilobytes = 1024;
                MaxConcurrentVideos = 4;
                break;
            case "experimental":
                ProfileName = "Experimental";
                AutoTune = false;
                ParallelTransfers = 16;
                PartSizeKilobytes = 1024;
                MaxConcurrentVideos = 4;
                break;
        }
    }
}

internal sealed class CatalogStore
{
    public int Version { get; set; } = 1;
    public ConcurrentDictionary<string, ChannelCatalog> Channels { get; set; } = new();
}

internal sealed class ChannelCatalog
{
    public long ChatId { get; set; }
    public string ChatTitle { get; set; } = "";
    public int LastScannedMessageId { get; set; }
    public DateTime LastFullScanUtc { get; set; }
    public DateTime LastIncrementalScanUtc { get; set; }
    public ConcurrentDictionary<string, CatalogVideoEntry> Videos { get; set; } = new();
}

internal sealed class CatalogVideoEntry
{
    public int MessageId { get; set; }
    public long DocumentId { get; set; }
    public long AccessHash { get; set; }
    public string FileReferenceBase64 { get; set; } = "";
    public int DcId { get; set; }
    public long Size { get; set; }
    public string MimeType { get; set; } = "video/mp4";
    public string OriginalFileName { get; set; } = "";
    public DateTime ReferenceUpdatedUtc { get; set; }

    public static CatalogVideoEntry FromDocument(int messageId, Document document)
    {
        return new CatalogVideoEntry
        {
            MessageId = messageId,
            DocumentId = document.id,
            AccessHash = document.access_hash,
            FileReferenceBase64 = Convert.ToBase64String(document.file_reference ?? []),
            DcId = document.dc_id,
            Size = document.size,
            MimeType = document.mime_type ?? "video/mp4",
            OriginalFileName = FileNames.GetOriginalFileName(document),
            ReferenceUpdatedUtc = DateTime.UtcNow
        };
    }
}

internal sealed class VideoItem
{
    private readonly object _locationLock = new();
    private int _locationVersion;
    private DateTime _referenceUpdatedUtc;

    public required string Key { get; init; }
    public required long ChatId { get; init; }
    public required int MessageId { get; init; }
    public required long DocumentId { get; init; }
    public required long Size { get; init; }
    public required string MimeType { get; init; }
    public required string OriginalFileName { get; init; }
    public required string ExpectedFileName { get; init; }
    public required string LegacyFileName { get; init; }
    public long AccessHash { get; private set; }
    public byte[] FileReference { get; private set; } = [];
    public int DcId { get; private set; }
    public string? FinalPath { get; set; }
    public long ProgressBytes;
    public int ResumeReported;
    public SemaphoreSlim ReferenceRefreshLock { get; } = new(1, 1);

    public static VideoItem FromDocument(long chatId, int messageId, Document document)
    {
        var originalName = FileNames.GetOriginalFileName(document);
        return new VideoItem
        {
            Key = FileNames.BuildVideoKey(chatId, document.id, document.size),
            ChatId = chatId,
            MessageId = messageId,
            DocumentId = document.id,
            Size = document.size,
            MimeType = document.mime_type ?? "video/mp4",
            OriginalFileName = originalName,
            ExpectedFileName = FileNames.BuildFileName(messageId, document.id, originalName),
            LegacyFileName = FileNames.BuildLegacyFileName(messageId, originalName),
            AccessHash = document.access_hash,
            FileReference = document.file_reference ?? [],
            DcId = document.dc_id,
            _referenceUpdatedUtc = DateTime.UtcNow
        };
    }

    public static VideoItem FromCatalog(long chatId, CatalogVideoEntry entry)
    {
        var originalName = FileNames.SanitizeFilename(entry.OriginalFileName);
        return new VideoItem
        {
            Key = FileNames.BuildVideoKey(chatId, entry.DocumentId, entry.Size),
            ChatId = chatId,
            MessageId = entry.MessageId,
            DocumentId = entry.DocumentId,
            Size = entry.Size,
            MimeType = entry.MimeType,
            OriginalFileName = originalName,
            ExpectedFileName = FileNames.BuildFileName(entry.MessageId, entry.DocumentId, originalName),
            LegacyFileName = FileNames.BuildLegacyFileName(entry.MessageId, originalName),
            AccessHash = entry.AccessHash,
            FileReference = DecodeReference(entry.FileReferenceBase64),
            DcId = entry.DcId,
            _referenceUpdatedUtc = entry.ReferenceUpdatedUtc
        };
    }

    public VideoFileLocationSnapshot CreateLocationSnapshot()
    {
        lock (_locationLock)
        {
            return new VideoFileLocationSnapshot(
                new InputDocumentFileLocation
                {
                    id = DocumentId,
                    access_hash = AccessHash,
                    file_reference = [.. FileReference],
                    thumb_size = ""
                },
                DcId,
                _locationVersion);
        }
    }

    public int GetLocationVersion()
    {
        lock (_locationLock)
            return _locationVersion;
    }

    public bool IsReferenceStale(DateTime utcNow, TimeSpan maximumAge)
    {
        lock (_locationLock)
        {
            return _referenceUpdatedUtc == default ||
                   utcNow - _referenceUpdatedUtc >= maximumAge;
        }
    }

    public void MarkReferenceStale()
    {
        lock (_locationLock)
            _referenceUpdatedUtc = default;
    }

    public bool MarkReferenceStaleIfVersion(int observedVersion)
    {
        lock (_locationLock)
        {
            if (_locationVersion != observedVersion)
                return false;
            _referenceUpdatedUtc = default;
            return true;
        }
    }

    public void RefreshLocation(Document document)
    {
        lock (_locationLock)
        {
            AccessHash = document.access_hash;
            FileReference = document.file_reference ?? [];
            DcId = document.dc_id;
            _referenceUpdatedUtc = DateTime.UtcNow;
            _locationVersion++;
        }
    }

    private static byte[] DecodeReference(string base64)
    {
        try
        {
            return string.IsNullOrWhiteSpace(base64)
                ? []
                : Convert.FromBase64String(base64);
        }
        catch (FormatException)
        {
            return [];
        }
    }
}

internal readonly record struct VideoFileLocationSnapshot(
    InputDocumentFileLocation Location,
    int DcId,
    int Version);

internal sealed class ScanResult
{
    public bool WasFullScan { get; init; }
    public int MessagesScanned { get; set; }
    public int VideosFound { get; set; }
    public long TotalVideoBytes { get; set; }
}

internal sealed class TransferStats
{
    public long NetworkBytes;
    public long LogicalBytes;
    public long TotalBytes;
    public long ResumedBytes;
    public long FloodWaitSeconds;
    public readonly FloodWaitCoordinator FloodCooldown = new();
    public int FloodWaitEvents;
    public int ShortFloodWaitEvents;
    public long ShortFloodWaitSeconds;
    public int WaitingPartRequests;
    public int TotalVideos;
    public int DiscoveredReferences;
    public int DuplicateReferences;
    public int AlreadyDownloaded;
    public int Completed;
    public int Failed;
    public int Active;
    public int Retries;
    public int Timeouts;
    public int ActiveTransferLimit;
    public int ActivePartRequests;
    public int PartRetries;
    public int RpcTimeouts;
    public int DcReconnects;
    public int FileReferenceRefreshes;
}

internal sealed class DownloadIndex
{
    public ConcurrentDictionary<string, DownloadIndexEntry> Videos { get; set; } = new();
}

internal sealed class DownloadIndexEntry
{
    public long DocumentId { get; set; }
    public int MessageId { get; set; }
    public long Size { get; set; }
    public string FileName { get; set; } = "";
    public DateTime DownloadedUtc { get; set; }
}

internal sealed class PartialDownloadState
{
    public int Version { get; set; } = 1;
    public string VideoKey { get; set; } = "";
    public long ExpectedSize { get; set; }
    public int PartSize { get; set; }
    public long ContiguousBytes { get; set; }
    public DateTime UpdatedUtc { get; set; }
}
