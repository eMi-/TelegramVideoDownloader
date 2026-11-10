using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Win32.SafeHandles;
using TL;
using WTelegram;

internal sealed class DownloadEngine : IDisposable
{
    private readonly Client _client;
    private readonly ChatBase _chat;
    private readonly ChannelCatalog _catalog;
    private readonly DownloadIndex _index;
    private readonly BatchedSaveCoordinator _indexSaver;
    private readonly BatchedSaveCoordinator _catalogSaver;
    private readonly ResizableTransferGate _transferGate;
    private readonly MediaDcClientProvider _mediaDcClients;
    private readonly PartRequestPolicy _partRequestPolicy;
    private readonly PerformanceSettings _settings;
    private readonly TransferStats _stats;
    private readonly object _pressureLock = new();
    private long _lastPressureReductionTimestamp;
    private long _lastPressureIncreaseTimestamp;
    private int _healthyParts;
    private int _recoveryTransferLimit;

    public DownloadEngine(
        Client client,
        ChatBase chat,
        ChannelCatalog catalog,
        DownloadIndex index,
        BatchedSaveCoordinator indexSaver,
        BatchedSaveCoordinator catalogSaver,
        ResizableTransferGate transferGate,
        PerformanceSettings settings,
        TransferStats stats)
    {
        _client = client;
        _chat = chat;
        _catalog = catalog;
        _index = index;
        _indexSaver = indexSaver;
        _catalogSaver = catalogSaver;
        _transferGate = transferGate;
        _mediaDcClients = new MediaDcClientProvider(client);
        _partRequestPolicy = new PartRequestPolicy(stats, transferGate, RegisterFloodWaitAsync);
        _settings = settings;
        _stats = stats;
        _recoveryTransferLimit = settings.ParallelTransfers;
    }

    public async ValueTask DownloadAsync(VideoItem video, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _stats.Active);
        var watch = Stopwatch.StartNew();
        var retryCount = 0;

        try
        {
            for (var attempt = 1; ; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var finalPath = video.FinalPath ?? throw new InvalidOperationException("Final path missing.");
                Ui.Print($"[START] {Path.GetFileName(finalPath)} ({Ui.FormatBytes(video.Size)})");

                try
                {
                    await RefreshStaleFileReferenceAsync(video, cancellationToken);
                    await DownloadToPartialAsync(video, finalPath, cancellationToken);
                    var actualPath = FinalizeDownload(video, finalPath);
                    FileDiscovery.MarkCompleted(video, actualPath, _index);
                    _indexSaver.MarkDirty(forceSoon: true);
                    Interlocked.Increment(ref _stats.Completed);
                    watch.Stop();
                    var speed = watch.Elapsed.TotalSeconds > 0
                        ? Math.Max(0, video.Size) / watch.Elapsed.TotalSeconds
                        : 0;
                    Ui.Print(
                        $"[DONE ] {Path.GetFileName(actualPath)} | " +
                        $"{Ui.FormatBytes((long)speed)}/s | retries {retryCount}");
                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (RpcException ex) when (IsExpiredFileReference(ex))
                {
                    ResetLogicalProgressToCheckpoint(video);
                    video.MarkReferenceStale();
                    if (attempt > AppConstants.MaxDownloadRetries)
                    {
                        Fail(video, ex);
                        return;
                    }

                    retryCount++;
                    Interlocked.Increment(ref _stats.Retries);
                }
                catch (RpcException ex) when (ex.Code == 420 && ex.X > 0)
                {
                    ResetLogicalProgressToCheckpoint(video);
                    await RegisterFloodWaitAsync(ex.X, cancellationToken);
                    await _stats.FloodCooldown.WaitAsync(cancellationToken);
                    attempt--;
                }
                catch (TimeoutException ex)
                {
                    ResetLogicalProgressToCheckpoint(video);
                    Interlocked.Increment(ref _stats.Timeouts);
                    if (attempt > AppConstants.MaxDownloadRetries)
                    {
                        Fail(video, ex);
                        return;
                    }

                    retryCount++;
                    Interlocked.Increment(ref _stats.Retries);
                    Ui.Print($"[RETRY] Timeout | {video.ExpectedFileName} | {attempt}/{AppConstants.MaxDownloadRetries}");
                    await Task.Delay(TimeSpan.FromSeconds(attempt * 3), cancellationToken);
                }
                catch (Exception ex)
                {
                    ResetLogicalProgressToCheckpoint(video);
                    if (attempt > AppConstants.MaxDownloadRetries)
                    {
                        Fail(video, ex);
                        return;
                    }

                    retryCount++;
                    Interlocked.Increment(ref _stats.Retries);
                    Ui.Print(
                        $"[RETRY] {video.ExpectedFileName} | " +
                        $"{attempt}/{AppConstants.MaxDownloadRetries} | {ex.Message}");
                    await Task.Delay(TimeSpan.FromSeconds(attempt * 3), cancellationToken);
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _stats.Active);
        }
    }

    // A yielded attempt keeps its file handle only when there are already-written
    // parts beyond a gap. FileShare.Read protects those bytes from replacement or
    // other writers until we reuse the tracker. No media buffers are retained.
    private readonly ConcurrentDictionary<VideoItem, PartialFileAttempt> _parkedPartials = new();

    public void Dispose()
    {
        foreach (var attempt in _parkedPartials.Values)
            attempt.Stream.Dispose();
        _parkedPartials.Clear();
    }

    private async Task DownloadToPartialAsync(
        VideoItem video,
        string finalPath,
        CancellationToken cancellationToken)
    {
        var partialPath = finalPath + ".part";
        var statePath = partialPath + ".state.json";
        var attempt = await OpenPartialAsync(video, partialPath, statePath, cancellationToken);
        var stream = attempt.Stream;
        var tracker = attempt.Tracker;
        var partSize = attempt.State.PartSize;
        var checkpoint = new PartialCheckpointWriter(statePath, attempt.State, tracker);
        var parked = false;

        try
        {
            ReportResume(video, attempt.State.ContiguousBytes, tracker.CompletedBytes);
            var offsets = tracker.MissingOffsets();
            await Parallel.ForEachAsync(
                offsets,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = AppConstants.MaximumTransferOperations,
                    CancellationToken = cancellationToken
                },
                async (offset, ct) =>
                {
                    var bytes = await DownloadPartAsync(video, offset, partSize, ct);
                    var expected = (int)Math.Min(partSize, video.Size - offset);
                    if (bytes.Length != expected)
                    {
                        throw new InvalidDataException(
                            $"Part at {offset} has {bytes.Length} bytes; expected {expected}.");
                    }

                    await RandomAccess.WriteAsync(stream.SafeFileHandle, bytes, offset, ct);
                    Interlocked.Add(ref _stats.NetworkBytes, bytes.Length);
                    Interlocked.Add(ref _stats.LogicalBytes, bytes.Length);
                    Interlocked.Add(ref video.ProgressBytes, bytes.Length);
                    tracker.Complete(offset, bytes.Length);
                    await checkpoint.CheckpointIfNeededAsync(ct);
                });

            await checkpoint.FlushAsync(cancellationToken);
            if (tracker.ContiguousBytes != video.Size)
            {
                throw new InvalidDataException(
                    $"Incomplete part map. Expected {video.Size}, completed {tracker.ContiguousBytes}.");
            }

            stream.SetLength(video.Size);
            await stream.FlushAsync(cancellationToken);
            stream.Flush(flushToDisk: true);
        }
        catch
        {
            await checkpoint.FlushAsync(CancellationToken.None);
            // Parallel.ForEachAsync has joined every writer before we hand the
            // handle to a future attempt. A process restart uses the sidecar.
            if (cancellationToken.IsCancellationRequested && tracker.CompletedBytes > tracker.ContiguousBytes)
                parked = _parkedPartials.TryAdd(video, attempt);
            throw;
        }
        finally
        {
            if (!parked)
                await stream.DisposeAsync();
        }
    }

    private async Task<byte[]> DownloadPartAsync(
        VideoItem video, long offset, int partSize, CancellationToken cancellationToken)
    {
        const int maximumPartAttempts = 6;
        for (var attempt = 1; ; attempt++)
        {
            await _stats.FloodCooldown.WaitAsync(cancellationToken);
            var result = await RequestPartFromDcAsync(video, offset, partSize, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            if (result.Error is null)
            {
                await ReportHealthyPartAsync(cancellationToken);
                return result.Value!;
            }

            var error = result.Error;
            if (error is RpcException { Code: 420, X: > 0 })
            {
                attempt--; // A server-mandated wait does not consume retries.
                continue;
            }
            if (attempt >= maximumPartAttempts || !IsTransientPartFailure(error, cancellationToken))
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw();

            Interlocked.Increment(ref _stats.PartRetries);
            if (IsTimeoutFailure(error))
            {
                Interlocked.Increment(ref _stats.RpcTimeouts);
                Interlocked.Exchange(ref _healthyParts, 0);
                await ReduceTransferPressureAsync(cancellationToken);
            }

            Ui.Print($"[PART RETRY] message {video.MessageId} | offset {offset} | " +
                $"{attempt}/{maximumPartAttempts - 1} | {error.Message}");
            var backoff = Math.Min(8_000, 400 * (1 << (attempt - 1)));
            await Task.Delay(backoff + Random.Shared.Next(100, 750), cancellationToken);
        }
    }

    internal bool IsMediaDcRecovering(int dcId) =>
        _partRequestPolicy.IsRecovering(_mediaDcClients.ResolveDc(dcId));

    internal string DescribeMediaWaits() => _partRequestPolicy.DescribeWaits();

    internal bool CanYieldMediaDc(int dcId) =>
        _partRequestPolicy.IsSleeping(_mediaDcClients.ResolveDc(dcId));

    private async Task RegisterFloodWaitAsync(int seconds, CancellationToken cancellationToken)
    {
        var added = _stats.FloodCooldown.Defer(TimeSpan.FromSeconds((double)seconds + 1));
        Interlocked.Increment(ref _stats.FloodWaitEvents);
        Interlocked.Add(ref _stats.FloodWaitSeconds, (long)Math.Ceiling(added.TotalSeconds));
        Interlocked.Exchange(ref _healthyParts, 0);
        Ui.Print($"[WAIT] Telegram requested a shared pause of {seconds}s; current offsets are preserved.");
        await ReduceTransferPressureAsync(cancellationToken, "Shared Telegram flood wait");
    }

    private async Task<RequestResult<byte[]>> RequestPartFromDcAsync(
        VideoItem video, long offset, int partSize, CancellationToken cancellationToken)
    {
        int? dcOverride = null;
        var refreshes = 0;
        var migrations = 0;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var location = video.CreateLocationSnapshot();
                var dcId = dcOverride ?? location.DcId;
                var connection = await RequestDeadline.RunAsync(
                    () => _mediaDcClients.GetAsync(dcId, cancellationToken),
                    AppConstants.TelegramRequestTimeout, cancellationToken);
                if (connection.Error is not null)
                {
                    if (connection.Error is RpcException { Code: 420, X: > 0 } connectionWait)
                        await RegisterFloodWaitAsync(connectionWait.X, cancellationToken);
                    return RequestResult<byte[]>.Failure(connection.Error);
                }

                var downloadClient = connection.Value!;
                var result = await _partRequestPolicy.RunAsync(
                    _mediaDcClients.ResolveDc(dcId),
                    () => downloadClient.Upload_GetFile(location.Location, offset, partSize),
                    AppConstants.PartRequestTimeout, cancellationToken);
                if (result.Error is null)
                    return result.Value is Upload_File file
                        ? RequestResult<byte[]>.Success(file.bytes)
                        : RequestResult<byte[]>.Failure(new InvalidDataException(
                            $"Telegram returned unsupported {result.Value?.GetType().Name ?? "null"}."));

                var error = result.Error;
                if (cancellationToken.IsCancellationRequested)
                    return RequestResult<byte[]>.Failure(error);

                if (error is RpcException rpc)
                {
                    if (rpc.Code == 303 && rpc.X > 0 && migrations++ < 2)
                    {
                        dcOverride = rpc.X;
                        continue;
                    }
                    if (IsExpiredFileReference(rpc) && refreshes++ < 3)
                    {
                        Interlocked.Increment(ref _stats.PartRetries);
                        await RefreshFileReferenceIfCurrentAsync(video, location.Version, cancellationToken);
                        dcOverride = null;
                        continue;
                    }
                }

                // WaitAsync used to abandon the RPC and reuse this very same
                // socket. Retire the observed generation, even if TCP still says
                // "connected"; its DisposeAsync faults outstanding RPCs. All
                // workers then share one replacement, after disposal completes.
                if (IsTimeoutFailure(error) ||
                    error is ObjectDisposedException or IOException ||
                    error is WTException && error.Message.Contains(
                        "connection shut down", StringComparison.OrdinalIgnoreCase))
                {
                    if (_mediaDcClients.Retire(dcId, downloadClient))
                    {
                        Interlocked.Increment(ref _stats.DcReconnects);
                        Ui.Print($"[RECONNECT] media DC {dcId} | {error.Message} | " +
                            "closing stalled connection; unfinished parts will retry.");
                    }
                }
                return RequestResult<byte[]>.Failure(error);
            }
        }
        catch (RpcException ex) when (ex.Code == 420 && ex.X > 0)
        {
            // A metadata refresh can also surface a wait on the main session.
            await RegisterFloodWaitAsync(ex.X, cancellationToken);
            return RequestResult<byte[]>.Failure(ex);
        }
        catch (Exception ex)
        {
            // Keep failures inside user code until the part retry policy runs.
            return RequestResult<byte[]>.Failure(ex);
        }
    }

    private async Task RefreshFileReferenceIfCurrentAsync(
        VideoItem video,
        int observedVersion,
        CancellationToken cancellationToken)
    {
        await video.ReferenceRefreshLock.WaitAsync(cancellationToken);
        try
        {
            // Several in-flight parts can receive FILE_REFERENCE_EXPIRED at
            // once. Only the first one fetches the message again; the others
            // immediately reuse the newly published location snapshot.
            if (video.GetLocationVersion() != observedVersion)
                return;

            video.MarkReferenceStaleIfVersion(observedVersion);
            await RefreshFileReferenceAsync(video, cancellationToken);
        }
        finally
        {
            video.ReferenceRefreshLock.Release();
        }
    }

    private async Task RefreshStaleFileReferenceAsync(
        VideoItem video,
        CancellationToken cancellationToken)
    {
        if (!video.IsReferenceStale(DateTime.UtcNow, AppConstants.FileReferenceMaximumAge))
            return;

        await video.ReferenceRefreshLock.WaitAsync(cancellationToken);
        try
        {
            if (video.IsReferenceStale(DateTime.UtcNow, AppConstants.FileReferenceMaximumAge))
                await RefreshFileReferenceAsync(video, cancellationToken);
        }
        finally
        {
            video.ReferenceRefreshLock.Release();
        }
    }

    private async Task ReduceTransferPressureAsync(CancellationToken cancellationToken,
        string reason = "Telegram RPC timeouts")
    {
        var now = Stopwatch.GetTimestamp();
        int currentLimit;
        lock (_pressureLock)
        {
            if (_lastPressureReductionTimestamp != 0 &&
                Stopwatch.GetElapsedTime(_lastPressureReductionTimestamp, now) < TimeSpan.FromSeconds(15))
                return;

            _lastPressureReductionTimestamp = now;
            currentLimit = _transferGate.Limit;
            _recoveryTransferLimit = Math.Max(_recoveryTransferLimit, currentLimit);
        }

        var reducedLimit = Math.Min(currentLimit, Math.Max(2, currentLimit - 2));
        if (reducedLimit == currentLimit)
            return;

        await _transferGate.SetLimitAsync(reducedLimit, cancellationToken);
        Volatile.Write(ref _stats.ActiveTransferLimit, reducedLimit);
        Ui.Print(
            $"[THROTTLE] {reason}; " +
            $"part operations reduced {currentLimit} -> {reducedLimit}.");
    }

    private async Task ReportHealthyPartAsync(CancellationToken cancellationToken)
    {
        if (_stats.FloodCooldown.Remaining > TimeSpan.Zero)
            return;
        if (Interlocked.Increment(ref _healthyParts) < 256)
            return;

        var now = Stopwatch.GetTimestamp();
        int currentLimit;
        lock (_pressureLock)
        {
            if (_lastPressureReductionTimestamp == 0 ||
                Stopwatch.GetElapsedTime(_lastPressureReductionTimestamp, now) < TimeSpan.FromMinutes(1) ||
                (_lastPressureIncreaseTimestamp != 0 &&
                 Stopwatch.GetElapsedTime(_lastPressureIncreaseTimestamp, now) < TimeSpan.FromMinutes(1)))
                return;

            currentLimit = _transferGate.Limit;
            if (currentLimit >= _recoveryTransferLimit)
                return;
            _lastPressureIncreaseTimestamp = now;
            Interlocked.Exchange(ref _healthyParts, 0);
        }

        var increasedLimit = Math.Min(_recoveryTransferLimit, currentLimit + 1);
        await _transferGate.SetLimitAsync(increasedLimit, cancellationToken);
        Volatile.Write(ref _stats.ActiveTransferLimit, increasedLimit);
        Ui.Print(
            $"[RECOVER] Telegram requests are healthy; " +
            $"part operations increased {currentLimit} -> {increasedLimit}.");
    }

    internal static bool IsTransientPartFailure(
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested || exception is OperationCanceledException)
            return false;
        if (exception is TimeoutException or IOException or ObjectDisposedException)
            return true;
        if (exception is WTException && exception.Message.Contains(
                "connection shut down", StringComparison.OrdinalIgnoreCase))
            return true;
        return exception is RpcException rpc &&
               rpc.Code != 420 &&
               !IsExpiredFileReference(rpc) &&
               (rpc.Code >= 500 ||
                IsTimeoutFailure(rpc) ||
                IsRecoverableBadMessageNotification(rpc));
    }

    private static bool IsRecoverableBadMessageNotification(RpcException exception) =>
        exception.Code == -503 &&
        exception.Message.Contains(
            "BadMsgNotification 48",
            StringComparison.OrdinalIgnoreCase);

    private static bool IsTimeoutFailure(Exception exception) =>
        exception is TimeoutException ||
        exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
        exception.Message.Contains("timed out", StringComparison.OrdinalIgnoreCase);

    private async Task<PartialFileAttempt> OpenPartialAsync(VideoItem video, string partialPath,
        string statePath, CancellationToken cancellationToken)
    {
        var maximumSafePrefix = video.Size;
        if (_parkedPartials.TryRemove(video, out var parked))
        {
            var reuse = false;
            try
            {
                reuse = await parked.CanResumeAsync(partialPath, statePath, cancellationToken);
                if (reuse)
                    return parked;
                // An edited sidecar cannot promote a known hole in this locked
                // file into a completed prefix.
                if (parked.IsSamePath(partialPath))
                    maximumSafePrefix = parked.State.ContiguousBytes;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Another dispatch yield can arrive during sidecar validation.
                // Preserve ownership so the next attempt can validate again.
                reuse = _parkedPartials.TryAdd(video, parked);
                throw;
            }
            finally
            {
                if (!reuse)
                    await parked.Stream.DisposeAsync();
            }
        }

        var state = await LoadOrCreatePartialStateAsync(
            video, partialPath, statePath, maximumSafePrefix, cancellationToken);
        return PartialFileAttempt.Open(partialPath, state);
    }

    private async Task<PartialDownloadState> LoadOrCreatePartialStateAsync(
        VideoItem video,
        string partialPath,
        string statePath,
        long maximumSafePrefix,
        CancellationToken cancellationToken)
    {
        PartialDownloadState? state = null;
        if (File.Exists(statePath))
        {
            state = await JsonFiles.LoadAsync<PartialDownloadState?>(
                statePath,
                () => null,
                cancellationToken);
        }

        var valid = state is not null &&
                    state.Version == 1 &&
                    state.VideoKey == video.Key &&
                    state.ExpectedSize == video.Size &&
                    state.PartSize is 256 * 1024 or 512 * 1024 or 1024 * 1024 &&
                    state.ContiguousBytes >= 0 &&
                    state.ContiguousBytes <= maximumSafePrefix &&
                    (state.ContiguousBytes == video.Size || state.ContiguousBytes % state.PartSize == 0) &&
                    File.Exists(partialPath) &&
                    new FileInfo(partialPath).Length >= state.ContiguousBytes;

        if (!valid)
        {
            SafeDelete(statePath);
            SafeDelete(partialPath);
            state = new PartialDownloadState
            {
                VideoKey = video.Key,
                ExpectedSize = video.Size,
                PartSize = _settings.PartSizeBytes,
                ContiguousBytes = 0,
                UpdatedUtc = DateTime.UtcNow
            };
            await JsonFiles.SaveAtomicAsync(
                statePath,
                state,
                cancellationToken,
                flushToDisk: false);
        }

        return state!;
    }

    private string FinalizeDownload(VideoItem video, string finalPath)
    {
        var partialPath = finalPath + ".part";
        var statePath = partialPath + ".state.json";
        var partialLength = new FileInfo(partialPath).Length;
        if (video.Size > 0 && partialLength != video.Size)
        {
            throw new InvalidDataException(
                $"Downloaded size mismatch. Expected {video.Size}, received {partialLength}.");
        }

        var actualPath = finalPath;
        if (File.Exists(actualPath))
        {
            if (FileDiscovery.IsCompleteFile(actualPath, video.Size))
            {
                SafeDelete(partialPath);
                SafeDelete(statePath);
                ResetLogicalProgress(video, video.Size);
                video.FinalPath = actualPath;
                return actualPath;
            }

            actualPath = FileNames.GetNonOverwritingPath(
                Path.GetDirectoryName(actualPath)!,
                Path.GetFileName(actualPath));
        }

        File.Move(partialPath, actualPath);
        SafeDelete(statePath);
        video.FinalPath = actualPath;
        return actualPath;
    }

    private async Task RefreshFileReferenceAsync(
        VideoItem video,
        CancellationToken cancellationToken)
    {
        await _stats.FloodCooldown.WaitAsync(cancellationToken);
        Ui.Print($"[REFRESH] Refreshing Telegram reference for {video.ExpectedFileName}");
        var ids = new InputMessage[] { new InputMessageID { id = video.MessageId } };
        Messages_MessagesBase messages = _chat switch
        {
            Channel channel => await _client.Channels_GetMessages(channel, ids)
                .WaitAsync(AppConstants.TelegramRequestTimeout, cancellationToken),
            _ => await _client.Messages_GetMessages(ids)
                .WaitAsync(AppConstants.TelegramRequestTimeout, cancellationToken)
        };

        var document = messages.Messages
            .OfType<TL.Message>()
            .Where(message => message.ID == video.MessageId)
            .Select(message => message.media)
            .OfType<MessageMediaDocument>()
            .Select(media => media.document)
            .OfType<Document>()
            .FirstOrDefault(candidate => candidate.id == video.DocumentId)
            ?? throw new InvalidOperationException(
                $"Could not refresh message {video.MessageId}; it may have been deleted.");

        video.RefreshLocation(document);
        Interlocked.Increment(ref _stats.FileReferenceRefreshes);
        _catalog.Videos[video.MessageId.ToString()] =
            CatalogVideoEntry.FromDocument(video.MessageId, document);
        _catalogSaver.MarkDirty(forceSoon: true);
    }

    private void ReportResume(VideoItem video, long prefixBytes, long completedBytes)
    {
        ResetLogicalProgress(video, completedBytes);
        // Only the prefix present on the first open came from an earlier run.
        // Later retries/yields may reopen bytes downloaded in this session.
        if (Interlocked.Exchange(ref video.ResumeReported, 1) == 0 && prefixBytes > 0)
        {
            Interlocked.Add(ref _stats.ResumedBytes, prefixBytes);
            Ui.Print($"[RESUME] {video.ExpectedFileName} from {Ui.FormatBytes(prefixBytes)}");
        }
    }

    private void ResetLogicalProgressToCheckpoint(VideoItem video)
    {
        var finalPath = video.FinalPath;
        if (string.IsNullOrWhiteSpace(finalPath))
            return;
        var statePath = finalPath + ".part.state.json";
        long checkpoint = 0;
        try
        {
            if (File.Exists(statePath))
            {
                var json = File.ReadAllText(statePath);
                checkpoint = System.Text.Json.JsonSerializer
                    .Deserialize<PartialDownloadState>(json)?.ContiguousBytes ?? 0;
            }
        }
        catch
        {
        }
        ResetLogicalProgress(video, checkpoint);
    }

    private void ResetLogicalProgress(VideoItem video, long newValue)
    {
        var oldValue = Interlocked.Exchange(ref video.ProgressBytes, newValue);
        Interlocked.Add(ref _stats.LogicalBytes, newValue - oldValue);
    }

    private void Fail(VideoItem video, Exception exception)
    {
        Interlocked.Increment(ref _stats.Failed);
        Ui.Print($"[FAIL ] {video.ExpectedFileName} | {exception.Message}");
    }

    private static bool IsExpiredFileReference(RpcException exception) =>
        exception.Message.Contains("FILE_REFERENCE", StringComparison.OrdinalIgnoreCase) ||
        exception.Message.Contains("FILEREF", StringComparison.OrdinalIgnoreCase);

    private static void SafeDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
        }
    }
}

internal sealed record PartialFileAttempt(FileStream Stream, PartialDownloadState State, PartialProgressTracker Tracker)
{
    public static PartialFileAttempt Open(string path, PartialDownloadState state)
    {
        var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
            1, FileOptions.Asynchronous | FileOptions.RandomAccess);
        try
        {
            stream.SetLength(state.ContiguousBytes);
            return new PartialFileAttempt(stream, state, new PartialProgressTracker(
                state.ExpectedSize, state.PartSize, state.ContiguousBytes));
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public bool IsSamePath(string path) =>
        string.Equals(Stream.Name, Path.GetFullPath(path), StringComparison.OrdinalIgnoreCase);

    public async Task<bool> CanResumeAsync(string path, string statePath, CancellationToken cancellationToken)
    {
        if (!IsSamePath(path))
            return false;
        // The held handle prevents writes, deletion and replacement of the .part
        // file. Validate the freely editable sidecar before trusting the gaps.
        var disk = await JsonFiles.LoadAsync<PartialDownloadState?>(statePath, () => null, cancellationToken);
        return disk is not null && disk.VideoKey == State.VideoKey && disk.Version == State.Version &&
            disk.ExpectedSize == State.ExpectedSize && disk.PartSize == State.PartSize &&
            disk.ContiguousBytes == State.ContiguousBytes && disk.UpdatedUtc == State.UpdatedUtc;
    }
}

internal sealed class PartialProgressTracker
{
    private readonly object _lock = new();
    private readonly long _expectedSize;
    private readonly int _partSize;
    private readonly HashSet<long> _completedOffsets = [];
    private long _contiguousBytes;
    private long _completedBytes;
    private int _completedSinceCheckpoint;

    public PartialProgressTracker(long expectedSize, int partSize, long contiguousBytes)
    {
        _expectedSize = expectedSize;
        _partSize = partSize;
        _contiguousBytes = contiguousBytes;
        _completedBytes = contiguousBytes;
    }

    public long ContiguousBytes
    {
        get
        {
            lock (_lock)
                return _contiguousBytes;
        }
    }

    public int CompletedSinceCheckpoint
    {
        get
        {
            lock (_lock)
                return _completedSinceCheckpoint;
        }
    }

    public long CompletedBytes
    {
        get { lock (_lock) return _completedBytes; }
    }

    public IEnumerable<long> MissingOffsets()
    {
        for (var offset = ContiguousBytes; offset < _expectedSize; offset += _partSize)
        {
            bool missing;
            lock (_lock)
                missing = offset >= _contiguousBytes && !_completedOffsets.Contains(offset);
            if (missing)
                yield return offset;
        }
    }

    public void Complete(long offset, int length)
    {
        lock (_lock)
        {
            var expected = (int)Math.Min(_partSize, _expectedSize - offset);
            if (length != expected)
                throw new InvalidDataException("A completed part has an unexpected length.");
            if (offset < _contiguousBytes || !_completedOffsets.Add(offset))
                throw new InvalidDataException("A file part was completed twice.");
            _completedBytes += length;
            _completedSinceCheckpoint++;
            while (_completedOffsets.Remove(_contiguousBytes))
                _contiguousBytes += Math.Min(_partSize, _expectedSize - _contiguousBytes);
        }
    }

    public (long ContiguousBytes, int CompletedParts) SnapshotForCheckpoint()
    {
        lock (_lock)
        {
            var result = (_contiguousBytes, _completedSinceCheckpoint);
            _completedSinceCheckpoint = 0;
            return result;
        }
    }
}

internal sealed class PartialCheckpointWriter
{
    private readonly string _statePath;
    private readonly PartialDownloadState _state;
    private readonly PartialProgressTracker _tracker;
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private long _lastCheckpointTimestamp = Stopwatch.GetTimestamp();

    public PartialCheckpointWriter(
        string statePath,
        PartialDownloadState state,
        PartialProgressTracker tracker)
    {
        _statePath = statePath;
        _state = state;
        _tracker = tracker;
    }

    public Task CheckpointIfNeededAsync(CancellationToken cancellationToken)
    {
        var elapsed = Stopwatch.GetElapsedTime(Volatile.Read(ref _lastCheckpointTimestamp));
        return _tracker.CompletedSinceCheckpoint >= AppConstants.PartialCheckpointChunks ||
               elapsed >= AppConstants.PartialCheckpointInterval
            ? TryFlushAsync(cancellationToken)
            : Task.CompletedTask;
    }

    private async Task TryFlushAsync(CancellationToken cancellationToken)
    {
        // A checkpoint is opportunistic during transfer. Never make all part
        // workers wait behind the same disk flush; one worker checkpoints while
        // the others keep the Telegram request pipeline full.
        if (!await _flushLock.WaitAsync(0, cancellationToken))
            return;

        try
        {
            await FlushCoreAsync(cancellationToken);
        }
        finally
        {
            _flushLock.Release();
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _flushLock.WaitAsync(cancellationToken);
        try
        {
            await FlushCoreAsync(cancellationToken);
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private async Task FlushCoreAsync(CancellationToken cancellationToken)
    {
        var snapshot = _tracker.SnapshotForCheckpoint();
        if (snapshot.CompletedParts == 0 && _state.ContiguousBytes == snapshot.ContiguousBytes)
            return;
        // RandomAccess.WriteAsync has already handed every checkpointed byte to
        // the OS. Avoid FlushFileBuffers here: on some disks it blocks every
        // writer for seconds and empties the network pipeline. The final file is
        // still force-flushed before it is renamed.
        var next = new PartialDownloadState
        {
            Version = _state.Version,
            VideoKey = _state.VideoKey,
            ExpectedSize = _state.ExpectedSize,
            PartSize = _state.PartSize,
            ContiguousBytes = snapshot.ContiguousBytes,
            UpdatedUtc = DateTime.UtcNow
        };
        await JsonFiles.SaveAtomicAsync(
            _statePath,
            next,
            cancellationToken,
            flushToDisk: false);
        _state.ContiguousBytes = next.ContiguousBytes;
        _state.UpdatedUtc = next.UpdatedUtc;
        Volatile.Write(ref _lastCheckpointTimestamp, Stopwatch.GetTimestamp());
    }
}
