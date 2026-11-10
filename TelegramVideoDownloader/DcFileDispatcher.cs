using System.Threading.Channels;

internal static class DcFileDispatcher
{
    private sealed class Running(VideoItem video, int weight, CancellationToken sessionToken, TimeProvider clock)
    {
        public VideoItem Video { get; } = video;
        public int DcId => Video.DcId;
        public int Weight { get; } = weight;
        public CancellationTokenSource Stop { get; } = CancellationTokenSource.CreateLinkedTokenSource(sessionToken);
        public Task Completion { get; set; } = Task.CompletedTask;
        public bool Yielding { get; set; }
        private long _bytes = Interlocked.Read(ref video.ProgressBytes);
        private long _lastProgress = clock.GetTimestamp();

        public TimeSpan ObserveIdleTime(long now)
        {
            var bytes = Interlocked.Read(ref Video.ProgressBytes);
            if (bytes != _bytes)
            {
                _bytes = bytes;
                _lastProgress = now;
            }
            return clock.GetElapsedTime(_lastProgress, now);
        }
    }

    public static async Task RunAsync(ChannelReader<VideoItem> input, int capacity,
        Func<int, bool> isRecovering, Func<VideoItem, CancellationToken, ValueTask> download,
        CancellationToken cancellationToken, TimeSpan? yieldAfter = null,
        Func<int, bool>? canYield = null, TimeProvider? timeProvider = null)
    {
        capacity = Math.Max(1, capacity);
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = stopping.Token;
        var running = new List<Running>(capacity);
        // Only queue heads compete for admission. The sequence preserves scan
        // order between equally suitable files without rescanning every video.
        var pending = new Dictionary<(int DcId, int Weight), Queue<(long Order, VideoItem Video)>>();
        long sequence = 0;
        Task<bool>? readable = null;
        var inputComplete = false;
        var clock = timeProvider ?? TimeProvider.System;
        var idleThreshold = yieldAfter ?? TimeSpan.FromSeconds(30);
        var checkInterval = TimeSpan.FromSeconds(2);
        using var timer = new PeriodicTimer(idleThreshold < checkInterval ? idleThreshold : checkInterval, clock);
        Task<bool>? tick = null;

        void Enqueue(VideoItem video)
        {
            var key = (video.DcId, GetWeight(video.Size, capacity));
            if (!pending.TryGetValue(key, out var bucket))
                pending[key] = bucket = new();
            bucket.Enqueue((sequence++, video));
        }

        try
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (var i = running.Count - 1; i >= 0; i--)
                {
                    var file = running[i];
                    if (!file.Completion.IsCompleted) continue;
                    try { await file.Completion; }
                    catch (OperationCanceledException) when (file.Yielding && !token.IsCancellationRequested)
                    {
                        Enqueue(file.Video);
                        Ui.Print($"[DEFER] message {file.Video.MessageId} on DC {file.DcId} checkpointed " +
                            "and queued again so a healthy DC can download.");
                    }
                    file.Stop.Dispose();
                    running.RemoveAt(i);
                }
                if (readable?.IsCompleted == true)
                {
                    inputComplete = !await readable;
                    readable = null;
                }
                while (input.TryRead(out var video))
                {
                    token.ThrowIfCancellationRequested();
                    Enqueue(video);
                }
                if (tick?.IsCompleted == true)
                {
                    await tick;
                    tick = null;
                }

                var free = capacity - running.Sum(file => file.Weight);
                while (pending.Count > 0)
                {
                    var healthy = pending.Where(bucket => !isRecovering(bucket.Key.DcId)).ToArray();
                    var candidates = healthy.Length > 0 ? healthy : pending.AsEnumerable();
                    var next = candidates.Where(bucket => bucket.Key.Weight <= free)
                        .OrderBy(bucket => running.Count(file => file.DcId == bucket.Key.DcId))
                        .ThenBy(bucket => bucket.Value.Peek().Order)
                        .FirstOrDefault();
                    if (next.Value is null)
                    {
                        if (healthy.Length > 0)
                            await YieldForHealthyFileAsync(running, healthy.Min(bucket => bucket.Key.Weight) - free,
                                canYield ?? isRecovering, idleThreshold, clock.GetTimestamp(), token);
                        break;
                    }
                    var video = next.Value.Dequeue().Video;
                    if (next.Value.Count == 0) pending.Remove(next.Key);
                    var started = new Running(video, next.Key.Weight, token, clock);
                    running.Add(started);
                    started.Completion = download(video, started.Stop.Token).AsTask();
                    free -= next.Key.Weight;
                }

                if (inputComplete && running.Count == 0) return;
                var wakeups = running.Select(file => file.Completion).ToList();
                if (!inputComplete)
                {
                    readable ??= input.WaitToReadAsync(token).AsTask();
                    wakeups.Add(readable);
                }
                if (running.Count > 0 && pending.Count > 0)
                {
                    // While all slots are occupied both the input and the running
                    // files can be silent. One shared timer checks for stale work.
                    tick ??= timer.WaitForNextTickAsync().AsTask();
                    wakeups.Add(tick);
                }
                await Task.WhenAny(wakeups);
            }
        }
        catch
        {
            await stopping.CancelAsync();
            // Wait for each active file to finish checkpointing before its
            // client and destination resources can be disposed by the caller.
            await Task.WhenAll(running.Select(file => file.Completion))
                .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            throw;
        }
        finally
        {
            foreach (var file in running) file.Stop.Dispose();
        }
    }

    private static async Task YieldForHealthyFileAsync(List<Running> running, int needed,
        Func<int, bool> canYield, TimeSpan idleThreshold, long now, CancellationToken token)
    {
        needed -= running.Where(file => file.Yielding).Sum(file => file.Weight);
        if (needed <= 0) return;
        var eligible = running.Where(file => file.ObserveIdleTime(now) >= idleThreshold &&
                !file.Yielding && !file.Completion.IsCompleted && canYield(file.DcId))
            .OrderByDescending(file => file.Weight)
            .ToArray();
        if (eligible.Sum(file => file.Weight) < needed) return;
        foreach (var file in eligible)
        {
            token.ThrowIfCancellationRequested();
            file.Yielding = true;
            await file.Stop.CancelAsync();
            needed -= file.Weight;
            if (needed <= 0) break;
        }
    }

    private static int GetWeight(long fileSize, int capacity)
    {
        if (fileSize <= 20L * 1024 * 1024) return 1;
        if (fileSize <= 4L * 1024 * 1024 * 1024) return Math.Min(2, capacity);
        return capacity;
    }
}
