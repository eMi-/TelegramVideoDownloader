using System.Collections.Concurrent;
using System.Threading.Channels;

internal static class DcFileDispatcherTests
{
    public static async Task RunAsync()
    {
        const long mib = 1024 * 1024;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var queue = Channel.CreateUnbounded<VideoItem>();
        var started = new ConcurrentQueue<int>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstBatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var occupied = 0;
        async ValueTask Download(VideoItem video, CancellationToken token)
        {
            var weight = video.Size switch { > 4L * 1024 * mib => 4, > 20 * mib => 2, _ => 1 };
            if (Interlocked.Add(ref occupied, weight) > 4)
                throw new Exception("A video started without enough file slots.");
            try
            {
                started.Enqueue(video.MessageId);
                if (started.Count == 3) firstBatch.TrySetResult();
                await release.Task.WaitAsync(token);
            }
            finally { Interlocked.Add(ref occupied, -weight); }
        }

        static VideoItem Video(int id, int dc, long bytes) => VideoItem.FromDocument(1, id,
            new TL.Document { id = id, dc_id = dc, size = bytes, attributes = [], file_reference = [] });
        queue.Writer.TryWrite(Video(1, 5, 40 * mib));    // A DC in recovery.
        queue.Writer.TryWrite(Video(2, 2, 40 * mib));    // First healthy video.
        queue.Writer.TryWrite(Video(3, 2, 40 * mib));    // Same DC as the active file.
        queue.Writer.TryWrite(Video(4, 4, 4 * mib));
        queue.Writer.TryWrite(Video(5, 3, 5 * 1024 * mib)); // Needs the full budget.
        queue.Writer.TryWrite(Video(6, 1, 7 * mib));
        var dispatch = DcFileDispatcher.RunAsync(queue.Reader, 4, dc => dc == 5, Download, timeout.Token);
        try
        {
            await firstBatch.Task.WaitAsync(timeout.Token);
            if (!started.SequenceEqual(new[] { 2, 4, 6 }))
                throw new Exception("Healthy DCs could not fill the available slots during discovery.");
            // Simulate another page arriving while earlier downloads are active.
            queue.Writer.TryWrite(Video(7, 1, 12 * mib));
        }
        finally
        {
            queue.Writer.TryComplete();
            release.TrySetResult();
        }
        await dispatch.WaitAsync(timeout.Token);
        if (!started.Order().SequenceEqual(Enumerable.Range(1, 7)) || occupied != 0)
            throw new Exception("A delayed video was lost, duplicated or left running.");

        // Cancellation during discovery must join the active file's checkpoint.
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var streaming = Channel.CreateUnbounded<VideoItem>();
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var checkpointed = false;
        async ValueTask UntilCancelled(VideoItem video, CancellationToken ct)
        {
            ready.TrySetResult();
            try { await Task.Delay(Timeout.Infinite, ct); }
            finally
            {
                await Task.Yield();
                checkpointed = true;
            }
        }
        var cancelling = DcFileDispatcher.RunAsync(streaming.Reader, 4, _ => false, UntilCancelled, stop.Token);
        streaming.Writer.TryWrite(Video(8, 2, 5 * mib));
        await ready.Task.WaitAsync(timeout.Token);
        stop.Cancel();
        try
        {
            await cancelling.WaitAsync(timeout.Token);
            throw new Exception("File dispatch ignored cancellation.");
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        if (!checkpointed) throw new Exception("File dispatch returned before checkpointing finished.");

        await TestStalledFileYieldAsync(timeout.Token);
    }

    private sealed class TestClock : TimeProvider
    {
        private long _ticks;
        public int Samples;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp()
        {
            Interlocked.Increment(ref Samples);
            return Volatile.Read(ref _ticks);
        }
        public void Advance(TimeSpan duration) => Interlocked.Add(ref _ticks, duration.Ticks);
    }

    private static async Task TestStalledFileYieldAsync(CancellationToken token)
    {
        const long mib = 1024 * 1024;
        static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
        static VideoItem Video(int id, int dc, long bytes) => VideoItem.FromDocument(1, id,
            new TL.Document { id = id, dc_id = dc, size = bytes, attributes = [], file_reference = [] });

        var clock = new TestClock();
        var input = Channel.CreateUnbounded<VideoItem>();
        var stalled = Video(9, 5, 40 * mib);
        var healthy = Video(10, 4, 40 * mib);
        var smallRecovering = Video(11, 2, 1 * mib);
        var started = Signal();
        var protectedProbeChecked = Signal();
        var checkpointing = Signal();
        var allowCheckpoint = Signal();
        var healthyStarted = Signal();
        var smallStarted = Signal();
        var allowYield = 0;
        var stalledAttempts = 0;
        var saved = false;
        bool CanYield(int dc)
        {
            var allowed = Volatile.Read(ref allowYield) == 1;
            if (dc == 5 && !allowed) protectedProbeChecked.TrySetResult();
            return allowed && dc == 5;
        }
        async ValueTask Download(VideoItem file, CancellationToken ct)
        {
            if (file == stalled)
            {
                if (++stalledAttempts > 1)
                {
                    if (!saved || !healthyStarted.Task.IsCompleted)
                        throw new Exception("The deferred file resumed before its checkpoint or healthy work.");
                    return;
                }
                started.TrySetResult();
                try { await Task.Delay(Timeout.Infinite, ct); }
                finally
                {
                    checkpointing.TrySetResult();
                    await allowCheckpoint.Task.WaitAsync(token);
                    saved = true;
                }
            }
            else if (file == healthy)
            {
                if (!saved) throw new Exception("A file slot was reused before checkpoint cleanup.");
                healthyStarted.TrySetResult();
            }
            else
            {
                smallStarted.TrySetResult();
                if (!healthyStarted.Task.IsCompleted)
                    throw new Exception("A smaller file on a throttled DC consumed the reserved capacity.");
            }
        }

        input.Writer.TryWrite(stalled);
        var threshold = TimeSpan.FromMilliseconds(20);
        var dispatch = DcFileDispatcher.RunAsync(input.Reader, 3, dc => dc is 2 or 5, Download, token,
            threshold, CanYield, clock);
        await started.Task.WaitAsync(token);
        clock.Advance(threshold * 2);
        input.Writer.TryWrite(healthy);
        input.Writer.TryWrite(smallRecovering);
        input.Writer.TryComplete();
        try
        {
            await protectedProbeChecked.Task.WaitAsync(token);
            if (checkpointing.Task.IsCompleted)
                throw new Exception("A protected network probe was cancelled.");
            Interlocked.Add(ref stalled.ProgressBytes, 512 * 1024);
            Volatile.Write(ref allowYield, 1);
            var observed = Volatile.Read(ref clock.Samples);
            while (Volatile.Read(ref clock.Samples) < observed + 3)
                await Task.Delay(5, token);
            if (checkpointing.Task.IsCompleted)
                throw new Exception("A recovering file was yielded immediately after byte progress.");
            clock.Advance(threshold * 2);
            await checkpointing.Task.WaitAsync(token);
            if (healthyStarted.Task.IsCompleted || smallStarted.Task.IsCompleted)
                throw new Exception("A new file started while the yielded file was checkpointing.");
        }
        finally { allowCheckpoint.TrySetResult(); }
        await dispatch.WaitAsync(token);
        if (stalledAttempts != 2 || !healthyStarted.Task.IsCompleted || !smallStarted.Task.IsCompleted)
            throw new Exception("Reclaiming slots lost or duplicated a scheduled download.");
    }
}
