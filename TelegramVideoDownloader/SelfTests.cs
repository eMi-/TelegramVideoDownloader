internal static class SelfTests
{
    public static async Task RunAsync()
    {
        var failures = new List<string>();
        await TestAsync("out-of-order checkpoint tracking and parked file recovery", TestCheckpointTrackingAsync, failures);
        Test("filename safety", TestFilenameSafety, failures);
        Test("file-reference snapshot refresh", TestFileReferenceSnapshotRefresh, failures);
        Test("server-salt retry classification", TestServerSaltRetryClassification, failures);
        await TestAsync("atomic JSON round-trip", TestJsonRoundTripAsync, failures);
        await TestAsync("stalled parallel requests recover at identical offsets", TestStalledRequestsAsync, failures);
        await TestAsync("cancelled waiter cannot duplicate connection setup", TestCancelledConnectionWaiterAsync, failures);
        await TestAsync("failed connection can retry", TestFailedConnectionAsync, failures);
        await TestAsync("request cancellation and RPC errors", TestRequestErrorsAsync, failures);
        await TestAsync("resize during stalled transfers and cancelled waiters", TestResizeAsync, failures);
        Test("ETA with no traffic is unknown", () => Equal("--:--:--", Ui.FormatEta(100, 0)), failures);
        Test("flood waits merge and never shorten", TestFloodWaitMerging, failures);
        await TestAsync("flood wait preserves RPC duration and cancels promptly", TestFloodWaitCancellationAsync, failures);
        await TestAsync("short flood wait leaves healthy transfers running", TestIndependentShortWaitAsync, failures);
        await TestAsync("long flood wait reaches shared recovery", TestLongWaitPolicyAsync, failures);
        await TestAsync("short flood wait cancellation releases its slot", TestShortWaitCancellationAsync, failures);

        await TestAsync("DC-aware file dispatch and weighted admission", DcFileDispatcherTests.RunAsync, failures);

        if (failures.Count > 0)
        {
            Console.Error.WriteLine($"SELF-TEST FAILED ({failures.Count})");
            foreach (var failure in failures)
                Console.Error.WriteLine(failure);
            Environment.ExitCode = 1;
            return;
        }

        Console.WriteLine("SELF-TEST PASSED (17 tests)");
    }

    private static async Task TestIndependentShortWaitAsync()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = limit.Token;
        var stats = new TransferStats();
        var gate = new ResizableTransferGate(8, 16);
        var clock = new TestClock();
        var policy = new PartRequestPolicy(stats, gate, (_, _) => throw new Exception("Unexpected long wait."), clock);
        var firstWave = Enumerable.Range(0, 8)
            .Select(_ => new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var requestsPerOffset = new int[9];
        var probeStarted = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        var permitProbe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var twoAdmitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var permitRest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int postWaitCalls = 0;
        async Task<int> ReadPartAsync(int offset)
        {
            int attempt = Interlocked.Increment(ref requestsPerOffset[offset]);
            if (offset < 8 && attempt == 1) return await firstWave[offset].Task.WaitAsync(token);

            int call = Interlocked.Increment(ref postWaitCalls);
            if (call == 1)
            {
                probeStarted.SetResult(TimeSpan.FromTicks(clock.GetTimestamp()));
                await permitProbe.Task.WaitAsync(token);
            }
            if (call > 4)
            {
                if (call == 6) twoAdmitted.SetResult();
                await permitRest.Task.WaitAsync(token);
            }
            return offset;
        }

        // Eight offsets on DC 5 fill the global gate. A ninth queues at that gate
        // before Telegram replies, so it must recheck admission when it wakes.
        var blocked = Enumerable.Range(0, requestsPerOffset.Length)
            .Select(offset => policy.RunAsync(5, () => ReadPartAsync(offset), TimeSpan.FromSeconds(5), token))
            .ToArray();
        Equal(8, stats.ActivePartRequests);
        Equal(0, requestsPerOffset[8]);
        var earliestRetry = TimeSpan.FromSeconds(2);
        for (var i = 0; i < firstWave.Length; i++)
            firstWave[i].SetException(new TL.RpcException(420, "FLOOD_WAIT_X", i == 0 ? 2 : 1));

        while (Volatile.Read(ref stats.WaitingPartRequests) != blocked.Length)
            await Task.Delay(5, token);
        Equal(true, policy.IsRecovering(5));
        Equal(false, policy.IsRecovering(2));
        Equal(8, stats.ShortFloodWaitEvents);
        Equal(9L, stats.ShortFloodWaitSeconds);
        Equal(0, stats.ActivePartRequests);
        Equal(0, postWaitCalls);
        Equal(TimeSpan.Zero, stats.FloodCooldown.Remaining);
        Equal("DC 5 wait 2s", policy.DescribeWaits());

        // All eight slots are usable on the other DC at the same time.
        var releaseHealthy = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthy = Enumerable.Range(0, 8)
            .Select(_ => policy.RunAsync(2, () => releaseHealthy.Task, TimeSpan.FromSeconds(5), token))
            .ToArray();
        Equal(8, stats.ActivePartRequests);
        releaseHealthy.SetResult(123);
        foreach (var completed in await Task.WhenAll(healthy).WaitAsync(token))
        {
            Equal(true, completed.Error is null);
            Equal(123, completed.Value);
        }

        // Respect the longest concurrent response. Only one probe can start.
        Interlocked.Exchange(ref clock.Ticks, earliestRetry.Ticks);
        var actualRetry = await probeStarted.Task.WaitAsync(token);
        if (actualRetry < earliestRetry) throw new Exception("Retried before Telegram's longest deadline.");
        Equal(1, Volatile.Read(ref postWaitCalls));
        Equal(1, stats.ActivePartRequests);
        permitProbe.SetResult();

        // Four successful parts admit exactly one more concurrent request on
        // this DC. In-flight siblings cannot instantly reopen all eight slots.
        await twoAdmitted.Task.WaitAsync(token);
        Equal(6, Volatile.Read(ref postWaitCalls));
        Equal(2, stats.ActivePartRequests);
        permitRest.SetResult();
        var recovered = await Task.WhenAll(blocked).WaitAsync(token);
        for (var i = 0; i < recovered.Length; i++)
        {
            Equal(true, recovered[i].Error is null);
            Equal(i, recovered[i].Value);
            Equal(i < 8 ? 2 : 1, requestsPerOffset[i]);
        }
        Equal(0, stats.WaitingPartRequests);
        Equal(0, stats.ActivePartRequests);
        Equal(8, gate.Limit);
    }

    private static async Task TestLongWaitPolicyAsync()
    {
        var stats = new TransferStats();
        var gate = new ResizableTransferGate(1, 16);
        int sharedWait = 0;
        var policy = new PartRequestPolicy(stats, gate, (seconds, _) =>
        {
            sharedWait = seconds;
            stats.FloodCooldown.Defer(TimeSpan.FromSeconds(seconds + 1));
            return Task.CompletedTask;
        });
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var raw = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = policy.RunAsync(5, () => raw.Task, TimeSpan.FromSeconds(2), limit.Token);
        bool sent = false;
        var queued = policy.RunAsync(2, () => { sent = true; return Task.FromResult(2); },
            TimeSpan.FromSeconds(2), limit.Token);
        var flood = new TL.RpcException(420, "FLOOD_WAIT_X", 61);
        raw.SetException(flood);
        var result = await wait.WaitAsync(limit.Token);
        Equal(flood, result.Error);
        Equal(61, sharedWait);
        Equal(0, stats.ShortFloodWaitEvents);
        limit.Cancel();
        try { await queued; throw new Exception("Queued transfer ignored the shared wait."); }
        catch (OperationCanceledException) { }
        Equal(false, sent);
    }

    private static async Task TestShortWaitCancellationAsync()
    {
        var stats = new TransferStats();
        var gate = new ResizableTransferGate(1, 16);
        var clock = new TestClock();
        var policy = new PartRequestPolicy(stats, gate, (_, _) => throw new Exception("Unexpected long wait."), clock);
        var timeout = TimeSpan.FromSeconds(5);
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        using var cancelledWaiter = new CancellationTokenSource();
        var waiting = policy.RunAsync(5,
            () => Task.FromException<int>(new TL.RpcException(420, "FLOOD_WAIT_X", 1)),
            timeout, cancelledWaiter.Token);
        while (Volatile.Read(ref stats.WaitingPartRequests) != 1)
            await Task.Delay(5, limit.Token);
        cancelledWaiter.Cancel();
        try { await waiting.WaitAsync(limit.Token); throw new Exception("Cancelled cooldown waiter survived."); }
        catch (OperationCanceledException) when (cancelledWaiter.IsCancellationRequested && !limit.IsCancellationRequested) { }
        Equal(0, stats.WaitingPartRequests);

        // Cancel during the first RPC after the pause, then fault the next one.
        // Both must relinquish the single recovery permit.
        Interlocked.Exchange(ref clock.Ticks, TimeSpan.FromSeconds(1).Ticks);
        using var cancelledProbe = new CancellationTokenSource();
        var probeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var probe = policy.RunAsync(5, () =>
        {
            probeStarted.SetResult();
            return pending.Task;
        }, timeout, cancelledProbe.Token);
        await probeStarted.Task.WaitAsync(limit.Token);
        cancelledProbe.Cancel();
        var cancelled = await probe.WaitAsync(limit.Token);
        Equal(true, cancelled.Error is OperationCanceledException);
        pending.SetResult(0);
        var error = new IOException("broken connection");
        var failed = await policy.RunAsync(5, () => Task.FromException<int>(error), timeout, limit.Token);
        Equal(error, failed.Error);
        var recovered = await policy.RunAsync(5, () => Task.FromResult(7), timeout, limit.Token);
        Equal(true, recovered.Error is null);
        Equal(7, recovered.Value);
        Equal(0, stats.ActivePartRequests);
    }

    private sealed class TestClock : TimeProvider
    {
        public long Ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Interlocked.Read(ref Ticks);
    }

    private static void TestFloodWaitMerging()
    {
        var clock = new TestClock();
        var cooldown = new FloodWaitCoordinator(clock);
        Equal(TimeSpan.FromSeconds(60), cooldown.Defer(TimeSpan.FromSeconds(60)));
        Equal(TimeSpan.Zero, cooldown.Defer(TimeSpan.FromSeconds(60)));
        clock.Ticks += TimeSpan.FromSeconds(10).Ticks;
        Equal(TimeSpan.FromSeconds(50), cooldown.Remaining);
        Equal(TimeSpan.Zero, cooldown.Defer(TimeSpan.FromSeconds(20)));
        Equal(TimeSpan.FromSeconds(40), cooldown.Defer(TimeSpan.FromSeconds(90)));
        Equal(TimeSpan.FromSeconds(90), cooldown.Remaining);
        clock.Ticks += TimeSpan.FromSeconds(90).Ticks;
        Equal(TimeSpan.Zero, cooldown.Remaining);
    }

    private static async Task TestFloodWaitCancellationAsync()
    {
        var rpc = new TL.RpcException(420, "FLOOD_WAIT_X", 73);
        var result = await RequestDeadline.RunAsync<byte[]>(
            () => Task.FromException<byte[]>(rpc), TimeSpan.FromSeconds(1), CancellationToken.None);
        Equal(rpc, result.Error);
        Equal(73, ((TL.RpcException)result.Error!).X);
        Equal(false, DownloadEngine.IsTransientPartFailure(rpc, CancellationToken.None));
        var cooldown = new FloodWaitCoordinator();
        cooldown.Defer(TimeSpan.FromSeconds(74));
        using var cancellation = new CancellationTokenSource();
        var waiters = Enumerable.Range(0, 16).Select(_ => cooldown.WaitAsync(cancellation.Token)).ToArray();
        if (waiters.Any(task => task.IsCompleted)) throw new Exception("A worker bypassed Telegram's wait.");
        cancellation.Cancel();
        try { await Task.WhenAll(waiters).WaitAsync(TimeSpan.FromSeconds(1)); throw new Exception("Cancelled cooldown did not cancel."); }
        catch (OperationCanceledException) { }
    }

    private sealed class FakeMediaConnection(bool stalled)
    {
        public readonly List<TaskCompletionSource<byte[]>> Pending = [];
        public bool Closed;
        public Task<byte[]> ReadAsync(long offset)
        {
            if (Closed) throw new ObjectDisposedException(nameof(FakeMediaConnection));
            if (!stalled) return Task.FromResult(BitConverter.GetBytes(offset));
            var source = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (Pending) Pending.Add(source);
            return source.Task;
        }
        public void Close()
        {
            Closed = true;
            lock (Pending)
                foreach (var source in Pending)
                    source.TrySetException(new ObjectDisposedException(nameof(FakeMediaConnection)));
        }
    }

    private static async Task TestStalledRequestsAsync()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var releaseClose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connects = 0;
        var closes = 0;
        var old = new FakeMediaConnection(true);
        var shared = new SharedConnection<FakeMediaConnection>(
            () => Task.FromResult(Interlocked.Increment(ref connects) == 1 ? old : new FakeMediaConnection(false)),
            async client =>
            {
                Interlocked.Increment(ref closes);
                client.Close();
                await releaseClose.Task;
            });
        var client = await shared.GetAsync(limit.Token);
        var offsets = Enumerable.Range(0, 16).Select(i => i * 524288L).ToArray();
        var attempts = offsets.Select(offset => RequestDeadline.RunAsync(
            () => client.ReadAsync(offset), TimeSpan.FromMilliseconds(25), limit.Token)).ToArray();
        var results = await Task.WhenAll(attempts).WaitAsync(limit.Token);
        foreach (var result in results)
            if (result.Error is not TimeoutException) throw new Exception("Stall did not become a recoverable timeout result.");
        // All 16 workers report the same dead generation concurrently.
        var retired = await Task.WhenAll(offsets.Select(_ => Task.Run(() => shared.Retire(client))));
        Equal(1, retired.Count(value => value));
        Equal(1, closes);
        var replacements = offsets.Select(_ => shared.GetAsync(limit.Token)).ToArray();
        Equal(1, connects);
        if (replacements.Any(task => task.IsCompleted)) throw new Exception("Replacement escaped before shutdown completed.");
        releaseClose.SetResult();
        var newClients = await Task.WhenAll(replacements).WaitAsync(limit.Token);
        Equal(2, connects);
        foreach (var newClient in newClients)
            if (!ReferenceEquals(newClient, newClients[0])) throw new Exception("Parallel workers opened duplicate connections.");
        Equal(false, shared.Retire(old)); // Late old failures cannot kill the replacement.
        for (var i = 0; i < offsets.Length; i++)
        {
            var result = await RequestDeadline.RunAsync(() => newClients[i].ReadAsync(offsets[i]),
                TimeSpan.FromSeconds(1), limit.Token);
            if (result.Error is not null) throw result.Error;
            Equal(offsets[i], BitConverter.ToInt64(result.Value!));
        }
        if (old.Pending.Any(task => !task.Task.IsCompleted)) throw new Exception("Old requests were abandoned.");
    }

    private static async Task TestCancelledConnectionWaiterAsync()
    {
        var connected = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        var connects = 0;
        var shared = new SharedConnection<object>(() => { connects++; return connected.Task; }, _ => Task.CompletedTask);
        using var cancellation = new CancellationTokenSource();
        var first = shared.GetAsync(cancellation.Token);
        cancellation.Cancel();
        try { await first; throw new Exception("Cancelled waiter completed normally."); }
        catch (OperationCanceledException) { }
        var second = shared.GetAsync(CancellationToken.None);
        Equal(1, connects);
        var expected = new object();
        connected.SetResult(expected);
        Equal(expected, await second.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    private static async Task TestFailedConnectionAsync()
    {
        var connects = 0;
        var shared = new SharedConnection<object>(() => ++connects == 1
            ? Task.FromException<object>(new IOException("simulated connection failure"))
            : Task.FromResult(new object()), _ => Task.CompletedTask);
        try { await shared.GetAsync(CancellationToken.None); throw new Exception("Connection failure was lost."); }
        catch (IOException) { }
        await shared.GetAsync(CancellationToken.None);
        Equal(2, connects);
    }

    private static async Task TestRequestErrorsAsync()
    {
        var timeout = await RequestDeadline.RunAsync<byte[]>(
            () => Task.FromException<byte[]>(new TL.RpcException(500, "Timeout")),
            TimeSpan.FromSeconds(1), CancellationToken.None);
        Equal(true, DownloadEngine.IsTransientPartFailure(timeout.Error!, CancellationToken.None));
        using var cancellation = new CancellationTokenSource();
        var never = new TaskCompletionSource<byte[]>();
        var request = RequestDeadline.RunAsync(() => never.Task, TimeSpan.FromMinutes(1), cancellation.Token);
        cancellation.Cancel();
        var cancelled = await request.WaitAsync(TimeSpan.FromSeconds(1));
        if (cancelled.Error is not OperationCanceledException) throw new Exception("Cancellation did not stop waiting.");
        Equal(false, DownloadEngine.IsTransientPartFailure(cancelled.Error, cancellation.Token));
        never.SetException(new IOException("late RPC failure"));
    }

    private static async Task TestResizeAsync()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var gate = new ResizableTransferGate(3, 16);
        var leases = new[] { await gate.EnterAsync(limit.Token), await gate.EnterAsync(limit.Token), await gate.EnterAsync(limit.Token) };
        await gate.SetLimitAsync(1, limit.Token).WaitAsync(TimeSpan.FromSeconds(1));
        using var cancellation = new CancellationTokenSource();
        var cancelledWaiter = gate.EnterAsync(cancellation.Token).AsTask();
        cancellation.Cancel();
        try { await cancelledWaiter; throw new Exception("Cancelled gate waiter entered."); }
        catch (OperationCanceledException) { }
        var waiter = gate.EnterAsync(limit.Token).AsTask();
        await leases[0].DisposeAsync();
        await leases[1].DisposeAsync();
        if (waiter.IsCompleted) throw new Exception("Shrinking gate admitted excess work.");
        await leases[2].DisposeAsync();
        await using var last = await waiter.WaitAsync(limit.Token);
        await gate.SetLimitAsync(3, limit.Token);
        await using var extra1 = await gate.EnterAsync(limit.Token);
        await using var extra2 = await gate.EnterAsync(limit.Token);
    }

    private static async Task TestCheckpointTrackingAsync()
    {
        const int part = 512 * 1024;
        var tracker = new PartialProgressTracker(part * 3L, part, 0);
        tracker.Complete(part, part);
        Equal(0L, tracker.ContiguousBytes);
        tracker.Complete(0, part);
        Equal(part * 2L, tracker.ContiguousBytes);
        tracker.Complete(part * 2L, part);
        Equal(part * 3L, tracker.ContiguousBytes);

        var directory = Directory.CreateTempSubdirectory("TelegramVideoDownloader-Checkpoint-");
        var path = Path.Combine(directory.FullName, "test.part");
        var sidecar = path + ".state.json";
        try
        {
            var state = new PartialDownloadState
            {
                VideoKey = "test", PartSize = part, ExpectedSize = part * 2L + 1, UpdatedUtc = DateTime.UtcNow
            };
            await JsonFiles.SaveAtomicAsync(sidecar, state, CancellationToken.None);
            var parked = PartialFileAttempt.Open(path, state);
            await using (parked.Stream)
            {
                // The middle part has stalled. Both surrounding writes succeeded.
                await RandomAccess.WriteAsync(parked.Stream.SafeFileHandle, new byte[part], 0);
                parked.Tracker.Complete(0, part);
                await RandomAccess.WriteAsync(parked.Stream.SafeFileHandle, new byte[] { 37 }, 2L * part);
                parked.Tracker.Complete(2L * part, 1);
                var writer = new PartialCheckpointWriter(sidecar, state, parked.Tracker);
                using (File.Open(sidecar + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    try
                    {
                        await writer.FlushAsync(CancellationToken.None);
                        throw new Exception("Locked checkpoint unexpectedly saved.");
                    }
                    catch (IOException) { }
                }
                Equal(0L, state.ContiguousBytes);
                await writer.FlushAsync(CancellationToken.None);
                Equal((long)part, state.ContiguousBytes);
                Equal(true, await parked.CanResumeAsync(path, sidecar, CancellationToken.None));
                Equal(part + 1L, parked.Tracker.CompletedBytes);
                Equal((long)part, parked.Tracker.MissingOffsets().Single());
                var tail = new byte[1];
                Equal(1, await RandomAccess.ReadAsync(parked.Stream.SafeFileHandle, tail, 2L * part));
                Equal((byte)37, tail[0]);

                var edited = await JsonFiles.LoadAsync(sidecar, () => new PartialDownloadState(), CancellationToken.None);
                edited.UpdatedUtc = edited.UpdatedUtc.AddSeconds(1);
                await JsonFiles.SaveAtomicAsync(sidecar, edited, CancellationToken.None);
                Equal(false, await parked.CanResumeAsync(path, sidecar, CancellationToken.None));
            }

            // A changed sidecar or a fresh process only trusts the saved prefix.
            var durable = await JsonFiles.LoadAsync(sidecar, () => new PartialDownloadState(), CancellationToken.None);
            var cold = PartialFileAttempt.Open(path, durable);
            await using (cold.Stream)
            {
                Equal((long)part, cold.Stream.Length);
                Equal((long)part, cold.Tracker.CompletedBytes);
                Equal(true, cold.Tracker.MissingOffsets().SequenceEqual(new long[] { part, 2L * part }));
            }
        }
        finally
        {
            File.Delete(sidecar + ".tmp");
            File.Delete(sidecar);
            File.Delete(path);
            directory.Delete();
        }
    }

    private static void TestFilenameSafety()
    {
        var name = FileNames.SanitizeFilename(" video?.mp4. ");
        if (name.Contains('?') || name.EndsWith('.'))
            throw new InvalidOperationException($"Unsafe filename: {name}");
        var key = FileNames.BuildVideoKey(12, 34, 56);
        Equal("12:34:56", key);
    }

    private static void TestFileReferenceSnapshotRefresh()
    {
        var video = VideoItem.FromDocument(
            10,
            20,
            new TL.Document
            {
                id = 30,
                access_hash = 40,
                file_reference = [1, 2, 3],
                dc_id = 2,
                size = 50,
                mime_type = "video/mp4",
                attributes = []
            });
        var before = video.CreateLocationSnapshot();
        if (video.IsReferenceStale(DateTime.UtcNow, TimeSpan.FromHours(1)))
            throw new InvalidOperationException("A newly captured reference was considered stale.");
        video.RefreshLocation(
            new TL.Document
            {
                id = 30,
                access_hash = 41,
                file_reference = [4, 5, 6],
                dc_id = 3,
                size = 50,
                mime_type = "video/mp4",
                attributes = []
            });
        var after = video.CreateLocationSnapshot();

        Equal(0, before.Version);
        Equal(1, after.Version);
        Equal(2, before.DcId);
        Equal(3, after.DcId);
        Equal((byte)1, before.Location.file_reference[0]);
        Equal((byte)4, after.Location.file_reference[0]);

        var cached = CatalogVideoEntry.FromDocument(
            20,
            new TL.Document
            {
                id = 30,
                access_hash = 41,
                file_reference = [4, 5, 6],
                dc_id = 3,
                size = 50,
                mime_type = "video/mp4",
                attributes = []
            });
        cached.ReferenceUpdatedUtc = default;
        var oldVideo = VideoItem.FromCatalog(10, cached);
        if (!oldVideo.IsReferenceStale(DateTime.UtcNow, TimeSpan.FromHours(1)))
            throw new InvalidOperationException("A legacy cached reference was considered fresh.");
    }

    private static void TestServerSaltRetryClassification()
    {
        var serverSaltChanged = new TL.RpcException(
            -503,
            "BadMsgNotification 48");
        if (!DownloadEngine.IsTransientPartFailure(
                serverSaltChanged,
                CancellationToken.None))
        {
            throw new InvalidOperationException(
                "A corrected Telegram server salt was not retried.");
        }

        var unrelatedBadMessage = new TL.RpcException(
            -503,
            "BadMsgNotification 18");
        if (DownloadEngine.IsTransientPartFailure(
                unrelatedBadMessage,
                CancellationToken.None))
        {
            throw new InvalidOperationException(
                "An unsupported bad-message notification was retried.");
        }
    }

    private static async Task TestJsonRoundTripAsync()
    {
        var directory = Path.Combine(
            Path.GetTempPath(),
            "TelegramVideoDownloader-SelfTest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "state.json");
        try
        {
            var expected = new PartialDownloadState
            {
                VideoKey = "test",
                ExpectedSize = 123,
                PartSize = 512 * 1024,
                ContiguousBytes = 100
            };
            await JsonFiles.SaveAtomicAsync(path, expected, CancellationToken.None);
            var actual = await JsonFiles.LoadAsync(
                path,
                () => new PartialDownloadState(),
                CancellationToken.None);
            Equal(expected.VideoKey, actual.VideoKey);
            Equal(expected.ContiguousBytes, actual.ContiguousBytes);
        }
        finally
        {
            var resolved = Path.GetFullPath(directory);
            var tempRoot = Path.GetFullPath(Path.GetTempPath());
            if (resolved.StartsWith(tempRoot, StringComparison.OrdinalIgnoreCase) &&
                Path.GetFileName(resolved).StartsWith(
                    "TelegramVideoDownloader-SelfTest-",
                    StringComparison.Ordinal))
                Directory.Delete(resolved, recursive: true);
        }
    }

    private static void Test(string name, Action action, List<string> failures)
    {
        try
        {
            action();
        }
        catch (Exception ex)
        {
            failures.Add($"{name}: {ex.Message}");
        }
    }

    private static async Task TestAsync(
        string name,
        Func<Task> action,
        List<string> failures)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            failures.Add($"{name}: {ex.Message}");
        }
    }

    private static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new InvalidOperationException($"Expected {expected}; got {actual}.");
    }
}
