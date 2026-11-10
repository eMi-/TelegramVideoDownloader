internal sealed class ResizableTransferGate
{
    private readonly int _maximum;
    private readonly object _sync = new();
    private TaskCompletionSource _changed = NewSignal();
    private int _active;
    private int _limit;

    public ResizableTransferGate(int initialLimit, int maximum)
    {
        _maximum = maximum;
        _limit = Math.Clamp(initialLimit, 1, maximum);
    }

    public int Limit => Volatile.Read(ref _limit);

    public async ValueTask<IAsyncDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Task changed;
            lock (_sync)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_active < _limit)
                {
                    _active++;
                    return new Releaser(this);
                }
                changed = _changed.Task;
            }
            await changed.WaitAsync(cancellationToken);
        }
    }

    public Task SetLimitAsync(int requestedLimit, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            // Shrinking never waits for stalled requests or consumes permits.
            // Existing work drains naturally before new requests can enter.
            Volatile.Write(ref _limit, Math.Clamp(requestedLimit, 1, _maximum));
            Pulse();
        }
        return Task.CompletedTask;
    }

    private void Exit()
    {
        lock (_sync)
        {
            _active--;
            Pulse();
        }
    }

    private void Pulse()
    {
        var previous = _changed;
        _changed = NewSignal();
        previous.TrySetResult();
    }

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Releaser(ResizableTransferGate owner) : IAsyncDisposable
    {
        private int _released;
        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) owner.Exit();
            return ValueTask.CompletedTask;
        }
    }
}

internal sealed class TransferAutoTuner(
    ResizableTransferGate gate,
    TransferStats stats,
    PerformanceSettings settings)
{
    public int? SelectedTransfers { get; private set; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (!settings.AutoTune)
            return;

        // Short burst measurements at 12-16 requests can look fast and then
        // cause long Telegram stalls. Measure the configured baseline first,
        // then alternatives; stop when recovery makes comparisons unreliable.
        var candidates = new[] { Math.Clamp(settings.ParallelTransfers, 4, 8), 4, 6, 8 }
            .Where(value => value <= AppConstants.MaximumTransferOperations)
            .Distinct()
            .ToArray();
        var measurements = new List<(int Transfers, double Speed, double Score)>();
        var initialTimeouts = Volatile.Read(ref stats.RpcTimeouts);
        var initialReconnects = Volatile.Read(ref stats.DcReconnects);
        var initialFloodWaits = Volatile.Read(ref stats.FloodWaitEvents);
        var initialShortWaits = Volatile.Read(ref stats.ShortFloodWaitEvents);

        try
        {
            foreach (var candidate in candidates)
            {
                if (Volatile.Read(ref stats.RpcTimeouts) != initialTimeouts ||
                    Volatile.Read(ref stats.FloodWaitEvents) != initialFloodWaits ||
                    Volatile.Read(ref stats.DcReconnects) != initialReconnects)
                    return;
                await gate.SetLimitAsync(candidate, cancellationToken);
                Volatile.Write(ref stats.ActiveTransferLimit, candidate);
                var startBytes = Interlocked.Read(ref stats.NetworkBytes);
                var startPartRetries = Volatile.Read(ref stats.PartRetries);
                var startRpcTimeouts = Volatile.Read(ref stats.RpcTimeouts);
                var start = System.Diagnostics.Stopwatch.GetTimestamp();
                await Task.Delay(AppConstants.AutoTuneSampleInterval, cancellationToken);
                var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(start).TotalSeconds;
                var bytes = Interlocked.Read(ref stats.NetworkBytes) - startBytes;
                var newPartRetries = Math.Max(
                    0,
                    Volatile.Read(ref stats.PartRetries) - startPartRetries);
                var newRpcTimeouts = Math.Max(
                    0,
                    Volatile.Read(ref stats.RpcTimeouts) - startRpcTimeouts);
                var actualLimit = gate.Limit;
                if (Volatile.Read(ref stats.RpcTimeouts) != initialTimeouts ||
                    Volatile.Read(ref stats.FloodWaitEvents) != initialFloodWaits ||
                    Volatile.Read(ref stats.DcReconnects) != initialReconnects)
                {
                    Ui.Print("[TUNE] Recovery active; keeping the reduced transfer limit.");
                    return;
                }
                if (Volatile.Read(ref stats.ShortFloodWaitEvents) != initialShortWaits)
                {
                    // Short server waits are already honored per DC. They
                    // invalidate speed comparisons, not the configured limit.
                    Ui.Print($"[TUNE] Short request waits detected; keeping {actualLimit} transfers.");
                    return;
                }
                if (elapsed <= 0 || bytes < 2L * 1024 * 1024)
                {
                    if (newRpcTimeouts > 0 || actualLimit < candidate)
                    {
                        Ui.Print("[TUNE] Telegram pressure detected; not testing higher concurrency.");
                        break;
                    }

                    continue;
                }

                var speed = bytes / elapsed;
                var score = speed / (1 + newPartRetries * 0.10 + newRpcTimeouts * 2.0);
                measurements.Add((actualLimit, speed, score));
                Ui.Print(
                    $"[TUNE] {actualLimit} transfers measured " +
                    $"{Ui.FormatBytes((long)speed)}/s | retries {newPartRetries}");

                if (newRpcTimeouts > 0 || actualLimit < candidate)
                {
                    Ui.Print("[TUNE] Telegram pressure detected; not testing higher concurrency.");
                    break;
                }
            }

            if (measurements.Count == 0)
                return;
            var best = measurements.MaxBy(sample => sample.Score);
            await gate.SetLimitAsync(best.Transfers, cancellationToken);
            Volatile.Write(ref stats.ActiveTransferLimit, best.Transfers);
            SelectedTransfers = best.Transfers;
            Ui.Print(
                $"[TUNE] Selected {best.Transfers} transfers at " +
                $"{Ui.FormatBytes((long)best.Speed)}/s.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
