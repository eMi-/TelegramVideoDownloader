using System.Collections.Concurrent;
using TL;

internal sealed class PartRequestPolicy(
    TransferStats stats,
    ResizableTransferGate transferGate,
    Func<int, CancellationToken, Task> registerLongWait,
    TimeProvider? timeProvider = null)
{
    public const int ShortWaitThresholdSeconds = 60;
    private readonly ConcurrentDictionary<int, DcAdmission> _dataCenters = new();
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    public bool IsRecovering(int dcId) =>
        _dataCenters.TryGetValue(dcId, out var dc) && dc.DelayNewFiles;

    public bool IsSleeping(int dcId) =>
        _dataCenters.TryGetValue(dcId, out var dc) && dc.IsSleeping;

    public string DescribeWaits() => string.Join(", ", _dataCenters.OrderBy(pair => pair.Key)
        .Select(pair => pair.Value.Describe()).Where(description => description is not null));

    public async Task<RequestResult<T>> RunAsync<T>(
        int dcId, Func<Task<T>> operation, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var dc = _dataCenters.GetOrAdd(dcId, static (id, state) => new DcAdmission(id, state.Time, state.Stats),
            (Time: _time, Stats: stats));
        while (true)
        {
            await stats.FloodCooldown.WaitAsync(cancellationToken);
            await dc.WaitUntilAvailableAsync(cancellationToken);
            await using var transfer = await transferGate.EnterAsync(cancellationToken);
            // A peer's response may have closed admission while we waited for a permit.
            if (stats.FloodCooldown.Remaining > TimeSpan.Zero || !dc.TryStart(out var generation))
                continue;

            RpcException? shortWait = null;
            bool succeeded = false;
            Interlocked.Increment(ref stats.ActivePartRequests);
            try
            {
                var result = await RequestDeadline.RunAsync(operation, timeout, cancellationToken);
                succeeded = result.Error is null;
                if (result.Error is not RpcException { Code: 420, X: > 0 } flood)
                    return result;

                if (flood.X > ShortWaitThresholdSeconds)
                {
                    // Publish the shared pause before another worker gets this permit.
                    await registerLongWait(flood.X, cancellationToken);
                    return result;
                }
                shortWait = flood;
            }
            finally
            {
                Interlocked.Decrement(ref stats.ActivePartRequests);
                dc.Finish(generation, succeeded, shortWait, transferGate.Limit);
            }
            // Dispose the global permit before waiting or retrying the same offset.
        }
    }

    private sealed class DcAdmission(int dcId, TimeProvider time, TransferStats stats)
    {
        private readonly object _sync = new();
        private readonly FloodWaitCoordinator _cooldown = new(time);
        private TaskCompletionSource? _changed;
        private int _inFlight;
        private int _waiting;
        private int _limit = AppConstants.MaximumTransferOperations;
        private int _generation;
        private int _successfulParts;
        private long _lastIncrease;
        private long? _lastReport;

        // An idle DC whose wait has expired may take a new file. Its first
        // requests still obey the reduced limit until enough parts succeed.
        public bool DelayNewFiles
        {
            get
            {
                lock (_sync) return _limit < AppConstants.MaximumTransferOperations &&
                    (_inFlight > 0 || Volatile.Read(ref _waiting) > 0 || _cooldown.Remaining > TimeSpan.Zero);
            }
        }

        public bool IsSleeping
        {
            get
            {
                lock (_sync) return _inFlight == 0 && _cooldown.Remaining > TimeSpan.Zero;
            }
        }

        public string? Describe()
        {
            lock (_sync)
            {
                if (!DelayNewFiles) return null;
                var remaining = _cooldown.Remaining;
                return remaining > TimeSpan.Zero
                    ? $"DC {dcId} wait {Math.Ceiling(remaining.TotalSeconds):0}s"
                    : $"DC {dcId} recovering (max {_limit} in flight)";
            }
        }

        public async Task WaitUntilAvailableAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                Task wait;
                lock (_sync)
                {
                    if (_cooldown.Remaining > TimeSpan.Zero)
                        wait = _cooldown.WaitAsync(cancellationToken);
                    else if (_inFlight >= _limit)
                    {
                        _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                        wait = _changed.Task.WaitAsync(cancellationToken);
                    }
                    else
                        return;
                }

                Interlocked.Increment(ref _waiting);
                Interlocked.Increment(ref stats.WaitingPartRequests);
                try { await wait; }
                finally
                {
                    Interlocked.Decrement(ref stats.WaitingPartRequests);
                    Interlocked.Decrement(ref _waiting);
                }
            }
        }

        public bool TryStart(out int generation)
        {
            lock (_sync)
            {
                generation = _generation;
                if (_cooldown.Remaining > TimeSpan.Zero || _inFlight >= _limit)
                    return false;
                _inFlight++;
                return true;
            }
        }

        public void Finish(int generation, bool succeeded, RpcException? flood, int globalLimit)
        {
            bool report = false;
            lock (_sync)
            {
                _inFlight--;
                var now = time.GetTimestamp();
                if (flood is not null)
                {
                    _cooldown.Defer(TimeSpan.FromSeconds(flood.X));
                    _limit = 1;
                    _generation++;
                    _successfulParts = 0;
                    _lastIncrease = now;
                    Interlocked.Increment(ref stats.ShortFloodWaitEvents);
                    Interlocked.Add(ref stats.ShortFloodWaitSeconds, flood.X);
                    report = _lastReport is null ||
                        time.GetElapsedTime(_lastReport.Value, now) >= TimeSpan.FromSeconds(15);
                    if (report) _lastReport = now;
                }
                else if (succeeded && generation == _generation && _limit < AppConstants.MaximumTransferOperations)
                {
                    // Only responses to post-cooldown requests can reopen this DC.
                    // Grow one slot at a time once the reduced rate proves sustainable.
                    _successfulParts++;
                    if (_successfulParts >= 4 * _limit &&
                        time.GetElapsedTime(_lastIncrease, now) >= TimeSpan.FromSeconds(2))
                    {
                        // Once this DC can sustain the shared limit, future tuner
                        // changes should apply to it like any other healthy DC.
                        _limit = _limit + 1 >= globalLimit ? AppConstants.MaximumTransferOperations : _limit + 1;
                        _successfulParts = 0;
                        _lastIncrease = now;
                    }
                }
                _changed?.TrySetResult();
                _changed = null;
            }

            if (report)
                Ui.Print($"[WAIT] media DC {dcId} | {flood!.Message} | {flood.X}s requested. " +
                    "Downloads from this DC resume with one request. Other DCs can continue.");
        }
    }
}
