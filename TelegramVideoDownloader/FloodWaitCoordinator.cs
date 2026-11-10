// One monotonic deadline shared by the download workers. Concurrent FLOOD_WAIT
// replies extend to the latest deadline; they do not add their durations.
internal sealed class FloodWaitCoordinator(TimeProvider? timeProvider = null)
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly object _sync = new();
    private long _until;

    public TimeSpan Remaining
    {
        get
        {
            lock (_sync)
                return TimeSpan.FromSeconds(Math.Max(0, _until - _time.GetTimestamp()) /
                    (double)_time.TimestampFrequency);
        }
    }

    public TimeSpan Defer(TimeSpan duration)
    {
        lock (_sync)
        {
            var now = _time.GetTimestamp();
            var next = now + (long)(Math.Max(0, duration.TotalSeconds) * _time.TimestampFrequency);
            var added = Math.Max(0, next - Math.Max(now, _until));
            _until = Math.Max(_until, next);
            return TimeSpan.FromSeconds(added / (double)_time.TimestampFrequency);
        }
    }

    public async Task WaitAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = Remaining;
            if (remaining <= TimeSpan.Zero) return;
            // Recheck after sleeping: another response may have extended it.
            await Task.Delay(remaining < TimeSpan.FromHours(1) ? remaining : TimeSpan.FromHours(1),
                _time, cancellationToken);
        }
    }
}
