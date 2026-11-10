// Connection tasks and retirement belong to the DC, not to an individual part.
internal sealed class SharedConnection<T>(Func<Task<T>> connect, Func<T, Task> close)
    where T : class
{
    private readonly object _sync = new();
    private Task<T>? _current;
    private Task _retirement = Task.CompletedTask;

    public Task<T> GetAsync(CancellationToken cancellationToken)
    {
        Task<T> current;
        lock (_sync)
        {
            if (_current is null || _current.IsFaulted || _current.IsCanceled)
            {
                _current = ConnectAsync(_retirement);
                RequestDeadline.Observe(_current);
            }
            current = _current;
        }
        return current.WaitAsync(cancellationToken);
    }

    public bool Retire(T observed)
    {
        lock (_sync)
        {
            if (_current is not { IsCompletedSuccessfully: true } ||
                !ReferenceEquals(_current.Result, observed))
                return false;
            // Replacement must wait for shutdown; late failures from an old
            // generation must never close the new connection.
            _retirement = CloseAsync(observed);
            RequestDeadline.Observe(_retirement);
            _current = null;
            return true;
        }
    }

    private async Task<T> ConnectAsync(Task retirement)
    {
        await retirement.ConfigureAwait(false);
        return await connect().ConfigureAwait(false);
    }

    private async Task CloseAsync(T value) => await close(value).ConfigureAwait(false);
}

internal readonly record struct RequestResult<T>(T? Value, Exception? Error)
{
    public static RequestResult<T> Success(T value) => new(value, null);
    public static RequestResult<T> Failure(Exception error) => new(default, error);
}

internal static class RequestDeadline
{
    // Local deadlines are recovery signals, handled before crossing the
    // parallel worker boundary. A deadline alone does NOT cancel the RPC.
    public static async Task<RequestResult<T>> RunAsync<T>(
        Func<Task<T>> operation, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task<T>? request = null;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            request = operation();
            using var deadlineCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var deadline = Task.Delay(timeout, deadlineCancellation.Token);
            await Task.WhenAny(request, deadline).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (request.IsCompleted)
            {
                await deadlineCancellation.CancelAsync();
                // Inspect an already-completed task without rethrowing its RPC
                // exception. Expected Telegram responses are policy inputs.
                if (request.IsFaulted)
                    return RequestResult<T>.Failure(request.Exception!.InnerExceptions[0]);
                if (request.IsCanceled)
                    return RequestResult<T>.Failure(new TaskCanceledException(request));
                return RequestResult<T>.Success(request.Result);
            }
            Observe(request);
            return RequestResult<T>.Failure(new TimeoutException("Telegram part request deadline exceeded."));
        }
        catch (Exception ex)
        {
            if (request is not null) Observe(request);
            return RequestResult<T>.Failure(ex);
        }
    }

    public static void Observe(Task task) => _ = task.ContinueWith(
        static failed => { _ = failed.Exception; }, CancellationToken.None,
        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
        TaskScheduler.Default);
}
