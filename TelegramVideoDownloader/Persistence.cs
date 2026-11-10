using System.Text.Json;

internal static class JsonFiles
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    public static async Task<T> LoadAsync<T>(
        string path,
        Func<T> createDefault,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
            return createDefault();

        try
        {
            await using var stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                64 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            return await JsonSerializer.DeserializeAsync<T>(stream, Options, cancellationToken)
                ?? createDefault();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Ui.Print($"Warning: Could not read {Path.GetFileName(path)}: {ex.Message}");
            return createDefault();
        }
    }

    public static async Task SaveAtomicAsync<T>(
        string path,
        T value,
        CancellationToken cancellationToken,
        bool flushToDisk = true)
    {
        var temporaryPath = path + ".tmp";
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using (var stream = new FileStream(
                         temporaryPath,
                         FileMode.Create,
                         FileAccess.Write,
                         FileShare.None,
                         64 * 1024,
                         FileOptions.Asynchronous | FileOptions.SequentialScan))
        {
            await JsonSerializer.SerializeAsync(stream, value, Options, cancellationToken);
            await stream.FlushAsync(cancellationToken);
            if (flushToDisk)
                stream.Flush(flushToDisk: true);
        }

        File.Move(temporaryPath, path, overwrite: true);
    }
}

internal sealed class BatchedSaveCoordinator : IAsyncDisposable
{
    private readonly Func<Task> _save;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _wake = new(0, 1);
    private readonly SemaphoreSlim _saveLock = new(1, 1);
    private readonly Task _loop;
    private int _dirtyCount;

    public BatchedSaveCoordinator(Func<Task> save)
    {
        _save = save;
        _loop = RunAsync();
    }

    public void MarkDirty(bool forceSoon = false)
    {
        var count = Interlocked.Increment(ref _dirtyCount);
        if ((forceSoon || count >= 10) && _wake.CurrentCount == 0)
        {
            try
            {
                _wake.Release();
            }
            catch (SemaphoreFullException)
            {
            }
        }
    }

    public async Task FlushAsync()
    {
        if (Interlocked.Exchange(ref _dirtyCount, 0) == 0)
            return;

        await _saveLock.WaitAsync();
        try
        {
            await _save();
        }
        catch
        {
            Interlocked.Increment(ref _dirtyCount);
            throw;
        }
        finally
        {
            _saveLock.Release();
        }
    }

    private async Task RunAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                await _wake.WaitAsync(AppConstants.BatchedSaveInterval, _stop.Token);
                await FlushAsync();
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Ui.Print($"Warning: Background state save failed: {ex.Message}");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try
        {
            await _loop;
        }
        catch (OperationCanceledException)
        {
        }

        await FlushAsync();
        _stop.Dispose();
        _wake.Dispose();
        _saveLock.Dispose();
    }
}
