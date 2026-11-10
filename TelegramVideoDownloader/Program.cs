using System.Diagnostics;
using System.Threading.Channels;
using TL;
using WTelegram;

internal static class Program
{

    private static async Task Main(string[] args)
    {
        if (args.Length == 1 && args[0].Equals("--self-test", StringComparison.OrdinalIgnoreCase))
        {
            await SelfTests.RunAsync();
            return;
        }

        SessionArguments options;
        try
        {
            options = SessionArguments.Parse(args);
        }
        catch (Exception ex) when (ex is ArgumentException or IOException or NotSupportedException)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine(SessionArguments.Usage);
            Environment.ExitCode = 2;
            return;
        }

        Console.Title = "Telegram Video Downloader";
        WTelegram.Helpers.Log = (_, _) => { };

        using var cancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Ui.Print("\nStopping... completed downloads and resumable parts will be preserved.");
            cancellation.Cancel();
        };

        Ui.PrintHeader();
        if (Debugger.IsAttached)
            Ui.Print("[DEBUG] For unattended downloads, start with Ctrl+F5. A debugger exception break pauses all recovery work.");
        try
        {
            await RunAsync(options, cancellation.Token);
        }
        catch (ArgumentOutOfRangeException ex) when (ex.ParamName == "channelNumber")
        {
            Console.Error.WriteLine(ex.Message);
            Environment.ExitCode = 2;
            return;
        }
        catch (OperationCanceledException)
        {
            Ui.Print("\nOperation cancelled.");
        }
        catch (Exception ex)
        {
            Ui.Print($"\nFATAL ERROR:\n{ex}");
        }

        Console.WriteLine();
        Console.WriteLine("Press ENTER to exit...");
        Console.ReadLine();
    }

    private static async Task RunAsync(SessionArguments options, CancellationToken cancellationToken)
    {
        await using var client = new Client(Config);
        client.FloodRetryThreshold = AppConstants.FloodRetryThresholdSeconds;

        Console.WriteLine("Connecting to Telegram...");
        var me = await client.LoginUserIfNeeded()
            .WaitAsync(AppConstants.LoginTimeout, cancellationToken);

        Console.WriteLine();
        Console.WriteLine($"Logged in as: {me.first_name} {me.last_name}".Trim());
        Console.WriteLine($"Telegram User ID: {me.id}");
        Console.WriteLine();

        var selectedChat = await SelectChatAsync(client, options.ChannelNumber, cancellationToken);
        InputPeer peer = selectedChat;
        var chatId = selectedChat.ID;
        var chatTitle = Ui.GetChatTitle(selectedChat);

        Console.WriteLine();
        Console.WriteLine($"Selected: {chatTitle}");
        Console.WriteLine();
        Console.WriteLine("Choose where the videos should be saved...");

        var downloadFolder = options.Destination ?? Ui.ChooseDownloadFolder();
        if (string.IsNullOrWhiteSpace(downloadFolder))
        {
            Console.WriteLine("No folder selected. Cancelled.");
            return;
        }

        Directory.CreateDirectory(downloadFolder);
        Console.WriteLine();
        Console.WriteLine($"Download directory:\n{downloadFolder}");

        var paths = new AppPaths(downloadFolder);
        var downloadIndex = await JsonFiles.LoadAsync(
            paths.IndexPath,
            () => new DownloadIndex(),
            cancellationToken);
        var catalog = await JsonFiles.LoadAsync(
            paths.CatalogPath,
            () => new CatalogStore(),
            cancellationToken);
        var settings = await JsonFiles.LoadAsync(
            paths.SettingsPath,
            PerformanceSettings.CreateDefault,
            cancellationToken);

        settings.ApplyEnvironmentOverrides();
        settings.Normalize();
        await JsonFiles.SaveAtomicAsync(paths.SettingsPath, settings, cancellationToken);

        var chatCatalog = catalog.Channels.GetOrAdd(
            chatId.ToString(),
            _ => new ChannelCatalog
            {
                ChatId = chatId,
                ChatTitle = chatTitle
            });
        chatCatalog.ChatTitle = chatTitle;

        var fullScanRequired = chatCatalog.Videos.IsEmpty ||
            DateTime.UtcNow - chatCatalog.LastFullScanUtc >= AppConstants.FullScanInterval;
        var scanMode = fullScanRequired
            ? "server-filtered full video scan"
            : $"incremental scan after message {chatCatalog.LastScannedMessageId:N0}";

        Console.WriteLine();
        Console.WriteLine("============================================");
        Console.WriteLine("OPTIMIZED DOWNLOAD SESSION");
        Console.WriteLine("============================================");
        Console.WriteLine();
        Console.WriteLine($"Scan mode            : {scanMode}");
        Console.WriteLine($"Cached video messages: {chatCatalog.Videos.Count:N0}");
        Console.WriteLine($"Performance profile  : {settings.ProfileName}");
        Console.WriteLine($"Part size            : {Ui.FormatBytes(settings.PartSizeBytes)}");
        Console.WriteLine($"Transfer operations  : {settings.ParallelTransfers}");
        Console.WriteLine($"Video scheduling     : up to {settings.MaxConcurrentVideos} small files");
        Console.WriteLine($"Automatic tuning     : {(settings.AutoTune ? "enabled" : "disabled")}");
        Console.WriteLine($"Stall recovery        : reconnect media DC after {AppConstants.PartRequestTimeout.TotalSeconds:0}s per stalled part");
        Console.WriteLine("Short Telegram waits : pause the affected DC, then try one part");
        Console.WriteLine();
        Console.Write("Start scanning and downloading? [Y/N]: ");

        var answer = options.ConfirmStart ? "Y" : Console.ReadLine();
        if (options.ConfirmStart)
            Console.WriteLine(answer);
        if (!string.Equals(answer, "Y", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("Cancelled.");
            return;
        }

        Console.WriteLine();
        Console.WriteLine("Indexing existing destination files once...");
        var diskWatch = Stopwatch.StartNew();
        var existingFiles = FileDiscovery.BuildExistingFileMap(downloadFolder);
        diskWatch.Stop();
        var physicalFilesMatched = existingFiles.Values
            .SelectMany(files => files)
            .Select(file => file.FullName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        Console.WriteLine($"Physical files matched : {physicalFilesMatched:N0}");
        Console.WriteLine($"Candidate document IDs: {existingFiles.Count:N0}");
        Console.WriteLine($"Disk indexing time    : {Ui.FormatDuration(diskWatch.Elapsed)}");

        await using var indexSaver = new BatchedSaveCoordinator(
            () => JsonFiles.SaveAtomicAsync(paths.IndexPath, downloadIndex, CancellationToken.None));
        await using var catalogSaver = new BatchedSaveCoordinator(
            () => JsonFiles.SaveAtomicAsync(paths.CatalogPath, catalog, CancellationToken.None));

        var stats = new TransferStats
        {
            ActiveTransferLimit = settings.ParallelTransfers
        };
        var transferGate = new ResizableTransferGate(
            settings.ParallelTransfers,
            AppConstants.MaximumTransferOperations);
        using var downloader = new DownloadEngine(
            client,
            selectedChat,
            chatCatalog,
            downloadIndex,
            indexSaver,
            catalogSaver,
            transferGate,
            settings,
            stats);

        var queue = System.Threading.Channels.Channel.CreateUnbounded<VideoItem>(
            new UnboundedChannelOptions
            {
                SingleWriter = true,
                SingleReader = true,
                AllowSynchronousContinuations = false
            });

        var sessionWatch = Stopwatch.StartNew();
        using var backgroundCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var statusTask = Ui.ShowStatusLoopAsync(
            stats,
            sessionWatch,
            downloader.DescribeMediaWaits,
            backgroundCancellation.Token);
        var tuner = new TransferAutoTuner(transferGate, stats, settings);
        var tunerTask = tuner.RunAsync(backgroundCancellation.Token);

        var seenDocuments = new HashSet<string>(StringComparer.Ordinal);
        var producerTask = ProduceDownloadsAsync(
            client,
            peer,
            chatId,
            chatTitle,
            chatCatalog,
            fullScanRequired,
            existingFiles,
            downloadIndex,
            indexSaver,
            catalogSaver,
            queue.Writer,
            seenDocuments,
            stats,
            downloadFolder,
            cancellationToken);

        var consumerTask = DcFileDispatcher.RunAsync(queue.Reader, settings.MaxConcurrentVideos,
            downloader.IsMediaDcRecovering, downloader.DownloadAsync, cancellationToken,
            canYield: downloader.CanYieldMediaDc);

        ScanResult scanResult;
        try
        {
            await Task.WhenAll(producerTask, consumerTask);
            scanResult = await producerTask;
        }
        catch
        {
            backgroundCancellation.Cancel();
            await ObserveFailureAsync(producerTask);
            await ObserveFailureAsync(consumerTask);
            throw;
        }
        finally
        {
            backgroundCancellation.Cancel();
            await IgnoreCancellationAsync(statusTask);
            await IgnoreCancellationAsync(tunerTask);
            await indexSaver.FlushAsync();
            await catalogSaver.FlushAsync();
            sessionWatch.Stop();
        }

        // A temporary recovery throttle is not a measured preference for the
        // next run. Persist only a completed tuning result, or the input setting.
        settings.ParallelTransfers = tuner.SelectedTransfers ?? settings.ParallelTransfers;
        settings.ProfileName = settings.AutoTune
            ? $"Auto ({settings.ParallelTransfers} transfers)"
            : settings.ProfileName;
        settings.LastSessionAverageBytesPerSecond = sessionWatch.Elapsed.TotalSeconds > 0
            ? Interlocked.Read(ref stats.NetworkBytes) / sessionWatch.Elapsed.TotalSeconds
            : 0;
        settings.LastUpdatedUtc = DateTime.UtcNow;
        await JsonFiles.SaveAtomicAsync(paths.SettingsPath, settings, CancellationToken.None);

        PrintResults(stats, scanResult, sessionWatch.Elapsed, downloadFolder);
    }

    private static async Task<ChatBase> SelectChatAsync(
        Client client,
        int? channelNumber,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("Loading your channels and groups...");
        var allChats = await client.Messages_GetAllChats()
            .WaitAsync(AppConstants.TelegramRequestTimeout, cancellationToken);
        var chats = allChats.chats.Values
            .Where(chat => chat.IsActive)
            .OrderBy(Ui.GetChatTitle)
            .ToList();

        if (chats.Count == 0)
            throw new InvalidOperationException("No active channels/groups were found.");

        Console.WriteLine();
        Console.WriteLine($"Found {chats.Count} channels/groups:\n");
        for (var i = 0; i < chats.Count; i++)
        {
            var type = chats[i] switch
            {
                TL.Channel channel when channel.IsChannel => "CHANNEL",
                TL.Channel => "GROUP",
                Chat => "GROUP",
                _ => "CHAT"
            };
            Console.WriteLine($"[{i + 1,3}] [{type,-7}] {Ui.GetChatTitle(chats[i])}");
        }

        Console.WriteLine();
        const string prompt = "Select channel/group number: ";
        var selectedNumber = channelNumber ?? Ui.ReadNumber(prompt, 1, chats.Count);
        if (channelNumber.HasValue)
            Console.WriteLine($"{prompt}{selectedNumber}");
        if (selectedNumber < 1 || selectedNumber > chats.Count)
            throw new ArgumentOutOfRangeException(nameof(channelNumber), selectedNumber,
                $"Select a channel/group number between 1 and {chats.Count}.");
        return chats[selectedNumber - 1];
    }

    private static async Task<ScanResult> ProduceDownloadsAsync(
        Client client,
        InputPeer peer,
        long chatId,
        string chatTitle,
        ChannelCatalog chatCatalog,
        bool fullScanRequired,
        Dictionary<long, List<FileInfo>> existingFiles,
        DownloadIndex index,
        BatchedSaveCoordinator indexSaver,
        BatchedSaveCoordinator catalogSaver,
        ChannelWriter<VideoItem> writer,
        HashSet<string> seenDocuments,
        TransferStats stats,
        string downloadFolder,
        CancellationToken cancellationToken)
    {
        try
        {
            async ValueTask PrepareAsync(VideoItem video)
            {
                Interlocked.Increment(ref stats.DiscoveredReferences);
                if (!seenDocuments.Add(video.Key))
                {
                    Interlocked.Increment(ref stats.DuplicateReferences);
                    return;
                }

                if (FileDiscovery.IsAlreadyDownloaded(
                        video,
                        index,
                        downloadFolder,
                        existingFiles,
                        out var indexChanged))
                {
                    if (indexChanged)
                        indexSaver.MarkDirty();
                    Interlocked.Increment(ref stats.AlreadyDownloaded);
                    return;
                }

                video.FinalPath = FileNames.GetNonOverwritingPath(
                    downloadFolder,
                    video.ExpectedFileName);
                Interlocked.Increment(ref stats.TotalVideos);
                Interlocked.Add(ref stats.TotalBytes, Math.Max(0, video.Size));
                await writer.WriteAsync(video, cancellationToken);
            }

            if (!fullScanRequired)
            {
                foreach (var cached in chatCatalog.Videos.Values.OrderBy(entry => entry.MessageId))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await PrepareAsync(VideoItem.FromCatalog(chatId, cached));
                }
            }

            Ui.Print("\nScanning Telegram while prepared videos download...");
            var scanner = new TelegramVideoScanner(client);
            var scanResult = await scanner.ScanAsync(
                peer,
                chatId,
                chatTitle,
                chatCatalog,
                fullScanRequired,
                PrepareAsync,
                cancellationToken);
            catalogSaver.MarkDirty(forceSoon: true);
            var catalogBytes = chatCatalog.Videos.Values.Sum(entry => Math.Max(0, entry.Size));
            Ui.PrintDownloadPlan(
                scanResult,
                chatCatalog.Videos.Count,
                catalogBytes,
                seenDocuments.Count,
                stats);
            return scanResult;
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private static void PrintResults(
        TransferStats stats,
        ScanResult scan,
        TimeSpan elapsed,
        string downloadFolder)
    {
        Console.WriteLine();
        Console.WriteLine("============================================");
        Console.WriteLine("FINISHED");
        Console.WriteLine("============================================");
        Console.WriteLine();
        Console.WriteLine($"Scan type          : {(scan.WasFullScan ? "filtered full" : "incremental")}");
        Console.WriteLine($"Messages inspected : {scan.MessagesScanned:N0}");
        Console.WriteLine($"Downloaded         : {Volatile.Read(ref stats.Completed):N0}");
        Console.WriteLine($"Already present    : {Volatile.Read(ref stats.AlreadyDownloaded):N0}");
        Console.WriteLine($"Duplicate refs     : {Volatile.Read(ref stats.DuplicateReferences):N0}");
        Console.WriteLine($"Failed             : {Volatile.Read(ref stats.Failed):N0}");
        Console.WriteLine($"Resumed            : {Ui.FormatBytes(Interlocked.Read(ref stats.ResumedBytes))}");
        Console.WriteLine($"Network transferred: {Ui.FormatBytes(Interlocked.Read(ref stats.NetworkBytes))}");
        Console.WriteLine($"Retries            : {Volatile.Read(ref stats.Retries):N0}");
        Console.WriteLine($"Part retries       : {Volatile.Read(ref stats.PartRetries):N0}");
        Console.WriteLine($"DC reconnects      : {Volatile.Read(ref stats.DcReconnects):N0}");
        Console.WriteLine($"Reference refreshes: {Volatile.Read(ref stats.FileReferenceRefreshes):N0}");
        Console.WriteLine($"Flood-wait time    : {Ui.FormatDuration(TimeSpan.FromSeconds(Interlocked.Read(ref stats.FloodWaitSeconds)))}");
        Console.WriteLine($"Short request waits: {Volatile.Read(ref stats.ShortFloodWaitEvents):N0} " +
            $"({Ui.FormatDuration(TimeSpan.FromSeconds(Interlocked.Read(ref stats.ShortFloodWaitSeconds)))} summed across workers)");
        Console.WriteLine($"Time               : {Ui.FormatDuration(elapsed)}");
        if (elapsed.TotalSeconds > 0)
        {
            var speed = Interlocked.Read(ref stats.NetworkBytes) / elapsed.TotalSeconds;
            Console.WriteLine($"Average network    : {Ui.FormatBytes((long)speed)}/s");
        }

        Console.WriteLine();
        Console.WriteLine($"Files are in:\n{downloadFolder}");
        Console.WriteLine();
    }

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task ObserveFailureAsync(Task task)
    {
        try
        {
            await task;
        }
        catch
        {
        }
    }

    private static string? Config(string what) => TelegramLoginConfiguration.Get(what, Ui.Prompt);
}
