using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using TL;
using WTelegram;

internal static class Program
{
    // ============================================================
    // PERFORMANCE SETTINGS
    // ============================================================

    // Number of videos downloaded at the same time.
    private const int MaxConcurrentVideoDownloads = 3;

    // WTelegram default is 2.
    // Our testing showed 8 works better than 16.
    private const int TelegramParallelTransfers = 8;

    // 512 KB performed better than 1 MB in our testing.
    private const int TelegramFilePartSize = 512 * 1024;

    // Retry ordinary failed downloads this many times.
    private const int MaxDownloadRetries = 3;


    // ============================================================
    // INTERNAL
    // ============================================================

    private const string IndexFileName =
        ".telegram-video-downloader-index.json";

    private static readonly object ConsoleLock = new();

    private static readonly SemaphoreSlim IndexWriteLock =
        new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions =
        new()
        {
            WriteIndented = true
        };

    // Fictional examples with a placeholder for the numeric document ID:
    //   42_<document-id>_paper-moon-demo.mp4
    //   42_paper-moon-demo_<document-id>.mp4
    // A numeric token counts as a match only if the scan found that document ID.
    private static readonly Regex NumberTokenRegex =
        new(
            @"(?<!\d)\d{1,20}(?!\d)",
            RegexOptions.Compiled |
            RegexOptions.CultureInvariant);


    // ============================================================
    // MAIN
    // ============================================================

    private static async Task Main()
    {
        Console.Title = "Telegram Video Downloader";

        // Hide WTelegram protocol spam:
        //
        // Upload_GetFile
        // RpcResult
        // MsgsAck
        // etc.
        WTelegram.Helpers.Log = (_, _) => { };

        using var cancellation =
            new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;

            lock (ConsoleLock)
            {
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(
                    "Stopping... completed downloads will be preserved.");
            }

            cancellation.Cancel();
        };

        PrintHeader();

        try
        {
            // ====================================================
            // TELEGRAM LOGIN
            // ====================================================

            await using var client =
                new Client(Config);

            client.FloodRetryThreshold = 120;

            client.ParallelTransfers =
                TelegramParallelTransfers;

            client.FilePartSize =
                TelegramFilePartSize;

            Console.WriteLine(
                "Connecting to Telegram...");

            var me =
                await client.LoginUserIfNeeded();

            Console.WriteLine();

            Console.WriteLine(
                $"Logged in as: " +
                $"{me.first_name} {me.last_name}".Trim());

            Console.WriteLine(
                $"Telegram User ID: {me.id}");

            Console.WriteLine();


            // ====================================================
            // LOAD CHATS
            // ====================================================

            Console.WriteLine(
                "Loading your channels and groups...");

            var allChats =
                await client.Messages_GetAllChats();

            var chats =
                allChats.chats.Values
                    .Where(x => x.IsActive)
                    .OrderBy(GetChatTitle)
                    .ToList();

            if (chats.Count == 0)
            {
                Console.WriteLine(
                    "No active channels/groups were found.");

                return;
            }

            Console.WriteLine();

            Console.WriteLine(
                $"Found {chats.Count} channels/groups:");

            Console.WriteLine();

            for (var i = 0; i < chats.Count; i++)
            {
                var chat =
                    chats[i];

                var type =
                    chat switch
                    {
                        Channel c when c.IsChannel
                            => "CHANNEL",

                        Channel
                            => "GROUP",

                        Chat
                            => "GROUP",

                        _
                            => "CHAT"
                    };

                Console.WriteLine(
                    $"[{i + 1,3}] " +
                    $"[{type,-7}] " +
                    $"{GetChatTitle(chat)}");
            }

            Console.WriteLine();

            var selectedNumber =
                ReadNumber(
                    "Select channel/group number: ",
                    1,
                    chats.Count);

            var selectedChat =
                chats[selectedNumber - 1];

            var chatTitle =
                GetChatTitle(selectedChat);

            var chatId =
                selectedChat.ID;

            InputPeer peer =
                selectedChat;

            Console.WriteLine();

            Console.WriteLine(
                $"Selected: {chatTitle}");


            // ====================================================
            // DOWNLOAD DIRECTORY
            // ====================================================

            Console.WriteLine();

            Console.WriteLine(
                "Choose where the videos should be saved...");

            var downloadFolder =
                ChooseDownloadFolder();

            if (string.IsNullOrWhiteSpace(
                    downloadFolder))
            {
                Console.WriteLine(
                    "No folder selected. Cancelled.");

                return;
            }

            Directory.CreateDirectory(
                downloadFolder);

            Console.WriteLine();

            Console.WriteLine(
                "Download directory:");

            Console.WriteLine(
                downloadFolder);

            var indexPath =
                Path.Combine(
                    downloadFolder,
                    IndexFileName);

            var downloadIndex =
                await LoadIndexAsync(
                    indexPath);


            // ====================================================
            // TELEGRAM CHANNEL SCAN
            // ====================================================

            Console.WriteLine();

            Console.WriteLine(
                "============================================");

            Console.WriteLine(
                "SCANNING CHANNEL");

            Console.WriteLine(
                "============================================");

            Console.WriteLine();

            var scanWatch =
                Stopwatch.StartNew();

            var scanResult =
                await ScanVideosAsync(
                    client,
                    peer,
                    chatId,
                    cancellation.Token);

            scanWatch.Stop();

            Console.WriteLine();
            Console.WriteLine();

            Console.WriteLine(
                $"Scan completed in " +
                $"{FormatDuration(scanWatch.Elapsed)}");

            Console.WriteLine(
                $"Messages scanned : " +
                $"{scanResult.MessagesScanned:N0}");

            Console.WriteLine(
                $"Videos found     : " +
                $"{scanResult.Videos.Count:N0}");

            Console.WriteLine(
                $"Total video size : " +
                $"{FormatBytes(scanResult.TotalVideoBytes)}");

            if (scanResult.Videos.Count == 0)
            {
                Console.WriteLine();

                Console.WriteLine(
                    "No videos were found.");

                return;
            }


            // ====================================================
            // INDEX EXISTING FILES ON DISK - ONE TIME ONLY
            // ====================================================

            Console.WriteLine();

            Console.WriteLine(
                "Indexing existing files in destination folder...");

            var diskIndexWatch =
                Stopwatch.StartNew();

            // All Telegram Document IDs that actually exist
            // in this selected channel.
            var knownDocumentIds =
                scanResult.Videos
                    .Select(x => x.DocumentId)
                    .ToHashSet();

            // Scan Windows directory ONCE.
            var existingFilesByDocumentId =
                BuildExistingFileMap(
                    downloadFolder,
                    knownDocumentIds);

            diskIndexWatch.Stop();

            var indexedPhysicalFiles =
                existingFilesByDocumentId
                    .Values
                    .Sum(x => x.Count);

            Console.WriteLine(
                $"Physical files matched : " +
                $"{indexedPhysicalFiles:N0}");

            Console.WriteLine(
                $"Document IDs represented: " +
                $"{existingFilesByDocumentId.Count:N0}");

            Console.WriteLine(
                $"Disk indexing time      : " +
                $"{FormatDuration(diskIndexWatch.Elapsed)}");


            // ====================================================
            // BUILD DOWNLOAD QUEUE
            // ====================================================

            var queue =
                new List<VideoItem>();

            var alreadyDownloaded =
                0;

            var duplicateReferences =
                0;

            // Same Telegram Document may appear in multiple
            // messages in the channel.
            var seenDocuments =
                new HashSet<string>();

            foreach (var video
                     in scanResult.Videos)
            {
                cancellation.Token
                    .ThrowIfCancellationRequested();

                // ------------------------------------------------
                // SAME TELEGRAM DOCUMENT REFERENCED MORE THAN ONCE
                // ------------------------------------------------

                if (!seenDocuments.Add(
                        video.Key))
                {
                    duplicateReferences++;

                    continue;
                }

                // ------------------------------------------------
                // ALREADY ON DISK?
                // ------------------------------------------------

                if (IsAlreadyDownloaded(
                        video,
                        downloadIndex,
                        downloadFolder,
                        existingFilesByDocumentId))
                {
                    alreadyDownloaded++;

                    continue;
                }

                // ------------------------------------------------
                // FILE PATH FOR NEW DOWNLOAD
                // ------------------------------------------------

                video.FinalPath =
                    GetNonOverwritingPath(
                        downloadFolder,
                        video.ExpectedFileName);

                queue.Add(
                    video);
            }


            // Save existing files that were discovered into
            // our persistent JSON index.
            await SaveIndexAsync(
                downloadIndex,
                indexPath);

            var bytesToDownload =
                queue.Sum(
                    x => x.Size);


            // ====================================================
            // DOWNLOAD PLAN
            // ====================================================

            Console.WriteLine();

            Console.WriteLine(
                "============================================");

            Console.WriteLine(
                "DOWNLOAD PLAN");

            Console.WriteLine(
                "============================================");

            Console.WriteLine();

            Console.WriteLine(
                $"Unique videos       : " +
                $"{seenDocuments.Count:N0}");

            Console.WriteLine(
                $"Already downloaded  : " +
                $"{alreadyDownloaded:N0}");

            Console.WriteLine(
                $"Duplicate references: " +
                $"{duplicateReferences:N0}");

            Console.WriteLine(
                $"Need downloading    : " +
                $"{queue.Count:N0}");

            Console.WriteLine(
                $"Remaining data      : " +
                $"{FormatBytes(bytesToDownload)}");

            Console.WriteLine(
                $"Concurrent videos   : " +
                $"{MaxConcurrentVideoDownloads}");

            Console.WriteLine(
                $"Parallel transfers  : " +
                $"{TelegramParallelTransfers}");

            Console.WriteLine(
                $"File part size      : " +
                $"{FormatBytes(TelegramFilePartSize)}");

            if (queue.Count == 0)
            {
                Console.WriteLine();

                Console.WriteLine(
                    "Everything is already downloaded.");

                Console.WriteLine(
                    "Nothing to do.");

                return;
            }

            Console.WriteLine();

            Console.Write(
                "Start downloading? [Y/N]: ");

            var confirm =
                Console.ReadLine();

            if (!string.Equals(
                    confirm,
                    "Y",
                    StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine(
                    "Cancelled.");

                return;
            }

            Console.WriteLine();


            // ====================================================
            // DOWNLOAD
            // ====================================================

            var stats =
                new TransferStats();

            var downloadWatch =
                Stopwatch.StartNew();

            using var statusCancellation =
                CancellationTokenSource
                    .CreateLinkedTokenSource(
                        cancellation.Token);

            var statusTask =
                ShowStatusLoopAsync(
                    stats,
                    queue.Count,
                    bytesToDownload,
                    downloadWatch,
                    statusCancellation.Token);

            try
            {
                await Parallel.ForEachAsync(
                    queue,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism =
                            MaxConcurrentVideoDownloads,

                        CancellationToken =
                            cancellation.Token
                    },
                    async (video, ct) =>
                    {
                        await DownloadVideoAsync(
                            client,
                            video,
                            downloadIndex,
                            indexPath,
                            stats,
                            ct);
                    });
            }
            catch (OperationCanceledException)
            {
                // Expected when Ctrl+C is pressed.
            }
            finally
            {
                statusCancellation.Cancel();

                try
                {
                    await statusTask;
                }
                catch (OperationCanceledException)
                {
                }

                downloadWatch.Stop();
            }


            // ====================================================
            // RESULTS
            // ====================================================

            Console.WriteLine();
            Console.WriteLine();

            Console.WriteLine(
                "============================================");

            Console.WriteLine(
                cancellation.IsCancellationRequested
                    ? "STOPPED"
                    : "FINISHED");

            Console.WriteLine(
                "============================================");

            Console.WriteLine();

            Console.WriteLine(
                $"Downloaded : " +
                $"{stats.Completed:N0}");

            Console.WriteLine(
                $"Failed     : " +
                $"{stats.Failed:N0}");

            Console.WriteLine(
                $"Skipped    : " +
                $"{alreadyDownloaded:N0}");

            Console.WriteLine(
                $"Transferred: " +
                $"{FormatBytes(
                    Interlocked.Read(
                        ref stats.BytesTransferred))}");

            Console.WriteLine(
                $"Time       : " +
                $"{FormatDuration(
                    downloadWatch.Elapsed)}");

            if (downloadWatch.Elapsed.TotalSeconds > 0)
            {
                var averageSpeed =
                    Interlocked.Read(
                        ref stats.BytesTransferred)
                    /
                    downloadWatch.Elapsed.TotalSeconds;

                Console.WriteLine(
                    $"Average    : " +
                    $"{FormatBytes(
                        (long)averageSpeed)}/s");
            }

            Console.WriteLine();

            Console.WriteLine(
                "Files are in:");

            Console.WriteLine(
                downloadFolder);

            Console.WriteLine();
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine();

            Console.WriteLine(
                "Operation cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine();

            Console.WriteLine(
                "FATAL ERROR:");

            Console.WriteLine(
                ex);
        }

        Console.WriteLine();

        Console.WriteLine(
            "Press ENTER to exit...");

        Console.ReadLine();
    }


    // ============================================================
    // TELEGRAM CHANNEL SCANNING
    // ============================================================

    private static async Task<ScanResult>
        ScanVideosAsync(
            Client client,
            InputPeer peer,
            long chatId,
            CancellationToken cancellationToken)
    {
        var result =
            new ScanResult();

        var offsetId =
            0;

        while (true)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            Messages_MessagesBase history;

            while (true)
            {
                try
                {
                    history =
                        await client.Messages_GetHistory(
                            peer,
                            offset_id: offsetId,
                            limit: 100);

                    break;
                }
                catch (RpcException ex)
                    when (
                        ex.Code == 420 &&
                        ex.X > 0)
                {
                    Console.WriteLine();

                    Console.WriteLine(
                        $"Flood wait while scanning: " +
                        $"{ex.X} seconds.");

                    await Task.Delay(
                        TimeSpan.FromSeconds(
                            ex.X + 1),
                        cancellationToken);
                }
            }

            if (history.Messages.Length == 0)
                break;

            foreach (var messageBase
                     in history.Messages)
            {
                result.MessagesScanned++;

                if (messageBase
                    is not TL.Message message)
                {
                    continue;
                }

                if (message.media
                    is not MessageMediaDocument
                    {
                        document: Document document
                    })
                {
                    continue;
                }

                var isVideo =
                    document.mime_type?
                        .StartsWith(
                            "video/",
                            StringComparison.OrdinalIgnoreCase)
                    == true
                    ||
                    document.attributes
                        .OfType<DocumentAttributeVideo>()
                        .Any();

                if (!isVideo)
                    continue;

                var originalFileName =
                    GetOriginalFileName(
                        document);

                var expectedFileName =
                    BuildFileName(
                        message.ID,
                        document.id,
                        originalFileName);

                // Filename used by an older version.
                var legacyFileName =
                    BuildLegacyFileName(
                        message.ID,
                        originalFileName);

                var key =
                    BuildVideoKey(
                        chatId,
                        document.id,
                        document.size);

                var item =
                    new VideoItem
                    {
                        Key =
                            key,

                        MessageId =
                            message.ID,

                        DocumentId =
                            document.id,

                        Document =
                            document,

                        Size =
                            document.size,

                        OriginalFileName =
                            originalFileName,

                        ExpectedFileName =
                            expectedFileName,

                        LegacyFileName =
                            legacyFileName
                    };

                result.Videos.Add(
                    item);

                result.TotalVideoBytes +=
                    Math.Max(
                        0,
                        document.size);
            }

            // Continue with older messages.
            offsetId =
                history.Messages[^1].ID;

            if (result.MessagesScanned % 1000 < 100)
            {
                Console.Write(
                    $"\rMessages scanned: " +
                    $"{result.MessagesScanned:N0} | " +
                    $"Videos: " +
                    $"{result.Videos.Count:N0} | " +
                    $"Size: " +
                    $"{FormatBytes(
                        result.TotalVideoBytes)}");
            }
        }

        return result;
    }


    // ============================================================
    // BUILD EXISTING FILE INDEX
    //
    // IMPORTANT:
    // The Windows folder is enumerated ONLY ONCE here.
    // ============================================================

    private static Dictionary<long, List<FileInfo>>
        BuildExistingFileMap(
            string downloadFolder,
            HashSet<long> knownDocumentIds)
    {
        var result =
            new Dictionary<long, List<FileInfo>>();

        foreach (var path
                 in Directory.EnumerateFiles(
                     downloadFolder,
                     "*",
                     SearchOption.TopDirectoryOnly))
        {
            var fileName =
                Path.GetFileName(
                    path);

            // ----------------------------------------------------
            // IGNORE PARTIAL DOWNLOADS
            // ----------------------------------------------------

            if (fileName.EndsWith(
                    ".part",
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            // ----------------------------------------------------
            // IGNORE JSON INDEX FILE
            // ----------------------------------------------------

            if (string.Equals(
                    fileName,
                    IndexFileName,
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (string.Equals(
                    fileName,
                    IndexFileName + ".tmp",
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            FileInfo fileInfo;

            try
            {
                fileInfo =
                    new FileInfo(
                        path);

                if (!fileInfo.Exists ||
                    fileInfo.Length <= 0)
                {
                    continue;
                }
            }
            catch
            {
                continue;
            }


            // ----------------------------------------------------
            // FIND TELEGRAM DOCUMENT ID INSIDE FILENAME
            // ----------------------------------------------------

            foreach (Match match
                     in NumberTokenRegex.Matches(
                         fileName))
            {
                if (!long.TryParse(
                        match.Value,
                        out var possibleDocumentId))
                {
                    continue;
                }

                // We only accept IDs that Telegram itself
                // reported while scanning this channel.
                if (!knownDocumentIds.Contains(
                        possibleDocumentId))
                {
                    continue;
                }

                if (!result.TryGetValue(
                        possibleDocumentId,
                        out var files))
                {
                    files =
                        new List<FileInfo>();

                    result[possibleDocumentId] =
                        files;
                }

                // Same document ID may occur more than once
                // inside the filename.
                //
                // Only add physical file once.
                if (!files.Any(
                        x => string.Equals(
                            x.FullName,
                            fileInfo.FullName,
                            StringComparison.OrdinalIgnoreCase)))
                {
                    files.Add(
                        fileInfo);
                }

                // One matching Telegram Document ID per physical
                // file is enough.
                break;
            }
        }

        return result;
    }


    // ============================================================
    // DUPLICATE / EXISTING FILE DETECTION
    // ============================================================

    private static bool IsAlreadyDownloaded(
        VideoItem video,
        DownloadIndex index,
        string downloadFolder,
        Dictionary<long, List<FileInfo>>
            existingFilesByDocumentId)
    {
        // --------------------------------------------------------
        // 1. PERSISTENT JSON INDEX
        // --------------------------------------------------------

        if (index.Videos.TryGetValue(
                video.Key,
                out var existingEntry))
        {
            var indexedPath =
                Path.Combine(
                    downloadFolder,
                    existingEntry.FileName);

            if (IsCompleteFile(
                    indexedPath,
                    video.Size))
            {
                return true;
            }

            // Stale index record:
            // physical file is gone or wrong size.
            index.Videos.TryRemove(
                video.Key,
                out _);
        }


        // --------------------------------------------------------
        // 2. FAST IN-MEMORY DOCUMENT-ID LOOKUP
        //
        // NO Directory.EnumerateFiles() HERE!
        // --------------------------------------------------------

        if (existingFilesByDocumentId.TryGetValue(
                video.DocumentId,
                out var candidateFiles))
        {
            foreach (var file
                     in candidateFiles)
            {
                try
                {
                    file.Refresh();

                    if (!file.Exists)
                        continue;

                    var correctSize =
                        video.Size > 0
                            ? file.Length ==
                              video.Size
                            : file.Length > 0;

                    if (!correctSize)
                        continue;

                    // SAME:
                    //
                    // Telegram Document ID
                    // +
                    // exact byte size
                    //
                    // Therefore already downloaded.
                    AddExistingFileToIndex(
                        video,
                        file.FullName,
                        index);

                    return true;
                }
                catch
                {
                    // Ignore inaccessible/broken candidate
                    // and continue checking others.
                }
            }
        }


        // --------------------------------------------------------
        // 3. CURRENT EXPECTED FILENAME FALLBACK
        // --------------------------------------------------------

        var expectedPath =
            Path.Combine(
                downloadFolder,
                video.ExpectedFileName);

        if (IsCompleteFile(
                expectedPath,
                video.Size))
        {
            AddExistingFileToIndex(
                video,
                expectedPath,
                index);

            AddFileToMemoryIndex(
                existingFilesByDocumentId,
                video.DocumentId,
                expectedPath);

            return true;
        }


        // --------------------------------------------------------
        // 4. LEGACY FILENAME FALLBACK
        // --------------------------------------------------------

        var legacyPath =
            Path.Combine(
                downloadFolder,
                video.LegacyFileName);

        if (IsCompleteFile(
                legacyPath,
                video.Size))
        {
            AddExistingFileToIndex(
                video,
                legacyPath,
                index);

            AddFileToMemoryIndex(
                existingFilesByDocumentId,
                video.DocumentId,
                legacyPath);

            return true;
        }


        return false;
    }


    private static void AddFileToMemoryIndex(
        Dictionary<long, List<FileInfo>> index,
        long documentId,
        string path)
    {
        if (!File.Exists(path))
            return;

        if (!index.TryGetValue(
                documentId,
                out var files))
        {
            files =
                new List<FileInfo>();

            index[documentId] =
                files;
        }

        if (!files.Any(
                x => string.Equals(
                    x.FullName,
                    path,
                    StringComparison.OrdinalIgnoreCase)))
        {
            files.Add(
                new FileInfo(path));
        }
    }


    private static bool IsCompleteFile(
        string path,
        long expectedSize)
    {
        if (!File.Exists(path))
            return false;

        try
        {
            var length =
                new FileInfo(path).Length;

            if (expectedSize > 0)
            {
                return length ==
                       expectedSize;
            }

            return length > 0;
        }
        catch
        {
            return false;
        }
    }


    // ============================================================
    // DOWNLOAD
    // ============================================================

    private static async Task DownloadVideoAsync(
        Client client,
        VideoItem video,
        DownloadIndex downloadIndex,
        string indexPath,
        TransferStats stats,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(
            ref stats.Active);

        try
        {
            var attempt =
                0;

            while (true)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();

                attempt++;

                ResetProgress(
                    video,
                    stats);

                var finalPath =
                    video.FinalPath
                    ??
                    throw new InvalidOperationException(
                        "Final path was not assigned.");

                var partialPath =
                    finalPath +
                    ".part";


                // ------------------------------------------------
                // REMOVE OLD INCOMPLETE .PART FILE
                // ------------------------------------------------

                if (File.Exists(
                        partialPath))
                {
                    try
                    {
                        File.Delete(
                            partialPath);
                    }
                    catch
                    {
                        // Real download will give a clearer
                        // error if this is locked.
                    }
                }


                Print(
                    $"[START] " +
                    $"{Path.GetFileName(finalPath)} " +
                    $"({FormatBytes(video.Size)})");


                try
                {
                    await using (
                        var fileStream =
                            new FileStream(
                                partialPath,
                                FileMode.Create,
                                FileAccess.ReadWrite,
                                FileShare.None,
                                bufferSize:
                                    1024 * 1024,
                                options:
                                    FileOptions.Asynchronous))
                    {
                        await client.DownloadFileAsync(
                            video.Document,
                            fileStream,
                            progress:
                                (current, total) =>
                                {
                                    cancellationToken
                                        .ThrowIfCancellationRequested();

                                    var previous =
                                        Interlocked.Exchange(
                                            ref video.ProgressBytes,
                                            current);

                                    var delta =
                                        current -
                                        previous;

                                    if (delta != 0)
                                    {
                                        Interlocked.Add(
                                            ref stats.BytesTransferred,
                                            delta);
                                    }
                                });
                    }


                    cancellationToken
                        .ThrowIfCancellationRequested();


                    // ============================================
                    // VERIFY EXACT FILE SIZE
                    // ============================================

                    var partialInfo =
                        new FileInfo(
                            partialPath);

                    if (video.Size > 0 &&
                        partialInfo.Length !=
                        video.Size)
                    {
                        throw new InvalidDataException(
                            $"Downloaded size mismatch. " +
                            $"Expected {video.Size}, " +
                            $"received {partialInfo.Length}.");
                    }


                    // ============================================
                    // NEVER OVERWRITE A COMPLETED FILE
                    // ============================================

                    var actualFinalPath =
                        finalPath;

                    if (File.Exists(
                            actualFinalPath))
                    {
                        if (IsCompleteFile(
                                actualFinalPath,
                                video.Size))
                        {
                            // Someone/file already completed
                            // this exact destination.
                            File.Delete(
                                partialPath);

                            // Remove bytes we unnecessarily
                            // downloaded from remaining-progress
                            // calculation.
                            ResetProgress(
                                video,
                                stats);

                            await MarkCompletedAsync(
                                video,
                                actualFinalPath,
                                downloadIndex,
                                indexPath);

                            Interlocked.Increment(
                                ref stats.Completed);

                            Print(
                                $"[SKIP] File already exists: " +
                                $"{Path.GetFileName(
                                    actualFinalPath)}");

                            return;
                        }


                        // Existing file has another size.
                        //
                        // Do NOT destroy it automatically.
                        // Create _new1 etc. instead.
                        actualFinalPath =
                            GetNonOverwritingPath(
                                Path.GetDirectoryName(
                                    actualFinalPath)!,
                                Path.GetFileName(
                                    actualFinalPath));
                    }


                    // ============================================
                    // FINALIZE .PART -> MP4
                    // ============================================

                    File.Move(
                        partialPath,
                        actualFinalPath);

                    video.FinalPath =
                        actualFinalPath;


                    // ============================================
                    // SAVE JSON INDEX
                    // ============================================

                    await MarkCompletedAsync(
                        video,
                        actualFinalPath,
                        downloadIndex,
                        indexPath);

                    Interlocked.Increment(
                        ref stats.Completed);

                    Print(
                        $"[DONE ] " +
                        $"{Path.GetFileName(
                            actualFinalPath)}");

                    return;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (RpcException ex)
                    when (
                        ex.Code == 420 &&
                        ex.X > 0)
                {
                    ResetProgress(
                        video,
                        stats);

                    Print(
                        $"[WAIT ] Telegram requested " +
                        $"{ex.X}s flood wait for " +
                        $"{video.ExpectedFileName}");

                    await Task.Delay(
                        TimeSpan.FromSeconds(
                            ex.X + 1),
                        cancellationToken);

                    // Flood wait does not count against
                    // normal retries.
                    attempt--;
                }
                catch (Exception ex)
                {
                    ResetProgress(
                        video,
                        stats);

                    if (attempt <=
                        MaxDownloadRetries)
                    {
                        Print(
                            $"[RETRY] " +
                            $"{video.ExpectedFileName} | " +
                            $"attempt " +
                            $"{attempt}/" +
                            $"{MaxDownloadRetries} | " +
                            $"{ex.Message}");

                        await Task.Delay(
                            TimeSpan.FromSeconds(
                                attempt * 3),
                            cancellationToken);

                        continue;
                    }

                    Interlocked.Increment(
                        ref stats.Failed);

                    Print(
                        $"[FAIL ] " +
                        $"{video.ExpectedFileName} | " +
                        $"{ex.Message}");

                    return;
                }
            }
        }
        finally
        {
            Interlocked.Decrement(
                ref stats.Active);
        }
    }


    // ============================================================
    // STATUS / SPEED
    // ============================================================

    private static async Task ShowStatusLoopAsync(
        TransferStats stats,
        int totalVideos,
        long totalBytes,
        Stopwatch stopwatch,
        CancellationToken cancellationToken)
    {
        long lastBytes =
            0;

        var lastTime =
            stopwatch.Elapsed;

        var recentSpeeds =
            new Queue<double>();

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(
                TimeSpan.FromSeconds(3),
                cancellationToken);

            var transferred =
                Interlocked.Read(
                    ref stats.BytesTransferred);

            var now =
                stopwatch.Elapsed;

            var elapsedSinceLast =
                (now - lastTime)
                .TotalSeconds;

            var bytesSinceLast =
                transferred -
                lastBytes;

            double currentSpeed =
                0;

            if (elapsedSinceLast > 0)
            {
                currentSpeed =
                    bytesSinceLast /
                    elapsedSinceLast;
            }

            lastBytes =
                transferred;

            lastTime =
                now;


            // Last 5 samples = about 15 seconds.
            recentSpeeds.Enqueue(
                currentSpeed);

            while (recentSpeeds.Count > 5)
            {
                recentSpeeds.Dequeue();
            }

            var rollingSpeed =
                recentSpeeds.Count > 0
                    ? recentSpeeds.Average()
                    : 0;


            // Whole session average.
            var totalAverageSpeed =
                stopwatch.Elapsed.TotalSeconds > 0
                    ? transferred /
                      stopwatch.Elapsed.TotalSeconds
                    : 0;


            var remainingBytes =
                Math.Max(
                    0,
                    totalBytes -
                    transferred);


            // Use total average for ETA because Telegram
            // transfers are very bursty.
            var etaSpeed =
                totalAverageSpeed > 0
                    ? totalAverageSpeed
                    : rollingSpeed;

            var eta =
                etaSpeed > 0
                    ? TimeSpan.FromSeconds(
                        remainingBytes /
                        etaSpeed)
                    : TimeSpan.Zero;


            var percent =
                totalBytes > 0
                    ? transferred *
                      100.0 /
                      totalBytes
                    : 0;


            var completed =
                Volatile.Read(
                    ref stats.Completed);

            var failed =
                Volatile.Read(
                    ref stats.Failed);

            var active =
                Volatile.Read(
                    ref stats.Active);


            lock (ConsoleLock)
            {
                Console.WriteLine();

                Console.WriteLine(
                    $"[STATUS] " +
                    $"{completed}/{totalVideos} done | " +
                    $"{active} active | " +
                    $"{percent:0.00}% | " +
                    $"{FormatBytes(transferred)} / " +
                    $"{FormatBytes(totalBytes)} | " +
                    $"NOW " +
                    $"{FormatBytes((long)currentSpeed)}/s | " +
                    $"AVG15 " +
                    $"{FormatBytes((long)rollingSpeed)}/s | " +
                    $"TOTALAVG " +
                    $"{FormatBytes((long)totalAverageSpeed)}/s | " +
                    $"ETA {FormatDuration(eta)} | " +
                    $"failed {failed}");
            }
        }
    }


    // ============================================================
    // JSON DOWNLOAD INDEX
    // ============================================================

    private static async Task<DownloadIndex>
        LoadIndexAsync(
            string indexPath)
    {
        if (!File.Exists(
                indexPath))
        {
            return new DownloadIndex();
        }

        try
        {
            var json =
                await File.ReadAllTextAsync(
                    indexPath);

            return
                JsonSerializer.Deserialize<DownloadIndex>(
                    json,
                    JsonOptions)
                ??
                new DownloadIndex();
        }
        catch (Exception ex)
        {
            Console.WriteLine();

            Console.WriteLine(
                $"Warning: Could not read existing index: " +
                $"{ex.Message}");

            Console.WriteLine(
                "Existing physical video files will still be checked.");

            return new DownloadIndex();
        }
    }


    private static async Task SaveIndexAsync(
        DownloadIndex index,
        string indexPath)
    {
        await IndexWriteLock.WaitAsync();

        try
        {
            var json =
                JsonSerializer.Serialize(
                    index,
                    JsonOptions);

            var temporaryPath =
                indexPath +
                ".tmp";

            await File.WriteAllTextAsync(
                temporaryPath,
                json);

            File.Move(
                temporaryPath,
                indexPath,
                overwrite: true);
        }
        finally
        {
            IndexWriteLock.Release();
        }
    }


    private static async Task MarkCompletedAsync(
        VideoItem video,
        string actualPath,
        DownloadIndex index,
        string indexPath)
    {
        index.Videos[video.Key] =
            new DownloadIndexEntry
            {
                DocumentId =
                    video.DocumentId,

                MessageId =
                    video.MessageId,

                Size =
                    video.Size,

                FileName =
                    Path.GetFileName(
                        actualPath),

                DownloadedUtc =
                    DateTime.UtcNow
            };

        await SaveIndexAsync(
            index,
            indexPath);
    }


    private static void AddExistingFileToIndex(
        VideoItem video,
        string actualPath,
        DownloadIndex index)
    {
        index.Videos[video.Key] =
            new DownloadIndexEntry
            {
                DocumentId =
                    video.DocumentId,

                MessageId =
                    video.MessageId,

                Size =
                    video.Size,

                FileName =
                    Path.GetFileName(
                        actualPath),

                DownloadedUtc =
                    DateTime.UtcNow
            };
    }


    // ============================================================
    // WINDOWS FOLDER PICKER
    // ============================================================

    private static string? ChooseDownloadFolder()
    {
        string? selectedPath =
            null;

        Exception? exception =
            null;

        var thread =
            new Thread(
                () =>
                {
                    try
                    {
                        using var dialog =
                            new FolderBrowserDialog
                            {
                                Description =
                                    "Select folder for Telegram videos",

                                ShowNewFolderButton =
                                    true
                            };

                        if (dialog.ShowDialog()
                            == DialogResult.OK)
                        {
                            selectedPath =
                                dialog.SelectedPath;
                        }
                    }
                    catch (Exception ex)
                    {
                        exception =
                            ex;
                    }
                });

        thread.SetApartmentState(
            ApartmentState.STA);

        thread.Start();

        thread.Join();

        if (exception != null)
        {
            throw new InvalidOperationException(
                "Could not open folder picker.",
                exception);
        }

        return selectedPath;
    }


    // ============================================================
    // FILE NAME HELPERS
    // ============================================================

    private static string GetOriginalFileName(
        Document document)
    {
        var name =
            document.Filename;

        if (string.IsNullOrWhiteSpace(
                name))
        {
            name =
                $"video_{document.id}" +
                GetExtension(
                    document.mime_type);
        }

        name =
            SanitizeFilename(
                name);

        name =
            LimitFilenameLength(
                name,
                160);

        return name;
    }


    private static string BuildFileName(
        int messageId,
        long documentId,
        string originalFileName)
    {
        return SanitizeFilename(
            $"{messageId}_" +
            $"{documentId}_" +
            $"{originalFileName}");
    }


    private static string BuildLegacyFileName(
        int messageId,
        string originalFileName)
    {
        return SanitizeFilename(
            $"{messageId}_" +
            $"{originalFileName}");
    }


    private static string BuildVideoKey(
        long chatId,
        long documentId,
        long size)
    {
        return
            $"{chatId}:" +
            $"{documentId}:" +
            $"{size}";
    }


    private static string SanitizeFilename(
        string filename)
    {
        foreach (var invalidCharacter
                 in Path.GetInvalidFileNameChars())
        {
            filename =
                filename.Replace(
                    invalidCharacter,
                    '_');
        }

        filename =
            filename
                .Trim()
                .TrimEnd('.');

        if (string.IsNullOrWhiteSpace(
                filename))
        {
            filename =
                "video";
        }

        return filename;
    }


    private static string LimitFilenameLength(
        string fileName,
        int maximumLength)
    {
        if (fileName.Length <=
            maximumLength)
        {
            return fileName;
        }

        var extension =
            Path.GetExtension(
                fileName);

        var nameWithoutExtension =
            Path.GetFileNameWithoutExtension(
                fileName);

        var allowedNameLength =
            Math.Max(
                1,
                maximumLength -
                extension.Length);

        if (nameWithoutExtension.Length >
            allowedNameLength)
        {
            nameWithoutExtension =
                nameWithoutExtension[
                    ..allowedNameLength];
        }

        return
            nameWithoutExtension +
            extension;
    }


    // ============================================================
    // DO NOT OVERWRITE FILES
    // ============================================================

    private static string GetNonOverwritingPath(
        string folder,
        string fileName)
    {
        var path =
            Path.Combine(
                folder,
                fileName);

        // IMPORTANT:
        //
        // We deliberately ONLY check final file here.
        //
        // A stale .part file must NOT cause _new1/_new2.
        // DownloadVideoAsync removes stale .part files.
        if (!File.Exists(
                path))
        {
            return path;
        }

        var extension =
            Path.GetExtension(
                fileName);

        var name =
            Path.GetFileNameWithoutExtension(
                fileName);

        for (var i = 1; ; i++)
        {
            var candidate =
                Path.Combine(
                    folder,
                    $"{name}_new{i}{extension}");

            if (!File.Exists(
                    candidate))
            {
                return candidate;
            }
        }
    }


    // ============================================================
    // DOWNLOAD PROGRESS RESET
    // ============================================================

    private static void ResetProgress(
        VideoItem video,
        TransferStats stats)
    {
        var previous =
            Interlocked.Exchange(
                ref video.ProgressBytes,
                0);

        if (previous != 0)
        {
            Interlocked.Add(
                ref stats.BytesTransferred,
                -previous);
        }
    }


    // ============================================================
    // CHAT HELPERS
    // ============================================================

    private static string GetChatTitle(
        ChatBase chat)
    {
        return chat switch
        {
            Channel channel
                => channel.title,

            Chat basicChat
                => basicChat.title,

            _
                => chat.ToString()
                   ??
                   "Unknown Chat"
        };
    }


    // ============================================================
    // TELEGRAM LOGIN CONFIG
    // ============================================================

    private static string? Config(string what) => TelegramLoginConfiguration.Get(what, Prompt);


    private static string Prompt(
        string message)
    {
        Console.Write(
            message);

        return
            Console.ReadLine()
            ??
            "";
    }


    // ============================================================
    // CONSOLE
    // ============================================================

    private static int ReadNumber(
        string prompt,
        int minimum,
        int maximum)
    {
        while (true)
        {
            Console.Write(
                prompt);

            var text =
                Console.ReadLine();

            if (int.TryParse(
                    text,
                    out var number)
                &&
                number >= minimum
                &&
                number <= maximum)
            {
                return number;
            }

            Console.WriteLine(
                $"Please enter a number between " +
                $"{minimum} and {maximum}.");
        }
    }


    private static void Print(
        string message)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine(
                message);
        }
    }


    private static void PrintHeader()
    {
        Console.WriteLine(
            "============================================");

        Console.WriteLine(
            "       TELEGRAM VIDEO DOWNLOADER");

        Console.WriteLine(
            "============================================");

        Console.WriteLine();
    }


    // ============================================================
    // FORMATTING
    // ============================================================

    private static string FormatBytes(
        long bytes)
    {
        string[] suffixes =
        {
            "B",
            "KB",
            "MB",
            "GB",
            "TB"
        };

        double size =
            Math.Max(
                0,
                bytes);

        var suffix =
            0;

        while (size >= 1024 &&
               suffix <
               suffixes.Length - 1)
        {
            size /=
                1024;

            suffix++;
        }

        return
            $"{size:0.##} " +
            $"{suffixes[suffix]}";
    }


    private static string FormatDuration(
        TimeSpan duration)
    {
        if (duration <
            TimeSpan.Zero)
        {
            duration =
                TimeSpan.Zero;
        }

        if (duration.TotalDays >= 1)
        {
            return
                $"{(int)duration.TotalDays}d " +
                $"{duration.Hours:00}:" +
                $"{duration.Minutes:00}:" +
                $"{duration.Seconds:00}";
        }

        return
            $"{(int)duration.TotalHours:00}:" +
            $"{duration.Minutes:00}:" +
            $"{duration.Seconds:00}";
    }


    private static string GetExtension(
        string? mimeType)
    {
        return mimeType?
            .ToLowerInvariant() switch
        {
            "video/mp4"
                => ".mp4",

            "video/webm"
                => ".webm",

            "video/quicktime"
                => ".mov",

            "video/x-matroska"
                => ".mkv",

            "video/mpeg"
                => ".mpeg",

            "video/x-msvideo"
                => ".avi",

            _
                => ".video"
        };
    }


    // ============================================================
    // DATA CLASSES
    // ============================================================

    private sealed class ScanResult
    {
        public int MessagesScanned
        {
            get;
            set;
        }

        public long TotalVideoBytes
        {
            get;
            set;
        }

        public List<VideoItem> Videos
        {
            get;
        }
            = new();
    }


    private sealed class VideoItem
    {
        public required string Key
        {
            get;
            init;
        }

        public required int MessageId
        {
            get;
            init;
        }

        public required long DocumentId
        {
            get;
            init;
        }

        public required Document Document
        {
            get;
            init;
        }

        public required long Size
        {
            get;
            init;
        }

        public required string OriginalFileName
        {
            get;
            init;
        }

        public required string ExpectedFileName
        {
            get;
            init;
        }

        public required string LegacyFileName
        {
            get;
            init;
        }

        public string? FinalPath
        {
            get;
            set;
        }

        public long ProgressBytes;
    }


    private sealed class TransferStats
    {
        public long BytesTransferred;

        public int Completed;

        public int Failed;

        public int Active;
    }


    private sealed class DownloadIndex
    {
        public ConcurrentDictionary<
            string,
            DownloadIndexEntry> Videos
        {
            get;
            set;
        }
            = new();
    }


    private sealed class DownloadIndexEntry
    {
        public long DocumentId
        {
            get;
            set;
        }

        public int MessageId
        {
            get;
            set;
        }

        public long Size
        {
            get;
            set;
        }

        public string FileName
        {
            get;
            set;
        }
            = "";

        public DateTime DownloadedUtc
        {
            get;
            set;
        }
    }
}
