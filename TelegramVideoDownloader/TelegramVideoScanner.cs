using System.Collections.Concurrent;
using TL;
using WTelegram;

internal sealed class TelegramVideoScanner(Client client)
{
    public Task<ScanResult> ScanAsync(
        InputPeer peer,
        long chatId,
        string chatTitle,
        ChannelCatalog catalog,
        bool fullScan,
        Func<VideoItem, ValueTask> onVideo,
        CancellationToken cancellationToken) => fullScan
        ? FullScanAsync(peer, chatId, chatTitle, catalog, onVideo, cancellationToken)
        : IncrementalScanAsync(peer, chatId, catalog, onVideo, cancellationToken);

    private async Task<ScanResult> FullScanAsync(
        InputPeer peer,
        long chatId,
        string chatTitle,
        ChannelCatalog catalog,
        Func<VideoItem, ValueTask> onVideo,
        CancellationToken cancellationToken)
    {
        var result = new ScanResult { WasFullScan = true };
        var replacement = new ConcurrentDictionary<string, CatalogVideoEntry>();
        var offsetId = 0;

        while (true)
        {
            var page = await ExecuteRequestAsync(
                () => client.Messages_Search<InputMessagesFilterVideo>(
                    peer,
                    q: "",
                    offset_id: offsetId,
                    limit: 100),
                "filtered video search",
                cancellationToken);
            if (page.Messages.Length == 0)
                break;

            result.MessagesScanned += page.Messages.Length;
            foreach (var message in page.Messages.OfType<TL.Message>())
            {
                if (!TryGetVideo(message, out var document))
                    continue;
                var entry = CatalogVideoEntry.FromDocument(message.ID, document);
                replacement[message.ID.ToString()] = entry;
                result.VideosFound++;
                result.TotalVideoBytes += Math.Max(0, document.size);
                await onVideo(VideoItem.FromDocument(chatId, message.ID, document));
            }

            var nextOffset = page.Messages[^1].ID;
            if (nextOffset <= 0 || nextOffset == offsetId)
                break;
            offsetId = nextOffset;
            Ui.Print(
                $"[SCAN] Filtered video messages: {result.MessagesScanned:N0} | " +
                $"videos: {result.VideosFound:N0} | {Ui.FormatBytes(result.TotalVideoBytes)}");
        }

        var latestPage = await ExecuteRequestAsync(
            () => client.Messages_GetHistory(peer, limit: 1),
            "latest-message lookup",
            cancellationToken);
        var latestMessageId = latestPage.Messages.Length == 0
            ? 0
            : latestPage.Messages.Max(message => message.ID);

        catalog.ChatId = chatId;
        catalog.ChatTitle = chatTitle;
        catalog.Videos = replacement;
        catalog.LastScannedMessageId = latestMessageId;
        catalog.LastFullScanUtc = DateTime.UtcNow;
        catalog.LastIncrementalScanUtc = DateTime.UtcNow;
        return result;
    }

    private async Task<ScanResult> IncrementalScanAsync(
        InputPeer peer,
        long chatId,
        ChannelCatalog catalog,
        Func<VideoItem, ValueTask> onVideo,
        CancellationToken cancellationToken)
    {
        var result = new ScanResult { WasFullScan = false };
        var startingMessageId = catalog.LastScannedMessageId;
        var highestSeenId = startingMessageId;
        var offsetId = 0;

        while (true)
        {
            var page = await ExecuteRequestAsync(
                () => client.Messages_GetHistory(
                    peer,
                    offset_id: offsetId,
                    limit: 100,
                    min_id: startingMessageId),
                "incremental history scan",
                cancellationToken);
            if (page.Messages.Length == 0)
                break;

            result.MessagesScanned += page.Messages.Length;
            foreach (var messageBase in page.Messages)
            {
                highestSeenId = Math.Max(highestSeenId, messageBase.ID);
                if (messageBase is not TL.Message message || !TryGetVideo(message, out var document))
                    continue;

                var entry = CatalogVideoEntry.FromDocument(message.ID, document);
                catalog.Videos[message.ID.ToString()] = entry;
                result.VideosFound++;
                result.TotalVideoBytes += Math.Max(0, document.size);
                await onVideo(VideoItem.FromDocument(chatId, message.ID, document));
            }

            var nextOffset = page.Messages[^1].ID;
            if (nextOffset <= startingMessageId || nextOffset == offsetId)
                break;
            offsetId = nextOffset;
            Ui.Print(
                $"[SCAN] New messages: {result.MessagesScanned:N0} | " +
                $"new videos: {result.VideosFound:N0}");
        }

        catalog.LastScannedMessageId = highestSeenId;
        catalog.LastIncrementalScanUtc = DateTime.UtcNow;
        return result;
    }

    private static bool TryGetVideo(TL.Message message, out Document document)
    {
        document = null!;
        if (message.media is not MessageMediaDocument { document: Document candidate })
            return false;
        var isVideo = candidate.mime_type?.StartsWith(
                          "video/",
                          StringComparison.OrdinalIgnoreCase) == true ||
                      candidate.attributes.OfType<DocumentAttributeVideo>().Any();
        if (!isVideo)
            return false;
        document = candidate;
        return true;
    }

    private static async Task<T> ExecuteRequestAsync<T>(
        Func<Task<T>> operation,
        string description,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation()
                    .WaitAsync(AppConstants.TelegramRequestTimeout, cancellationToken);
            }
            catch (RpcException ex) when (ex.Code == 420 && ex.X > 0)
            {
                Ui.Print($"[WAIT] Telegram requested {ex.X}s during {description}.");
                await Task.Delay(TimeSpan.FromSeconds(ex.X + 1), cancellationToken);
            }
            catch (TimeoutException) when (attempt <= AppConstants.MaxDownloadRetries)
            {
                Ui.Print($"[RETRY] Timed out during {description}; attempt {attempt}.");
                await Task.Delay(TimeSpan.FromSeconds(attempt * 2), cancellationToken);
            }
        }
    }
}
