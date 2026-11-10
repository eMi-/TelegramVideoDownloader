# Telegram Video Downloader

Download videos on Windows from public Telegram channels and from private
channels or groups you've joined. In chats with **Restrict Saving Content**
enabled, the downloader can save a video when Telegram's API returns its
media to your signed-in account.

## Telegram login

Create your Telegram API application at [my.telegram.org](https://my.telegram.org/apps).
Set `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and `TELEGRAM_PHONE_NUMBER` in your
environment, or create `%LOCALAPPDATA%\TelegramVideoDownloader\telegram-credentials.json`
with your own values:

```json
{
  "api_id": "YOUR_API_ID",
  "api_hash": "YOUR_API_HASH",
  "phone_number": "YOUR_PHONE_IN_INTERNATIONAL_FORMAT"
}
```

The environment overrides the corresponding settings in the file. On the first
login, enter the verification code and, if enabled, your Telegram 2FA password
when prompted. Later runs reuse the existing session.

## Performance design

- Initial discovery uses Telegram's server-side video filter instead of reading
  every text message.
- Later runs use a per-channel incremental catalogue and only inspect new
  messages. A filtered reconciliation scan runs every seven days.
- Discovery and downloading are pipelined, so the first videos start while later
  result pages are still being scanned.
- File parts download in parallel with live transfer-count auto-tuning.
- Secondary Telegram media-DC clients are cached per DC. Reconnection is
  serialized without blocking healthy part transfers, and normal secondary-DC
  shutdowns resume automatically instead of aborting the file.
- A part that does not finish within 45 seconds retires its media connection,
  including outstanding RPCs. Concurrent failures share one replacement and
  retry the same offsets; late failures cannot close the replacement. Connection
  creation is shared even when a waiting file is cancelled. Retries are bounded;
  a persistently unavailable file is reported failed and the queue continues.
- The file scheduler allows more simultaneous small files and reserves the full
  transfer budget for videos larger than 4 GB.
- Partial downloads use atomic contiguous-prefix checkpoints and resume without
  downloading checkpointed bytes again. Periodic checkpoints avoid expensive
  whole-disk barriers; completed files are force-flushed before finalization.
- Download index and catalogue changes are written in atomic batches instead of
  rewriting JSON after every video.
- Telegram requests have timeouts, retry handling, flood-wait accounting, and
  coordinated in-place expired file-reference refresh. Active part workers
  share one refresh and continue their same offsets without restarting a file.
- Cached references carry a freshness timestamp. Legacy or older-than-30-minute
  references are refreshed immediately before their queued video starts.

## Performance configuration

The selected destination receives
`.telegram-video-downloader-settings.json`. It can be edited between runs.
Environment variables override that file:

| Variable | Values |
| --- | --- |
| `TVD_PROFILE` | `auto`, `conservative`, `balanced`, `fast`, `experimental` |
| `TVD_TRANSFERS` | `1` through `16` |
| `TVD_PART_SIZE_KB` | `256`, `512`, or `1024` |
| `TVD_VIDEO_SLOTS` | `1` through `8` |
| `TVD_AUTOTUNE` | `true` or `false` |

`auto` measures the configured transfer count first (within 4-8, default 8),
then benchmarks the remaining values among 4, 6, and 8 during real downloads.
Retry/timeout pressure is included in the score. Short Telegram waits stop
benchmarking and reduce concurrency on the affected media DC. Connection recovery or longer waits
stop benchmarking and allow the recovery policy to reduce pressure.
Only a completed benchmark changes the saved transfer count; temporary recovery
throttles do not become the starting setting for the next run.

## Running unattended and diagnosing stalls

Use **Start Without Debugging (Ctrl+F5)** in Visual Studio, or run
`TelegramVideoDownloader/bin/Release/net10.0-windows/TelegramVideoDownloader.exe`.
An attached debugger can pause on exceptions before recovery handlers run.
Moving a catch block does not override the debugger's exception settings.

You can supply the numbered selection, destination folder, and start confirmation
on the command line. Omitted options use their usual interactive prompts.

```powershell
.\TelegramVideoDownloader\bin\Release\net10.0-windows\TelegramVideoDownloader.exe --channel-number 3 --destination "D:\Example\Videos" --yes
```

`[RECONNECT]` includes the media DC and reason; `[PART RETRY]` includes the
message and byte offset. `ETA --:--:--` means speed is currently zero. The status
also shows how long no data has arrived and how many parts are waiting.
When a media DC requests a wait of up to 60 seconds, `upload.getFile` requests
for that DC pause for the full duration. Sleeping workers release their shared
transfer slots. After the longest outstanding wait expires, the downloader
tries one part and increases parallelism as successful responses arrive. The
file scheduler gives available slots to healthy DCs first. The status shows
which DC is waiting or recovering. The 45-second RPC timer measures requests
on the network and excludes time spent waiting for admission.
If a video has made no byte progress for 30 seconds, its DC is waiting, and a
queued video on an available DC needs its file slots, `[DEFER]` reports when
the downloader has checkpointed and requeued it. Parts already written out
of order are reused within the session. After restarting the app, downloads
resume from the saved contiguous prefix.
Longer waits use one shared countdown; concurrent responses may extend it, but
do not add duplicate durations. Workers resume the same offsets without spending
a retry. These changes cannot guarantee throughput from a particular Telegram
server or channel.

## Offline verification

```powershell
dotnet run --project TelegramVideoDownloader -- --self-test
```

Tests include simultaneous stalled requests, closing old RPCs before replacing
the connection, replay at identical offsets, cancellation during connection
creation, failed connection setup, resizing the transfer limit under load, and
healthy transfers progressing while another request honors a short flood wait.
They use a simulated transport and do not verify live Telegram throughput.

Keep the authentication session under `bin` and your credentials file private.
