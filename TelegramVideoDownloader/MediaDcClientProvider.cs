using System.Collections.Concurrent;
using WTelegram;

internal sealed class MediaDcClientProvider(Client mainClient)
{
    private readonly ConcurrentDictionary<int, SharedConnection<Client>> _connections = new();

    internal int ResolveDc(int dcId) => dcId == 0
        ? mainClient.TLConfig.this_dc
        : Math.Abs(dcId);

    public Task<Client> GetAsync(int dcId, CancellationToken cancellationToken) =>
        _connections.GetOrAdd(ResolveDc(dcId), id => new SharedConnection<Client>(
            async () =>
            {
                // Setup is not cancellable in WTelegram. Share the actual task;
                // never release a creation lock while a timed-out task continues.
                var client = await mainClient.GetClientForDC(-id, connect: true);
                client.MaxAutoReconnects = Math.Max(client.MaxAutoReconnects, 10);
                // PartRequestPolicy honors short waits outside the RPC timer;
                // the engine coordinates longer waits across workers.
                client.FloodRetryThreshold = 0;
                return client;
            },
            async client =>
            {
                if (ReferenceEquals(client, mainClient) || client.IsMainDC)
                    throw new InvalidOperationException("Refusing to dispose the main Telegram session during media recovery.");
                await client.DisposeAsync();
            })).GetAsync(cancellationToken);

    public bool Retire(int dcId, Client observedClient) =>
        _connections.TryGetValue(ResolveDc(dcId), out var connection) &&
        connection.Retire(observedClient);
}
