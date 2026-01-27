using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

const int DiscoveryPort = 54545;
const string DiscoveryMessage = "DISCOVER_BOOTH_V1";
const int RetryDelayMs = 2000;

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss ";
        });
});

var logger = loggerFactory.CreateLogger("OperatorDiscovery");

Console.WriteLine("Discovering Booth...");
logger.LogInformation("Discovery started");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var state = new DiscoveryState();
var client = new BoothDiscoveryClient(logger, DiscoveryPort, DiscoveryMessage, RetryDelayMs);

_ = Task.Run(async () =>
{
    var booth = await client.DiscoverAsync(cts.Token);
    if (booth is null)
    {
        logger.LogWarning("Discovery canceled before a Booth was found");
        return;
    }

    state.Url = $"http://{booth.Endpoint.Address}:{booth.Endpoint.Port}";
    state.NodeId = booth.NodeId;
    state.Version = booth.Version;
    state.IsReady = true;
}, cts.Token);

var builder = WebApplication.CreateBuilder();
builder.WebHost.UseUrls("http://127.0.0.1:7777");

var app = builder.Build();
app.MapGet("/booth", () =>
{
    if (!state.IsReady)
    {
        return Results.Json(new { status = "discovering" });
    }

    return Results.Json(new { status = "ready", url = state.Url });
});

await app.StartAsync(cts.Token);
await app.WaitForShutdownAsync(cts.Token);

internal sealed class BoothDiscoveryClient
{
    private readonly ILogger _logger;
    private readonly int _port;
    private readonly string _message;
    private readonly int _retryDelayMs;

    public BoothDiscoveryClient(ILogger logger, int port, string message, int retryDelayMs)
    {
        _logger = logger;
        _port = port;
        _message = message;
        _retryDelayMs = retryDelayMs;
    }

    public async Task<BoothDiscoveryInfo?> DiscoverAsync(CancellationToken cancellationToken)
    {
        using var udpClient = new UdpClient(0)
        {
            EnableBroadcast = true
        };

        var broadcastTargets = GetBroadcastTargets(_port);

        _logger.LogInformation("Broadcast targets {Targets}", string.Join(", ", broadcastTargets));

        while (!cancellationToken.IsCancellationRequested)
        {
            await BroadcastDiscoveryAsync(udpClient, broadcastTargets, cancellationToken);

            var found = await WaitForResponseAsync(udpClient, cancellationToken);
            if (found is not null)
            {
                return found;
            }

            _logger.LogInformation("No Booth response yet. Retrying discovery");
        }

        return null;
    }

    private async Task BroadcastDiscoveryAsync(UdpClient udpClient, IReadOnlyList<IPEndPoint> targets, CancellationToken cancellationToken)
    {
        var payload = Encoding.UTF8.GetBytes(_message);

        foreach (var target in targets)
        {
            try
            {
                await udpClient.SendAsync(payload, target, cancellationToken);
                _logger.LogInformation("Discovery broadcast sent to {Target}", target);
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
            {
                _logger.LogWarning(ex, "Failed to send discovery broadcast to {Target}", target);
            }
        }
    }

    private async Task<BoothDiscoveryInfo?> WaitForResponseAsync(UdpClient udpClient, CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_retryDelayMs);

        while (!timeoutCts.IsCancellationRequested)
        {
            try
            {
                var result = await udpClient.ReceiveAsync(timeoutCts.Token);
                var responseText = Encoding.UTF8.GetString(result.Buffer);
                _logger.LogInformation("Discovery response received from {Remote} {Payload}", result.RemoteEndPoint, responseText);

                var booth = ParseResponse(responseText);
                if (booth is not null)
                {
                    _logger.LogInformation("Booth selected {BoothIp}:{BoothPort}", booth.Endpoint.Address, booth.Endpoint.Port);
                    return booth;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Invalid JSON response received");
            }
            catch (SocketException ex)
            {
                _logger.LogWarning(ex, "Socket error while receiving responses");
            }
        }

        return null;
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private static BoothDiscoveryInfo? ParseResponse(string responseText)
    {
        var response = JsonSerializer.Deserialize<BoothDiscoveryResponse>(responseText, JsonOptions);
        if (response is null)
        {
            return null;
        }

        if (!string.Equals(response.Role, "booth-operator", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (!IPAddress.TryParse(response.Ip, out var ipAddress) || ipAddress.AddressFamily != AddressFamily.InterNetwork)
        {
            return null;
        }

        if (response.UiPort is null || response.UiPort <= 0 || response.UiPort > 65535)
        {
            return null;
        }

        return new BoothDiscoveryInfo(new IPEndPoint(ipAddress, response.UiPort.Value), response.NodeId, response.Version);
    }

    private static IReadOnlyList<IPEndPoint> GetBroadcastTargets(int port)
    {
        var targets = new List<IPEndPoint>();

        foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (networkInterface.OperationalStatus != OperationalStatus.Up ||
                networkInterface.NetworkInterfaceType == NetworkInterfaceType.Loopback)
            {
                continue;
            }

            var ipProperties = networkInterface.GetIPProperties();
            foreach (var unicast in ipProperties.UnicastAddresses)
            {
                if (unicast.Address.AddressFamily != AddressFamily.InterNetwork)
                {
                    continue;
                }

                if (unicast.IPv4Mask is null)
                {
                    continue;
                }

                var broadcast = ComputeBroadcast(unicast.Address, unicast.IPv4Mask);
                targets.Add(new IPEndPoint(broadcast, port));
            }
        }

        if (targets.Count == 0)
        {
            targets.Add(new IPEndPoint(IPAddress.Broadcast, port));
        }

        return targets;
    }

    private static IPAddress ComputeBroadcast(IPAddress address, IPAddress mask)
    {
        var addressBytes = address.GetAddressBytes();
        var maskBytes = mask.GetAddressBytes();
        var broadcastBytes = new byte[addressBytes.Length];

        for (var i = 0; i < addressBytes.Length; i++)
        {
            broadcastBytes[i] = (byte)(addressBytes[i] | ~maskBytes[i]);
        }

        return new IPAddress(broadcastBytes);
    }
}

internal sealed class DiscoveryState
{
    public volatile bool IsReady;
    public string? Url;
    public string? NodeId;
    public string? Version;
}

internal sealed record BoothDiscoveryInfo(IPEndPoint Endpoint, string? NodeId, string? Version);

internal sealed class BoothDiscoveryResponse
{
    public string? NodeId { get; init; }
    public string? Role { get; init; }
    public string? Ip { get; init; }
    public int? UiPort { get; init; }
    public string? Version { get; init; }
}
