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
using Microsoft.Extensions.DependencyInjection;

const int DiscoveryPort = 54545;
const string DiscoveryMessage = "DISCOVER_BOOTH_V1";
const int RetryDelayMs = 2000;
const int DiscoveryWindowMs = 10_000;
const string PersistedFileName = "booth-discovery.json";
const string PersistedFolderName = "Guestcam\\operator-utils-app";

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
var programData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
var persistencePath = Path.Combine(programData, PersistedFolderName, PersistedFileName);
var persistence = new BoothPersistence(persistencePath, logger);
persistence.TryLoad(state);

var client = new BoothDiscoveryClient(logger, DiscoveryPort, DiscoveryMessage, RetryDelayMs);
var discoveryService = new BoothDiscoveryService(client, state, persistence, TimeSpan.FromMilliseconds(DiscoveryWindowMs), logger);
discoveryService.Start(cts.Token);

var builder = WebApplication.CreateBuilder();
builder.WebHost.UseUrls("http://127.0.0.1:7777");
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();
app.UseCors();

app.MapGet("/booth", () =>
{
    lock (state.Sync)
    {
        if (state.Selected is not null)
        {
            return Results.Json(BoothPayload.Ready(state.Selected));
        }

        if (state.Discovered.Count > 1)
        {
            var booths = state.Discovered.Select(BoothPayload.ForList).ToArray();
            return Results.Json(new { status = "select", booths });
        }

        if (state.Discovered.Count == 1)
        {
            state.Selected = state.Discovered[0];
            persistence.Save(state);
            return Results.Json(BoothPayload.Ready(state.Selected));
        }

        return Results.Json(new { status = "discovering" });
    }
});

app.MapPost("/booth/select", async (HttpRequest request) =>
{
    var selection = await request.ReadFromJsonAsync<BoothSelectionRequest>();
    if (selection is null || string.IsNullOrWhiteSpace(selection.Ip) || selection.UiPort is null)
    {
        return Results.BadRequest(new { error = "ip and uiPort are required" });
    }

    BoothDiscoveryInfo? selected = null;

    lock (state.Sync)
    {
        selected = state.Discovered.FirstOrDefault(info =>
            info.Endpoint.Address.ToString() == selection.Ip &&
            info.Endpoint.Port == selection.UiPort.Value);

        if (selected is null)
        {
            return Results.NotFound(new { error = "booth not found" });
        }

        state.Selected = selected;
        persistence.Save(state);
    }

    return Results.Json(BoothPayload.Ready(selected));
});

app.MapPost("/booth/clear", () =>
{
    discoveryService.Reset();
    return Results.Json(new { status = "cleared" });
});

await app.StartAsync(cts.Token);
await app.WaitForShutdownAsync(cts.Token);

internal sealed class BoothDiscoveryService
{
    private readonly BoothDiscoveryClient _client;
    private readonly DiscoveryState _state;
    private readonly BoothPersistence _persistence;
    private readonly TimeSpan _window;
    private readonly ILogger _logger;
    private CancellationTokenSource _resetCts = new();

    public BoothDiscoveryService(
        BoothDiscoveryClient client,
        DiscoveryState state,
        BoothPersistence persistence,
        TimeSpan window,
        ILogger logger)
    {
        _client = client;
        _state = state;
        _persistence = persistence;
        _window = window;
        _logger = logger;
    }

    public void Start(CancellationToken applicationToken)
    {
        _ = Task.Run(() => RunAsync(applicationToken), applicationToken);
    }

    public void Reset()
    {
        lock (_state.Sync)
        {
            _state.Clear();
        }

        _persistence.Clear();

        var previous = Interlocked.Exchange(ref _resetCts, new CancellationTokenSource());
        previous.Cancel();
        previous.Dispose();
    }

    private async Task RunAsync(CancellationToken applicationToken)
    {
        while (!applicationToken.IsCancellationRequested)
        {
            BoothDiscoveryInfo? selected;
            bool discoveryComplete;

            lock (_state.Sync)
            {
                selected = _state.Selected;
                discoveryComplete = _state.DiscoveryComplete;
            }

            if (selected is not null || discoveryComplete)
            {
                await Task.Delay(500, applicationToken);
                continue;
            }

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(applicationToken, _resetCts.Token);
            IReadOnlyList<BoothDiscoveryInfo> discovered;

            try
            {
                discovered = await _client.DiscoverAllAsync(_window, linkedCts.Token);
            }
            catch (OperationCanceledException)
            {
                continue;
            }

            if (linkedCts.IsCancellationRequested)
            {
                continue;
            }

            if (discovered.Count == 0)
            {
                _logger.LogInformation("No Booth responses received during discovery window");
                continue;
            }

            lock (_state.Sync)
            {
                _state.Discovered = discovered.ToList();
                _state.DiscoveryComplete = true;

                if (_state.Discovered.Count == 1)
                {
                    _state.Selected = _state.Discovered[0];
                }
            }

            _persistence.Save(_state);
        }
    }
}

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

    public async Task<IReadOnlyList<BoothDiscoveryInfo>> DiscoverAllAsync(TimeSpan window, CancellationToken cancellationToken)
    {
        using var udpClient = new UdpClient(0)
        {
            EnableBroadcast = true
        };

        var broadcastTargets = GetBroadcastTargets(_port);

        _logger.LogInformation("Broadcast targets {Targets}", string.Join(", ", broadcastTargets));

        await BroadcastDiscoveryAsync(udpClient, broadcastTargets, cancellationToken);

        var discovered = new Dictionary<string, BoothDiscoveryInfo>(StringComparer.OrdinalIgnoreCase);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(window);

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
                    var key = $"{booth.Endpoint.Address}:{booth.Endpoint.Port}";
                    discovered[key] = booth;
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

        return discovered.Values.OrderBy(info => info.EventName).ThenBy(info => info.Endpoint.Address.ToString()).ToList();
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

        return new BoothDiscoveryInfo(
            new IPEndPoint(ipAddress, response.UiPort.Value),
            response.NodeId,
            response.Version,
            response.EventName,
            response.ClientName);
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

internal sealed class BoothPersistence
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _path;
    private readonly ILogger _logger;

    public BoothPersistence(string path, ILogger logger)
    {
        _path = path;
        _logger = logger;
        EnsureDirectory();
    }

    public void TryLoad(DiscoveryState state)
    {
        if (!File.Exists(_path))
        {
            return;
        }

        try
        {
            var json = File.ReadAllText(_path);
            var persisted = JsonSerializer.Deserialize<PersistedBoothState>(json, JsonOptions);
            if (persisted is null)
            {
                return;
            }

            lock (state.Sync)
            {
                state.Discovered = persisted.Discovered
                    .Select(MapToInfo)
                    .Where(info => info is not null)
                    .Select(info => info!)
                    .ToList();

                state.Selected = MapToInfo(persisted.Selected);
                state.DiscoveryComplete = state.Selected is not null || state.Discovered.Count > 0;
            }
        }
        catch (Exception ex) when (ex is IOException or JsonException)
        {
            _logger.LogWarning(ex, "Failed to load persisted booth settings from {Path}", _path);
        }
    }

    public void Save(DiscoveryState state)
    {
        PersistedBoothState persisted;

        lock (state.Sync)
        {
            persisted = new PersistedBoothState
            {
                SavedUtc = DateTime.UtcNow,
                Selected = MapToPersisted(state.Selected),
                Discovered = state.Discovered
                    .Select(MapToPersisted)
                    .Where(info => info is not null)
                    .Select(info => info!)
                    .ToList()
            };
        }

        var tempPath = _path + ".tmp";
        var json = JsonSerializer.Serialize(persisted, JsonOptions);

        EnsureDirectory();
        File.WriteAllText(tempPath, json);
        File.Copy(tempPath, _path, true);
        File.Delete(tempPath);
    }

    public void Clear()
    {
        if (!File.Exists(_path))
        {
            return;
        }

        try
        {
            File.Delete(_path);
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Failed to delete persisted booth settings at {Path}", _path);
        }
    }

    private void EnsureDirectory()
    {
        var directory = Path.GetDirectoryName(_path);
        if (string.IsNullOrWhiteSpace(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
    }

    private static BoothDiscoveryInfo? MapToInfo(PersistedBoothInfo? persisted)
    {
        if (persisted is null || string.IsNullOrWhiteSpace(persisted.Ip) || persisted.UiPort <= 0)
        {
            return null;
        }

        if (!IPAddress.TryParse(persisted.Ip, out var ipAddress))
        {
            return null;
        }

        return new BoothDiscoveryInfo(
            new IPEndPoint(ipAddress, persisted.UiPort),
            persisted.NodeId,
            persisted.Version,
            persisted.EventName,
            persisted.ClientName);
    }

    private static PersistedBoothInfo? MapToPersisted(BoothDiscoveryInfo? info)
    {
        if (info is null)
        {
            return null;
        }

        return new PersistedBoothInfo
        {
            Ip = info.Endpoint.Address.ToString(),
            UiPort = info.Endpoint.Port,
            NodeId = info.NodeId,
            Version = info.Version,
            EventName = info.EventName,
            ClientName = info.ClientName
        };
    }
}

internal sealed class DiscoveryState
{
    public readonly object Sync = new();
    public bool DiscoveryComplete;
    public List<BoothDiscoveryInfo> Discovered = new();
    public BoothDiscoveryInfo? Selected;

    public void Clear()
    {
        DiscoveryComplete = false;
        Discovered.Clear();
        Selected = null;
    }
}

internal sealed record BoothDiscoveryInfo(
    IPEndPoint Endpoint,
    string? NodeId,
    string? Version,
    string? EventName,
    string? ClientName);

internal sealed class BoothDiscoveryResponse
{
    public string? NodeId { get; init; }
    public string? Role { get; init; }
    public string? Ip { get; init; }
    public int? UiPort { get; init; }
    public string? Version { get; init; }
    public string? EventName { get; init; }
    public string? ClientName { get; init; }
}

internal sealed class BoothSelectionRequest
{
    public string? Ip { get; init; }
    public int? UiPort { get; init; }
}

internal sealed class PersistedBoothState
{
    public DateTime? SavedUtc { get; set; }
    public PersistedBoothInfo? Selected { get; set; }
    public List<PersistedBoothInfo> Discovered { get; set; } = new();
}

internal sealed class PersistedBoothInfo
{
    public string? Ip { get; set; }
    public int UiPort { get; set; }
    public string? NodeId { get; set; }
    public string? Version { get; set; }
    public string? EventName { get; set; }
    public string? ClientName { get; set; }
}

internal static class BoothPayload
{
    public static object Ready(BoothDiscoveryInfo booth)
    {
        return new
        {
            status = "ready",
            url = BuildUrl(booth),
            booth = ForList(booth)
        };
    }

    public static object ForList(BoothDiscoveryInfo booth)
    {
        return new
        {
            ip = booth.Endpoint.Address.ToString(),
            uiPort = booth.Endpoint.Port,
            url = BuildUrl(booth),
            eventName = booth.EventName,
            clientName = booth.ClientName,
            version = booth.Version,
            nodeId = booth.NodeId
        };
    }

    private static string BuildUrl(BoothDiscoveryInfo booth)
    {
        return $"http://{booth.Endpoint.Address}:{booth.Endpoint.Port}";
    }
}
