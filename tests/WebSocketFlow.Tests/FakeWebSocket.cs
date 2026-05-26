using System.Net.WebSockets;

namespace WebSocketFlow.Tests;

/// <summary>
/// A controllable fake WebSocket that replays pre-queued ReceiveAsync results.
/// Each queued item represents one underlying ReceiveAsync call (i.e., one fragment read).
/// </summary>
internal sealed class FakeWebSocket : WebSocket
{
    public record Fragment(byte[] Data, WebSocketMessageType MessageType, bool EndOfMessage);

    private sealed record ExceptionFragment(Exception Exception) : Fragment([], WebSocketMessageType.Text, false);

    private readonly Queue<Fragment> _fragments = new();

    public override WebSocketCloseStatus? CloseStatus => null;
    public override string? CloseStatusDescription => null;
    public override WebSocketState State { get; } = WebSocketState.Open;
    public override string? SubProtocol => null;

    public override void Abort()
    {
    }

    public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;

    public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;

    public override void Dispose()
    {
    }

    public void Enqueue(byte[] data, WebSocketMessageType type, bool endOfMessage) =>
                        _fragments.Enqueue(new(data, type, endOfMessage));

    public void EnqueueClose() =>
        _fragments.Enqueue(new([], WebSocketMessageType.Close, true));

    public void EnqueueException(Exception exception) =>
        _fragments.Enqueue(new ExceptionFragment(exception));

    public override async ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer, CancellationToken cancellationToken)
    {
        await Task.Yield();

        if (!_fragments.TryDequeue(out var fragment))
            throw new InvalidOperationException("No more queued fragments.");

        if (fragment is ExceptionFragment ex)
            throw ex.Exception;

        fragment.Data.AsSpan().CopyTo(buffer.Span);
        return new(fragment.Data.Length, fragment.MessageType, fragment.EndOfMessage);
    }

    // Unused overrides required by the abstract base class
    public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        var result = await ReceiveAsync(buffer.AsMemory(), cancellationToken);
        return new(result.Count, result.MessageType, result.EndOfMessage);
    }

    public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken) => Task.CompletedTask;
}