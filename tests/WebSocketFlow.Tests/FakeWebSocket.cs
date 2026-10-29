using System.Net.WebSockets;

namespace WebSocketFlow.Tests;

/// <summary>
/// A controllable fake WebSocket that replays pre-queued ReceiveAsync results.
/// Each queued item represents one underlying ReceiveAsync call (i.e., one fragment read).
/// </summary>
internal sealed class FakeWebSocket : WebSocket
{
    /// <summary>
    /// One queued frame: its payload, message type and end-of-message flag.
    /// </summary>
    public record Fragment(byte[] Data, WebSocketMessageType MessageType, bool EndOfMessage);

    /// <summary>
    /// A queued item that makes the next receive throw instead of returning data.
    /// </summary>
    private sealed record ExceptionFragment(Exception Exception) : Fragment([], WebSocketMessageType.Text, false);

    /// <summary>
    /// The frames that have not been started yet.
    /// </summary>
    private readonly Queue<Fragment> _fragments = new();

    /// <summary>
    /// The frame currently being handed out, when the buffer was smaller than the frame.
    /// </summary>
    private Fragment? _current;

    /// <summary>
    /// How many bytes of the current frame were already handed out.
    /// </summary>
    private int _offset;

    /// <inheritdoc />
    public override WebSocketCloseStatus? CloseStatus => null;

    /// <inheritdoc />
    public override string? CloseStatusDescription => null;

    /// <inheritdoc />
    public override WebSocketState State
        => StateException is not null
            ? throw StateException
            : ThrowOnStateRead ? throw new ObjectDisposedException(nameof(FakeWebSocket)) : StateOverride;

    /// <summary>
    /// Gets or sets a value indicating whether reading <see cref="State"/> throws, like a disposed socket.
    /// </summary>
    public bool ThrowOnStateRead { get; set; }

    /// <summary>
    /// Gets or sets an exception that reading <see cref="State"/> throws.
    /// </summary>
    public Exception? StateException { get; set; }

    /// <summary>
    /// Gets or sets the state reported by <see cref="State"/>.
    /// </summary>
    public WebSocketState StateOverride { get; set; } = WebSocketState.Open;

    /// <summary>
    /// Length of the buffer passed to the most recent ReceiveAsync call.
    /// </summary>
    public int LastBufferLength { get; private set; }

    /// <summary>
    /// Invoked at the start of every ReceiveAsync call.
    /// </summary>
    public Action? OnReceive { get; set; }

    /// <summary>
    /// When set, a receive on an empty queue waits until cancelled instead of throwing.
    /// </summary>
    public bool BlockWhenEmpty { get; set; }

    /// <summary>
    /// Awaited at the start of every ReceiveAsync call, e.g. to simulate a slow peer.
    /// </summary>
    public Func<CancellationToken, Task>? BeforeReceive { get; set; }

    /// <inheritdoc />
    public override string? SubProtocol => null;

    /// <summary>
    /// Replaces the behaviour of CloseOutputAsync, e.g. to fail or to hang until cancelled.
    /// </summary>
    public Func<CancellationToken, Task>? OnCloseOutput { get; set; }

    /// <summary>
    /// Status passed to the most recent CloseOutputAsync call, if any.
    /// </summary>
    public WebSocketCloseStatus? LastCloseOutputStatus { get; private set; }

    /// <inheritdoc />
    public override void Abort() => StateOverride = WebSocketState.Aborted;

    /// <inheritdoc />
    public override Task CloseAsync(
        WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        => Task.CompletedTask;

    /// <inheritdoc />
    public override Task CloseOutputAsync(
        WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
    {
        LastCloseOutputStatus = closeStatus;
        return OnCloseOutput?.Invoke(cancellationToken) ?? Task.CompletedTask;
    }

    /// <inheritdoc />
    public override void Dispose()
    {
    }

    /// <summary>
    /// Queues one frame; it is split across several receives when the buffer is smaller than the frame.
    /// </summary>
    public void Enqueue(byte[] data, WebSocketMessageType type, bool endOfMessage) =>
        _fragments.Enqueue(new(data, type, endOfMessage));

    /// <summary>
    /// Queues a close frame.
    /// </summary>
    public void EnqueueClose() =>
        _fragments.Enqueue(new([], WebSocketMessageType.Close, true));

    /// <summary>
    /// Queues an exception that the next receive throws.
    /// </summary>
    public void EnqueueException(Exception exception) =>
        _fragments.Enqueue(new ExceptionFragment(exception));

    /// <inheritdoc />
    public override async ValueTask<ValueWebSocketReceiveResult> ReceiveAsync(
        Memory<byte> buffer, CancellationToken cancellationToken)
    {
        LastBufferLength = buffer.Length;
        OnReceive?.Invoke();
        if (BeforeReceive is not null)
        {
            await BeforeReceive(cancellationToken);
        }

        await Task.Yield();

        if (_current is null)
        {
            if (!_fragments.TryDequeue(out var next))
            {
                if (BlockWhenEmpty)
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }

                throw new InvalidOperationException("No more queued fragments.");
            }

            if (next is ExceptionFragment ex)
            {
                throw ex.Exception;
            }

            _current = next;
            _offset = 0;
        }

        // Like a real socket, hand out at most buffer.Length bytes and keep the rest for the next read.
        var count = Math.Min(buffer.Length, _current.Data.Length - _offset);
        _current.Data.AsSpan(_offset, count).CopyTo(buffer.Span);
        _offset += count;

        var finished = _offset == _current.Data.Length;
        var endOfMessage = _current.EndOfMessage && finished;
        var messageType = _current.MessageType;
        if (finished)
        {
            _current = null;
        }

        return new(count, messageType, endOfMessage);
    }

    // Unused overrides required by the abstract base class
    /// <inheritdoc />
    public override async Task<WebSocketReceiveResult> ReceiveAsync(
        ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        var result = await ReceiveAsync(buffer.AsMemory(), cancellationToken);
        return new(result.Count, result.MessageType, result.EndOfMessage);
    }

    /// <inheritdoc />
    public override Task SendAsync(
        ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
