using System.Buffers;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace WebSocketFlow;

/// <summary>
/// Provides extension methods for <see cref="System.Net.WebSockets.WebSocket"/> to simplify
/// receiving complete messages and asynchronously enumerating incoming messages.
/// </summary>
public static class WebSocketFlow
{
    /// <summary>
    /// Default buffer size (in bytes) for intermediate fragment reads when receiving WebSocket messages.
    /// </summary>
    private const int DefaultBufferSize = 4096;

    /// <summary>
    /// Asynchronously enumerates complete WebSocket messages until the socket closes or
    /// the cancellation token is triggered.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to read messages from.</param>
    /// <param name="bufferSize">
    /// Size of the intermediate receive buffer per fragment read. Defaults to 4096 bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> that can be used to stop enumeration early.
    /// </param>
    /// <returns>
    /// An <see cref="IAsyncEnumerable{T}"/> of <see cref="WebSocketMessage"/> representing
    /// each complete message received from the WebSocket.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="webSocket"/> is <see langword="null"/>.
    /// </exception>
    public static async IAsyncEnumerable<WebSocketMessage> ReadAllMessagesAsync(
        this WebSocket webSocket,
        int bufferSize = DefaultBufferSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (webSocket is null)
            throw new ArgumentNullException(nameof(webSocket));

        while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
        {
            WebSocketMessage message;
            try
            {
                message = await webSocket.ReceiveMessageAsync(bufferSize, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }
            catch (WebSocketException)
            {
                yield break;
            }

            if (message.CloseReceived)
                yield break;

            yield return message;
        }
    }

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments.
    /// Uses the default buffer size of 4096 bytes.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask{T}"/> whose result is a <see cref="WebSocketMessage"/>
    /// containing the message type and the fully assembled payload.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="webSocket"/> is <see langword="null"/>.
    /// </exception>
    public static ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        CancellationToken cancellationToken = default)
    {
        return ReceiveMessageAsync(webSocket, DefaultBufferSize, cancellationToken);
    }

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask{T}"/> whose result is a <see cref="WebSocketMessage"/>
    /// containing the message type and the fully assembled payload.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="webSocket"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="bufferSize"/> is less than or equal to zero.
    /// </exception>
    public static async ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        int bufferSize,
        CancellationToken cancellationToken = default)
    {
        if (webSocket is null)
            throw new ArgumentNullException(nameof(webSocket));
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "Buffer size must be positive.");

        var rentedBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            var result = await ReceiveAsync(webSocket, rentedBuffer, cancellationToken).ConfigureAwait(false);

            if (result.EndOfMessage)
            {
                var data = new byte[result.Count];
                rentedBuffer.AsSpan(0, result.Count).CopyTo(data);
                return new(result.MessageType, data);
            }

            using var accumulator = new SegmentedBuffer();
            accumulator.Append(rentedBuffer.AsSpan(0, result.Count));

            while (!result.EndOfMessage)
            {
                result = await ReceiveAsync(webSocket, rentedBuffer, cancellationToken).ConfigureAwait(false);
                accumulator.Append(rentedBuffer.AsSpan(0, result.Count));
            }

            return new(result.MessageType, accumulator.ToArray());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    /// <summary>
    /// Receives data asynchronously from the specified WebSocket and writes it into the provided buffer.
    /// </summary>
    /// <param name="webSocket">The WebSocket instance from which to receive data. Must not be null and must be open for reading.</param>
    /// <param name="buffer">The buffer that will receive the incoming data. The method writes the received bytes into this array segment.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests. If cancellation is requested, the operation is canceled.</param>
    /// <returns>
    /// A task that represents the asynchronous receive operation. The task result contains a WebSocketReceiveResult describing the received message.
    /// </returns>
    private static Task<WebSocketReceiveResult> ReceiveAsync(
        WebSocket webSocket, byte[] buffer, CancellationToken cancellationToken)
        => webSocket.ReceiveAsync(new(buffer), cancellationToken);
}