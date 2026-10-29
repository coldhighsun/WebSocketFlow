using System.Buffers;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;

namespace WebSocketFlow;

/// <summary>
/// Provides extension methods for <see cref="System.Net.WebSockets.WebSocket"/> to simplify
/// receiving complete messages and asynchronously enumerating incoming messages.
/// </summary>
/// <remarks>
/// These methods never complete the closing handshake. After a close frame is received, call
/// <see cref="WebSocket.CloseOutputAsync(WebSocketCloseStatus, string?, CancellationToken)"/> or
/// <see cref="WebSocket.CloseAsync(WebSocketCloseStatus, string?, CancellationToken)"/> yourself.
/// </remarks>
public static class WebSocketFlow
{
    /// <summary>
    /// Default buffer size (in bytes) for intermediate fragment reads when receiving WebSocket messages.
    /// </summary>
    private const int DefaultBufferSize = 4096;

    /// <summary>
    /// Default upper bound (in bytes) for a single assembled message: 16 MiB.
    /// </summary>
    internal const int DefaultMaxMessageSize = 16 * 1024 * 1024;

    /// <summary>
    /// Upper bound (in bytes) for the per-fragment receive buffer: 1 MiB.
    /// </summary>
    private const int MaxBufferSize = 1024 * 1024;

    /// <summary>
    /// How many consecutive empty, non-final fragments are tolerated. Empty fragments add nothing to
    /// the assembled size, so without this cap a peer could keep a receive running forever.
    /// </summary>
    private const int MaxConsecutiveEmptyFragments = 1024;

    /// <summary>
    /// How long the best-effort close frame may take before the socket is aborted regardless, so a
    /// peer that stopped reading cannot stall the rejection. Callers who need a shorter bound can
    /// pass a cancellation token with a deadline.
    /// </summary>
    private static readonly TimeSpan DefaultCloseTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Longest close timeout <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/> accepts.
    /// </summary>
    private static readonly TimeSpan MaxCloseTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// Asynchronously enumerates complete WebSocket messages until the socket closes or
    /// the cancellation token is triggered. Messages are limited to 16 MiB.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to read messages from.</param>
    /// <param name="bufferSize">
    /// Size of the intermediate receive buffer per fragment read. Defaults to 4096 bytes.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> that can be used to stop enumeration early.
    /// </param>
    /// <inheritdoc cref="ReadAllMessagesAsync(WebSocket, int, int, CancellationToken)"/>
    public static IAsyncEnumerable<WebSocketMessage> ReadAllMessagesAsync(
        this WebSocket webSocket,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
        => ReadAllMessagesAsync(webSocket, bufferSize, DefaultMaxMessageSize, cancellationToken);

    /// <summary>
    /// Asynchronously enumerates complete WebSocket messages until the socket closes or
    /// the cancellation token is triggered.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to read messages from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">
    /// Maximum size in bytes of a single assembled message; values above <see cref="Array.MaxLength"/>
    /// are enforced as <see cref="Array.MaxLength"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> that can be used to stop enumeration early.
    /// </param>
    /// <returns>
    /// An <see cref="IAsyncEnumerable{T}"/> of <see cref="WebSocketMessage"/> representing
    /// each complete message received from the WebSocket.
    /// </returns>
    /// <remarks>
    /// Enumeration ends quietly when a close frame arrives, the token is cancelled, or the socket is
    /// aborted. Any failure, including a remote side that drops the connection without a closing
    /// handshake (<see cref="WebSocketError.ConnectionClosedPrematurely"/>), propagates so a truncated
    /// stream is never mistaken for a clean close. The one exception: once this side has sent its own
    /// close frame (state <see cref="WebSocketState.CloseSent"/>), a peer that drops the connection
    /// instead of replying simply ends the enumeration, unless it had already started sending a message
    /// (a drop mid-message would truncate it, so it propagates). A peer that stays silent for 5 seconds
    /// after our close frame (configurable via the <c>closeTimeout</c> overload; the timer stops once
    /// the peer starts sending) also ends the enumeration quietly. A single receive buffer is reused for
    /// the lifetime of the enumeration.
    /// When the enumeration ends because the peer stayed silent after your close frame, the pending
    /// receive was cancelled and the socket is left in whatever state the transport chose (usually
    /// <see cref="WebSocketState.CloseSent"/>); the closing handshake did not complete, so call
    /// <see cref="WebSocket.Abort"/> if you do not need it to. The timeout is checked by polling the
    /// socket state, so it is accurate to within one second.
    /// </remarks>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="webSocket"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="bufferSize"/> is not between 1 and 1 MiB, or
    /// <paramref name="maxMessageSize"/> is not positive.
    /// </exception>
    /// <exception cref="InvalidDataException">
    /// Thrown during enumeration when a message exceeds <paramref name="maxMessageSize"/>
    /// (a <see cref="WebSocketCloseStatus.MessageTooBig"/> close frame is sent) or consists of too many
    /// consecutive empty fragments (a <see cref="WebSocketCloseStatus.PolicyViolation"/> close frame is
    /// sent). The close frame is best-effort (waiting at most 5 seconds) and the socket is aborted before
    /// the exception is thrown.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown instead when <paramref name="cancellationToken"/> is cancelled while the close frame is
    /// being sent; the socket is still aborted.
    /// </exception>
    public static IAsyncEnumerable<WebSocketMessage> ReadAllMessagesAsync(
        this WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        CancellationToken cancellationToken = default)
        => ReadAllMessagesAsync(webSocket, bufferSize, maxMessageSize, DefaultCloseTimeout, cancellationToken);

    /// <summary>
    /// Asynchronously enumerates complete WebSocket messages until the socket closes or
    /// the cancellation token is triggered, with a configurable close timeout.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to read messages from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">Maximum size in bytes of a single assembled message.</param>
    /// <param name="closeTimeout">
    /// Bounds both the best-effort close frame sent when a message is rejected and, once this side has
    /// sent its own close frame, the wait for the peer to start replying. Use
    /// <see cref="Timeout.InfiniteTimeSpan"/> for no bound. The default is 5 seconds.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> that can be used to stop enumeration early.
    /// </param>
    /// <inheritdoc cref="ReadAllMessagesAsync(WebSocket, int, int, CancellationToken)"/>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Also thrown when <paramref name="closeTimeout"/> is zero, negative (other than
    /// <see cref="Timeout.InfiniteTimeSpan"/>) or longer than about 49 days.
    /// </exception>
    public static IAsyncEnumerable<WebSocketMessage> ReadAllMessagesAsync(
        this WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        TimeSpan closeTimeout,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(webSocket, bufferSize, maxMessageSize, closeTimeout);
        return ReadAllMessagesCoreAsync(
            webSocket, bufferSize, maxMessageSize, closeTimeout, TimeProvider.System, cancellationToken);
    }

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments.
    /// Uses the default buffer size of 4096 bytes and a 16 MiB message limit.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <inheritdoc cref="ReceiveMessageAsync(WebSocket, int, int, CancellationToken)"/>
    public static ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        CancellationToken cancellationToken = default)
        => ReceiveMessageAsync(webSocket, DefaultBufferSize, DefaultMaxMessageSize, cancellationToken);

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments.
    /// Messages are limited to 16 MiB.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <inheritdoc cref="ReceiveMessageAsync(WebSocket, int, int, CancellationToken)"/>
    public static ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        int bufferSize,
        CancellationToken cancellationToken = default)
        => ReceiveMessageAsync(webSocket, bufferSize, DefaultMaxMessageSize, cancellationToken);

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">
    /// Maximum size in bytes of the assembled message; values above <see cref="Array.MaxLength"/>
    /// are enforced as <see cref="Array.MaxLength"/>.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask{T}"/> whose result is a <see cref="WebSocketMessage"/>
    /// containing the message type and the fully assembled payload.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown immediately when <paramref name="webSocket"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown immediately when <paramref name="bufferSize"/> is not between 1 and 1 MiB, or
    /// <paramref name="maxMessageSize"/> is not positive.
    /// </exception>
    /// <exception cref="InvalidDataException">
    /// Thrown when the message exceeds <paramref name="maxMessageSize"/> (a
    /// <see cref="WebSocketCloseStatus.MessageTooBig"/> close frame is sent) or consists of too many
    /// consecutive empty fragments (a <see cref="WebSocketCloseStatus.PolicyViolation"/> close frame is
    /// sent). The close frame is sent on a best-effort basis and the socket is then aborted, because
    /// the rest of the message is still in flight and the stream can no longer be framed reliably.
    /// Sending the close frame waits at most 5 seconds. If sending it fails, the failure is attached
    /// as <see cref="Exception.InnerException"/>.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown instead when <paramref name="cancellationToken"/> is cancelled while the close frame is
    /// being sent; the socket is still aborted.
    /// </exception>
    public static ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        CancellationToken cancellationToken = default)
        => ReceiveMessageAsync(webSocket, bufferSize, maxMessageSize, DefaultCloseTimeout, cancellationToken);

    /// <summary>
    /// Receives a complete WebSocket message, automatically assembling all fragments, with a
    /// configurable close timeout.
    /// </summary>
    /// <param name="webSocket">The <see cref="WebSocket"/> to receive the message from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">Maximum size in bytes of the assembled message.</param>
    /// <param name="closeTimeout">
    /// Bounds the best-effort close frame sent when the message is rejected. Use
    /// <see cref="Timeout.InfiniteTimeSpan"/> for no bound. The default is 5 seconds.
    /// </param>
    /// <param name="cancellationToken">
    /// A <see cref="CancellationToken"/> to cancel the receive operation.
    /// </param>
    /// <inheritdoc cref="ReceiveMessageAsync(WebSocket, int, int, CancellationToken)"/>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Also thrown when <paramref name="closeTimeout"/> is zero, negative (other than
    /// <see cref="Timeout.InfiniteTimeSpan"/>) or longer than about 49 days.
    /// </exception>
    public static ValueTask<WebSocketMessage> ReceiveMessageAsync(
        this WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        TimeSpan closeTimeout,
        CancellationToken cancellationToken = default)
    {
        ValidateArguments(webSocket, bufferSize, maxMessageSize, closeTimeout);
        return ReceiveMessageCoreAsync(webSocket, bufferSize, maxMessageSize, closeTimeout, cancellationToken);
    }

    /// <summary>
    /// Receives one complete message with a receive buffer that lives only for this call.
    /// </summary>
    /// <param name="webSocket">The socket to receive from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">Maximum size in bytes of the assembled message.</param>
    /// <param name="closeTimeout">Bounds the best-effort close frame sent when the message is rejected.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>The assembled message.</returns>
    internal static async ValueTask<WebSocketMessage> ReceiveMessageCoreAsync(
        WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        TimeSpan closeTimeout,
        CancellationToken cancellationToken)
    {
        maxMessageSize = EffectiveMaxMessageSize(maxMessageSize);
        using var buffer = new ReceiveBuffer(Math.Min(bufferSize, maxMessageSize));
        var message = await ReceiveCoreAsync(
            webSocket, buffer, maxMessageSize, closeTimeout, null, endOnPrematureClose: false, cancellationToken)
            .ConfigureAwait(false);

        // Only a receive that may end quietly returns null.
        return message!;
    }

    /// <summary>
    /// Limits <paramref name="maxMessageSize"/> to the largest array an assembled message can occupy.
    /// </summary>
    /// <param name="maxMessageSize">The limit requested by the caller.</param>
    /// <returns>The limit that is actually enforced.</returns>
    internal static int EffectiveMaxMessageSize(int maxMessageSize) => Math.Min(maxMessageSize, Array.MaxLength);

    /// <summary>
    /// Enumerates complete messages, reusing one receive buffer for the whole enumeration.
    /// </summary>
    /// <param name="webSocket">The socket to receive from.</param>
    /// <param name="bufferSize">Size of the intermediate receive buffer per fragment read.</param>
    /// <param name="maxMessageSize">Maximum size in bytes of a single assembled message.</param>
    /// <param name="closeTimeout">
    /// Bounds the best-effort close frame and the wait for the peer's reply after our close frame.
    /// </param>
    /// <param name="timeProvider">The clock behind the silent-peer timeout.</param>
    /// <param name="cancellationToken">A token to stop the enumeration early.</param>
    /// <returns>The messages received until the stream ends.</returns>
    internal static async IAsyncEnumerable<WebSocketMessage> ReadAllMessagesCoreAsync(
        WebSocket webSocket,
        int bufferSize,
        int maxMessageSize,
        TimeSpan closeTimeout,
        TimeProvider timeProvider,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // A message larger than the biggest array could never be assembled, so that is the real limit.
        maxMessageSize = EffectiveMaxMessageSize(maxMessageSize);

        // Capped by the message limit: a larger buffer could never be filled by an accepted message.
        using var buffer = new ReceiveBuffer(Math.Min(bufferSize, maxMessageSize));

        // Once our close frame is out, a peer that neither replies nor disconnects must not hold the
        // enumeration forever: it gets closeTimeout to start answering. The watchdog polls the state, so
        // a close frame sent by another thread during a pending receive is covered as well, and it only
        // bounds the wait for the first bytes, never a message in flight.
        using var watchdog = closeTimeout == Timeout.InfiniteTimeSpan
            ? null
            : new CloseHandshakeWatchdog(webSocket, closeTimeout, cancellationToken, timeProvider);

        // CloseSent is included so the remote close frame (the handshake reply) is still read.
        while (webSocket.State is WebSocketState.Open or WebSocketState.CloseSent
               && !cancellationToken.IsCancellationRequested)
        {
            WebSocketMessage? message;
            try
            {
                message = await ReceiveCoreAsync(
                    webSocket, buffer, maxMessageSize, closeTimeout, watchdog, endOnPrematureClose: true, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (EndsEnumerationQuietly(ex, webSocket, watchdog, cancellationToken))
            {
                yield break;
            }

            // The peer dropped the connection instead of replying to our close frame.
            if (message is null || message.CloseReceived)
            {
                yield break;
            }

            yield return message;
        }
    }

    /// <summary>
    /// Decides whether a failed receive is an expected way for the enumeration to end. Anything else,
    /// such as an I/O failure or a rejected message, is a genuine failure and propagates.
    /// </summary>
    /// <param name="exception">The exception the receive threw.</param>
    /// <param name="webSocket">The socket that was being read.</param>
    /// <param name="watchdog">The silent-peer watchdog, or <see langword="null"/> when disabled.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns><see langword="true"/> if the enumeration should end without an exception.</returns>
    private static bool EndsEnumerationQuietly(
        Exception exception,
        WebSocket webSocket,
        CloseHandshakeWatchdog? watchdog,
        CancellationToken cancellationToken)
    {
        // The caller cancelled while a rejected message was being closed: that is a requested stop that
        // the documentation promises to surface, not a quiet end.
        if (exception is RejectionCancelledException)
        {
            return false;
        }

        // The silent-peer timeout cancelled the first read; transports surface that cancellation as an
        // OperationCanceledException, an ObjectDisposedException or a WebSocketException with one of
        // the codes below. Any other failure that merely coincides with the timeout still propagates.
        if (watchdog?.TimedOut == true
            && exception is OperationCanceledException
                or ObjectDisposedException
                or WebSocketException
                {
                    WebSocketErrorCode: WebSocketError.InvalidState or WebSocketError.ConnectionClosedPrematurely,
                })
        {
            return true;
        }

        return exception switch
        {
            OperationCanceledException => cancellationToken.IsCancellationRequested
                                          || webSocket.State == WebSocketState.Aborted,

            // Another thread aborted or disposed the socket while (or just before) we were receiving;
            // disposing leaves the state at Closed, aborting at Aborted.
            ObjectDisposedException => webSocket.State is WebSocketState.Aborted or WebSocketState.Closed,
            WebSocketException { WebSocketErrorCode: WebSocketError.InvalidState } =>
                webSocket.State == WebSocketState.Aborted,
            _ => false,
        };
    }

    /// <summary>
    /// Receives and assembles one message, enforcing the size and empty-fragment limits.
    /// </summary>
    /// <param name="webSocket">The socket to receive from.</param>
    /// <param name="buffer">The reusable fragment buffer.</param>
    /// <param name="maxMessageSize">Maximum size in bytes of the assembled message.</param>
    /// <param name="closeTimeout">Bounds the best-effort close frame sent when the message is rejected.</param>
    /// <param name="watchdog">
    /// Bounds the wait for the first bytes after our close frame, or <see langword="null"/> for no bound.
    /// It is stopped as soon as the first bytes arrive.
    /// </param>
    /// <param name="endOnPrematureClose">
    /// Whether a dropped connection ends the receive quietly (returning <see langword="null"/>) when our
    /// close frame was already sent and the peer has not started the message. Once the peer has started
    /// it, a drop would truncate the message, so it always propagates.
    /// </param>
    /// <param name="cancellationToken">The caller's token; it applies to every read.</param>
    /// <returns>The assembled message, or <see langword="null"/> when the receive ended quietly.</returns>
    private static async ValueTask<WebSocketMessage?> ReceiveCoreAsync(
        WebSocket webSocket,
        ReceiveBuffer buffer,
        int maxMessageSize,
        TimeSpan closeTimeout,
        CloseHandshakeWatchdog? watchdog,
        bool endOnPrematureClose,
        CancellationToken cancellationToken)
    {
        var closeAlreadySent = webSocket.State == WebSocketState.CloseSent;
        // Created lazily: a message that fits in one fragment never needs it.
        PooledMessageBuffer? accumulator = null;
        WebSocketMessageType? messageType = null;
        var emptyFragments = 0;
        var firstReadDone = false;
        var firstReadToken = watchdog?.Begin() ?? cancellationToken;
        try
        {
            while (true)
            {
                var result = await webSocket.ReceiveAsync(
                    buffer.BeginReceive(), firstReadDone ? cancellationToken : firstReadToken).ConfigureAwait(false);
                var chunk = buffer.EndReceive(result.Count);

                // The peer has started to answer: from here on only the caller's token may interrupt.
                if (!firstReadDone)
                {
                    firstReadDone = true;
                    watchdog?.End();
                }

                // A close frame can arrive between the fragments of a data message. It is not part of that
                // message: report the close and drop the partial payload instead of delivering it truncated.
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return new(WebSocketMessageType.Close, chunk.ToArray());
                }

                if (chunk.Length > maxMessageSize - (accumulator?.Length ?? 0))
                {
                    throw await RejectOversizedAsync(webSocket, maxMessageSize, closeTimeout, cancellationToken)
                        .ConfigureAwait(false);
                }

                if (result.EndOfMessage)
                {
                    if (accumulator is null)
                    {
                        return new(result.MessageType, chunk.ToArray());
                    }

                    accumulator.Append(chunk);
                    return new(messageType ?? result.MessageType, accumulator.ToArray());
                }

                emptyFragments = chunk.Length == 0 ? emptyFragments + 1 : 0;
                if (emptyFragments > MaxConsecutiveEmptyFragments)
                {
                    throw await RejectAsync(
                        webSocket, WebSocketCloseStatus.PolicyViolation, "Too many empty fragments.",
                        "The WebSocket message has too many consecutive empty fragments.",
                        closeTimeout, cancellationToken).ConfigureAwait(false);
                }

                messageType ??= result.MessageType;
                (accumulator ??= new PooledMessageBuffer()).Append(chunk);
            }
        }
        catch (WebSocketException ex)
            when (endOnPrematureClose
                  && !firstReadDone
                  && ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely
                  && (closeAlreadySent || webSocket.State == WebSocketState.CloseSent))
        {
            // We initiated the close and the peer dropped the connection instead of replying.
            return null;
        }
        finally
        {
            // A first read that failed never reached End(); do not leave the poll timer running.
            if (!firstReadDone)
            {
                watchdog?.StopPolling();
            }

            accumulator?.Dispose();
        }
    }

    /// <summary>
    /// Rejects a message that exceeds <paramref name="maxMessageSize"/>.
    /// </summary>
    /// <param name="webSocket">The socket to close and abort.</param>
    /// <param name="maxMessageSize">The limit that was exceeded.</param>
    /// <param name="closeTimeout">Bounds the best-effort close frame.</param>
    /// <param name="cancellationToken">The caller's token; it also stops the close frame.</param>
    /// <returns>The exception to throw.</returns>
    private static ValueTask<Exception> RejectOversizedAsync(
        WebSocket webSocket, int maxMessageSize, TimeSpan closeTimeout, CancellationToken cancellationToken)
        => RejectAsync(
            webSocket, WebSocketCloseStatus.MessageTooBig, "Message too big.",
            $"The WebSocket message exceeds the maximum allowed size of {maxMessageSize} bytes.",
            closeTimeout, cancellationToken);

    /// <summary>
    /// Sends a best-effort close frame, aborts the socket and builds the exception to throw.
    /// </summary>
    /// <param name="webSocket">The socket to close and abort.</param>
    /// <param name="status">The close status sent to the peer.</param>
    /// <param name="closeDescription">The close description sent to the peer.</param>
    /// <param name="errorMessage">The message of the returned exception.</param>
    /// <param name="closeTimeout">Bounds the best-effort close frame.</param>
    /// <param name="cancellationToken">The caller's token; it also stops the close frame.</param>
    /// <returns>
    /// An <see cref="InvalidDataException"/> carrying any close failure as inner exception, or an
    /// <see cref="OperationCanceledException"/> when the caller's token cancelled the close frame.
    /// </returns>
    private static async ValueTask<Exception> RejectAsync(
        WebSocket webSocket,
        WebSocketCloseStatus status,
        string closeDescription,
        string errorMessage,
        TimeSpan closeTimeout,
        CancellationToken cancellationToken)
    {
        // The remainder of the rejected message is still on the wire, so the socket cannot be
        // reused: tell the peer why (best effort, bounded by closeTimeout), then abort so no later
        // receive can mistake the remainder for the start of a new message. Whatever goes wrong while
        // closing is kept as the inner exception so the real cause (the rejection) is not masked.
        Exception? closeError = null;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(closeTimeout);
        try
        {
            await webSocket.CloseOutputAsync(status, closeDescription, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            closeError = ex;
        }
        finally
        {
            webSocket.Abort();
        }

        // The caller asked to stop: honour that instead of reporting the rejection.
        if (closeError is OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            return new RejectionCancelledException(cancellationToken);
        }

        return new InvalidDataException(errorMessage, closeError);
    }

    /// <summary>
    /// Marks the <see cref="OperationCanceledException"/> thrown when the caller's token cancelled the
    /// close frame of a rejected message, so the enumeration does not mistake it for a plain cancelled read.
    /// </summary>
    /// <param name="cancellationToken">The token that was cancelled.</param>
    private sealed class RejectionCancelledException(CancellationToken cancellationToken)
        : OperationCanceledException(cancellationToken);

    /// <summary>
    /// Validates the arguments shared by the public overloads, so errors surface at the call.
    /// </summary>
    /// <param name="webSocket">Must not be <see langword="null"/>.</param>
    /// <param name="bufferSize">Must be between 1 and 1 MiB.</param>
    /// <param name="maxMessageSize">Must be positive.</param>
    /// <param name="closeTimeout">Must be positive, at most about 49 days, or infinite.</param>
    private static void ValidateArguments(
        WebSocket webSocket, int bufferSize, int maxMessageSize, TimeSpan closeTimeout)
    {
        ArgumentNullException.ThrowIfNull(webSocket);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bufferSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(bufferSize, MaxBufferSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessageSize);
        if (closeTimeout != Timeout.InfiniteTimeSpan)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(closeTimeout, TimeSpan.Zero);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(closeTimeout, MaxCloseTimeout);
        }
    }
}
