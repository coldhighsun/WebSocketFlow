# Changelog

## Unreleased

### Breaking changes

- Target framework is now `net10.0` only; `netstandard2.0` and `net8.0` are no longer supported.
- Assembled messages are limited to 16 MiB by default. Larger messages send a best-effort `MessageTooBig` close frame (bounded by a 5 second timeout), abort the socket and throw `InvalidDataException` (a failure while sending the close frame is attached as `InnerException`). Use the new `maxMessageSize` overloads to change the limit.
- `ReadAllMessagesAsync` only ends quietly on a close frame, cancellation, or abort. `WebSocketException`s, including `WebSocketError.ConnectionClosedPrematurely`, now propagate (except a premature disconnect after you already sent your own close frame, which ends the enumeration).
- `bufferSize` must be between 1 and 1 MiB. More than 1024 consecutive empty non-final fragments send a `PolicyViolation` close frame, abort the socket and throw `InvalidDataException`.

- Debug symbols are now embedded in the assembly in every configuration (`DebugType=embedded`); no separate pdb/snupkg is shipped.

- `ReadAllMessagesAsync` and `ReceiveMessageAsync` validate their arguments when called instead of on first enumeration/await.

### Changes

- When the caller's token is cancelled while the rejection close frame is being sent, `OperationCanceledException` is thrown instead of `InvalidDataException` (the socket is still aborted).
- A close frame that arrives between the fragments of a data message is reported as a close (`CloseReceived`); the partial payload is discarded instead of being delivered as a truncated message.
- After you sent your own close frame, a peer that drops the connection in the middle of a message no longer ends `ReadAllMessagesAsync` quietly; the `ConnectionClosedPrematurely` exception propagates.
- `maxMessageSize` values above `Array.MaxLength` are enforced as `Array.MaxLength`, so an oversized message is rejected (close frame, abort, `InvalidDataException`) instead of failing with `OutOfMemoryException`.
- New `closeTimeout` overloads of `ReceiveMessageAsync` and `ReadAllMessagesAsync` configure the 5 second close-frame / close-handshake bound (`Timeout.InfiniteTimeSpan` disables it).
- `ReadAllMessagesAsync` ends quietly when the socket is aborted from another thread (including `ObjectDisposedException`/`InvalidState` races), and, after you sent your own close frame, stops waiting if the peer has not started to reply within 5 seconds (the timer only covers the wait for the first bytes, not a message in flight; it also applies when the close frame was sent by another thread during a pending receive).
- The receive buffer is capped at `maxMessageSize`.
- `bufferSize` is now honored exactly instead of being rounded up to the pool's array size.
- `ReadAllMessagesAsync` keeps reading while the socket is in `CloseSent`, so the remote close frame is received.
- `WebSocketMessage.GetString()` decodes the payload as UTF-8.
- Fragment assembly uses a single growing pooled buffer, one receive buffer is reused across `ReadAllMessagesAsync`, and used pooled bytes are cleared before being returned.
