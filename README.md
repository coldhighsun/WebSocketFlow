# WebSocketFlow

[![CI](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![NuGet Downloads](https://img.shields.io/nuget/dt/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**English** | [简体中文](https://github.com/coldhighsun/WebSocketFlow/blob/main/README.zh-CN.md)

Extension methods for `System.Net.WebSockets.WebSocket` that simplify receiving complete messages and asynchronously enumerating incoming messages.

---

## Features

- **`ReceiveMessageAsync`** — Receives a complete WebSocket message, automatically assembling all fragments into a single `WebSocketMessage`.
- **`ReadAllMessagesAsync`** — Returns an `IAsyncEnumerable<WebSocketMessage>` that yields complete messages until the socket closes or a cancellation token is triggered.

## Target Frameworks

| Framework | Supported |
|---|---|
| .NET 10.0+ | ✓ |

## Installation

```
dotnet add package WebSocketFlow
```

## Usage

### Receive a single message

```csharp
using WebSocketFlow;

// Default buffer size (4096 bytes)
WebSocketMessage message = await webSocket.ReceiveMessageAsync(cancellationToken);

// Custom buffer size
WebSocketMessage message = await webSocket.ReceiveMessageAsync(bufferSize: 8192, cancellationToken);

Console.WriteLine(Encoding.UTF8.GetString(message.Data.Span));
```

### Enumerate all messages

```csharp
using WebSocketFlow;

await foreach (var msg in webSocket.ReadAllMessagesAsync(cancellationToken: cancellationToken))
{
    Console.WriteLine(Encoding.UTF8.GetString(msg.Data.Span));
}

// Custom buffer size
await foreach (var msg in webSocket.ReadAllMessagesAsync(bufferSize: 8192, cancellationToken))
{
    Console.WriteLine(Encoding.UTF8.GetString(msg.Data.Span));
}
```

Enumeration stops automatically when a close frame is received, the cancellation token is triggered, the socket is aborted, or the remote side drops the connection (or stays silent for 5 seconds, configurable, without starting to send) after you already sent your own close frame. Any other `WebSocketException`, including a premature disconnect, propagates to the caller. The library does not complete the closing handshake; call `CloseOutputAsync`/`CloseAsync` yourself. After a silent-peer timeout the closing handshake did not complete, so call `Abort()` if you do not need it to; the timeout is accurate to within one second.

### Limits

Messages are limited to 16 MiB by default. A larger message sends a `MessageTooBig` close frame (best effort, 5 s timeout by default), aborts the socket and throws `InvalidDataException`; pass `maxMessageSize` to change the limit. `bufferSize` must be between 1 byte and 1 MiB, and more than 1024 consecutive empty non-final fragments are rejected the same way (with a `PolicyViolation` close frame).

```csharp
var message = await webSocket.ReceiveMessageAsync(bufferSize: 8192, maxMessageSize: 1_000_000, cancellationToken);
await foreach (var msg in webSocket.ReadAllMessagesAsync(8192, 1_000_000, cancellationToken)) { }

// Optional close timeout (default 5 s; Timeout.InfiniteTimeSpan disables it)
await foreach (var msg in webSocket.ReadAllMessagesAsync(8192, 1_000_000, TimeSpan.FromSeconds(10), cancellationToken)) { }
```

### `WebSocketMessage`

| Member | Type | Description |
|---|---|---|
| `MessageType` | `WebSocketMessageType` | Text, Binary, or Close |
| `Data` | `ReadOnlyMemory<byte>` | Fully assembled payload |
| `CloseReceived` | `bool` | `true` when `MessageType == Close` |
| `GetString()` | `string` | Decodes the payload as UTF-8 |

