# WebSocketFlow

[![CI](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![NuGet Downloads](https://img.shields.io/nuget/dt/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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

---

---

# WebSocketFlow

[![CI](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![NuGet Downloads](https://img.shields.io/nuget/dt/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

对 `System.Net.WebSockets.WebSocket` 的扩展方法，简化完整消息的接收与异步枚举。

---

## 功能

- **`ReceiveMessageAsync`** — 接收一条完整的 WebSocket 消息，自动将所有分片合并为单个 `WebSocketMessage`。
- **`ReadAllMessagesAsync`** — 返回 `IAsyncEnumerable<WebSocketMessage>`，持续产出完整消息，直到连接关闭或取消令牌触发。

## 目标框架

| 框架 | 支持 |
|---|---|
| .NET 10.0+ | ✓ |

## 安装

```
dotnet add package WebSocketFlow
```

## 使用示例

### 接收单条消息

```csharp
using WebSocketFlow;

// 默认缓冲区大小（4096 字节）
WebSocketMessage message = await webSocket.ReceiveMessageAsync(cancellationToken);

// 自定义缓冲区大小
WebSocketMessage message = await webSocket.ReceiveMessageAsync(bufferSize: 8192, cancellationToken);

Console.WriteLine(Encoding.UTF8.GetString(message.Data.Span));
```

### 枚举所有消息

```csharp
using WebSocketFlow;

await foreach (var msg in webSocket.ReadAllMessagesAsync(cancellationToken: cancellationToken))
{
    Console.WriteLine(Encoding.UTF8.GetString(msg.Data.Span));
}

// 自定义缓冲区大小
await foreach (var msg in webSocket.ReadAllMessagesAsync(bufferSize: 8192, cancellationToken))
{
    Console.WriteLine(Encoding.UTF8.GetString(msg.Data.Span));
}
```

收到关闭帧、取消令牌触发、socket 被中止，或在你已发送关闭帧之后对端直接断开连接（或 5 秒内毫无响应，可配置）时，枚举自动停止；其他 `WebSocketException`（包括对端意外断开）会抛给调用方。库不会完成关闭握手，请自行调用 `CloseOutputAsync`/`CloseAsync`。因对端无响应而超时结束时，关闭握手并未完成，不需要时请调用 `Abort()`；超时精度约为 1 秒。

### 限制

默认单条消息上限为 16 MiB，超出时会先尽力发送 `MessageTooBig` 关闭帧（最长等待 5 秒），再中止 socket 并抛出 `InvalidDataException`；可通过 `maxMessageSize` 调整；`closeTimeout` 重载可调整 5 秒的关闭超时。`bufferSize` 必须在 1 字节到 1 MiB 之间；连续超过 1024 个空的非末尾分片同样会被拒绝（`PolicyViolation` 关闭帧）：

```csharp
var message = await webSocket.ReceiveMessageAsync(bufferSize: 8192, maxMessageSize: 1_000_000, cancellationToken);
await foreach (var msg in webSocket.ReadAllMessagesAsync(8192, 1_000_000, cancellationToken)) { }

// Optional close timeout (default 5 s; Timeout.InfiniteTimeSpan disables it)
await foreach (var msg in webSocket.ReadAllMessagesAsync(8192, 1_000_000, TimeSpan.FromSeconds(10), cancellationToken)) { }
```

### `WebSocketMessage`

| 成员 | 类型 | 说明 |
|---|---|---|
| `MessageType` | `WebSocketMessageType` | Text、Binary 或 Close |
| `Data` | `ReadOnlyMemory<byte>` | 完整合并后的消息内容 |
| `CloseReceived` | `bool` | `MessageType == Close` 时为 `true` |
| `GetString()` | `string` | 将内容按 UTF-8 解码 |
