# WebSocketFlow

[![CI](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/coldhighsun/WebSocketFlow/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)
[![NuGet Downloads](https://img.shields.io/nuget/dt/WebSocketFlow)](https://www.nuget.org/packages/WebSocketFlow)

Extension methods for `System.Net.WebSockets.WebSocket` that simplify receiving complete messages and asynchronously enumerating incoming messages.

---

## Features

- **`ReceiveMessageAsync`** — Receives a complete WebSocket message, automatically assembling all fragments into a single `WebSocketMessage`.
- **`ReadAllMessagesAsync`** — Returns an `IAsyncEnumerable<WebSocketMessage>` that yields complete messages until the socket closes or a cancellation token is triggered.

## Target Frameworks

| Framework | Supported |
|---|---|
| .NET Standard 2.0 | ✓ |
| .NET 8.0 | ✓ |
| .NET 9.0 | ✓ |
| .NET 10.0 | ✓ |

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

Enumeration stops automatically when a close frame is received or the cancellation token is triggered.

### `WebSocketMessage`

| Member | Type | Description |
|---|---|---|
| `MessageType` | `WebSocketMessageType` | Text, Binary, or Close |
| `Data` | `ReadOnlyMemory<byte>` | Fully assembled payload |
| `CloseReceived` | `bool` | `true` when `MessageType == Close` |

---

---

# WebSocketFlow

对 `System.Net.WebSockets.WebSocket` 的扩展方法，简化完整消息的接收与异步枚举。

---

## 功能

- **`ReceiveMessageAsync`** — 接收一条完整的 WebSocket 消息，自动将所有分片合并为单个 `WebSocketMessage`。
- **`ReadAllMessagesAsync`** — 返回 `IAsyncEnumerable<WebSocketMessage>`，持续产出完整消息，直到连接关闭或取消令牌触发。

## 目标框架

| 框架 | 支持 |
|---|---|
| .NET Standard 2.0 | ✓ |
| .NET 8.0 | ✓ |
| .NET 9.0 | ✓ |
| .NET 10.0 | ✓ |

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

收到关闭帧或取消令牌触发时，枚举自动停止。

### `WebSocketMessage`

| 成员 | 类型 | 说明 |
|---|---|---|
| `MessageType` | `WebSocketMessageType` | Text、Binary 或 Close |
| `Data` | `ReadOnlyMemory<byte>` | 完整合并后的消息内容 |
| `CloseReceived` | `bool` | `MessageType == Close` 时为 `true` |
