using System.Net.WebSockets;
using System.Text;

namespace WebSocketFlow.Tests;

public class WebSocketFlowTests
{
    // --- ReceiveMessageAsync ---

    [Fact]
    public async Task ReadAllMessagesAsync_CancellationToken_StopsEnumeration()
    {
        var ws = new FakeWebSocket();
        using var cts = new CancellationTokenSource();

        // Enqueue enough messages; cancel after the first yield
        ws.Enqueue("a"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("b"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("c"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: cts.Token))
        {
            count++;
            if (count == 1)
                await cts.CancelAsync();
        }

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ReadAllMessagesAsync_FragmentedMessages_AssemblesEach()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("fo"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("o"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("ba"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("r"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        var messages = new List<string>();
        await foreach (var msg in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            messages.Add(Encoding.UTF8.GetString(msg.Data.Span));

        Assert.Equal(["foo", "bar"], messages);
    }

    [Fact]
    public async Task ReadAllMessagesAsync_NullWebSocket_ThrowsArgumentNullException()
    {
        WebSocket ws = null!;
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });
    }

    [Fact]
    public async Task ReadAllMessagesAsync_StopsOnCloseFrame()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("msg"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();
        // This would throw "no more fragments" if the enumerator reads past the close
        ws.Enqueue("after-close"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ReadAllMessagesAsync_WebSocketException_StopsEnumeration()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("msg"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueException(new WebSocketException("connection reset"));

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ReadAllMessagesAsync_YieldsAllMessages()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("one"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("two"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("three"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        var messages = new List<string>();
        await foreach (var msg in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            messages.Add(Encoding.UTF8.GetString(msg.Data.Span));

        Assert.Equal(["one", "two", "three"], messages);
    }

    [Fact]
    public async Task ReceiveMessageAsync_BinaryMessage_PreservesBytes()
    {
        var ws = new FakeWebSocket();
        var payload = new byte[] { 0x00, 0xFF, 0x7F, 0x80 };
        ws.Enqueue(payload, WebSocketMessageType.Binary, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketMessageType.Binary, msg.MessageType);
        Assert.Equal(payload, msg.Data.ToArray());
    }

    [Fact]
    public async Task ReceiveMessageAsync_BufferSizeZero_ThrowsArgumentOutOfRangeException()
    {
        var ws = new FakeWebSocket();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ws.ReceiveMessageAsync(bufferSize: 0, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReceiveMessageAsync_CloseFrame_SetsCloseReceived()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueClose();

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.True(msg.CloseReceived);
    }

    [Fact]
    public async Task ReceiveMessageAsync_EmptyPayload_ReturnsEmptyMessage()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, msg.Data.Length);
    }

    [Fact]
    public async Task ReceiveMessageAsync_FragmentSmallerThanBuffer_Works()
    {
        // Use a very small buffer (2 bytes) to force many reads
        var ws = new FakeWebSocket();
        ws.Enqueue("AB"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("CD"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(bufferSize: 2, TestContext.Current.CancellationToken);

        Assert.Equal("ABCD", Encoding.UTF8.GetString(msg.Data.Span));
    }

    [Fact]
    public async Task ReceiveMessageAsync_MultipleFragments_AssemblesCorrectly()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("hel"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("lo"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("!"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.Equal("hello!", Encoding.UTF8.GetString(msg.Data.Span));
    }

    [Fact]
    public async Task ReceiveMessageAsync_NegativeBufferSize_ThrowsArgumentOutOfRangeException()
    {
        var ws = new FakeWebSocket();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ws.ReceiveMessageAsync(bufferSize: -1, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReceiveMessageAsync_NullWebSocket_ThrowsArgumentNullException()
    {
        WebSocket ws = null!;
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ws.ReceiveMessageAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReceiveMessageAsync_SingleFragment_ReturnsCompleteMessage()
    {
        var ws = new FakeWebSocket();
        var payload = "hello"u8.ToArray();
        ws.Enqueue(payload, WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketMessageType.Text, msg.MessageType);
        Assert.Equal(payload, msg.Data.ToArray());
    }
}