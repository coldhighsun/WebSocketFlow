using System.Net.WebSockets;
using System.Text;

namespace WebSocketFlow.Tests;

/// <summary>
/// Tests for <see cref="WebSocketFlow"/>.
/// </summary>
public class WebSocketFlowTests
{
    // --- ReceiveMessageAsync ---

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: cancellation token. Expected: stops enumeration.
    /// </summary>
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
            {
                    await cts.CancelAsync();
            }
        }

        Assert.Equal(1, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: fragmented messages. Expected: assembles each.
    /// </summary>
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

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: null web socket. Expected: throws argument null exception.
    /// </summary>
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

    /// <summary>
    /// Tests ReadAllMessagesAsync. Read all messages async stops on close frame.
    /// </summary>
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

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: connection closed prematurely. Expected: propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ConnectionClosedPrematurely_Propagates()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("msg"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        var count = 0;
        var ex = await Assert.ThrowsAsync<WebSocketException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
                count++;
        });

        Assert.Equal(WebSocketError.ConnectionClosedPrematurely, ex.WebSocketErrorCode);
        Assert.Equal(1, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: connection closed prematurely. Expected: after close sent ends quietly.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ConnectionClosedPrematurely_AfterCloseSent_EndsQuietly()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.Enqueue("msg"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(1, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: connection drops mid-message after close sent. Expected: propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ConnectionClosedPrematurely_MidMessageAfterCloseSent_Propagates()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.Enqueue("par"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        var ex = await Assert.ThrowsAsync<WebSocketException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });

        Assert.Equal(WebSocketError.ConnectionClosedPrematurely, ex.WebSocketErrorCode);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: close frame between fragments. Expected: returns the close, not partial data.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_CloseFrameBetweenFragments_ReturnsCloseNotPartialData()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("par"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.EnqueueClose();

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.True(msg.CloseReceived);
        Assert.Empty(msg.Data.ToArray());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close frame between fragments. Expected: ends without yielding partial data.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseFrameBetweenFragments_EndsWithoutPartialMessage()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("par"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.EnqueueClose();

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
        {
            count++;
        }

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests EffectiveMaxMessageSize. Scenario: values around the array limit. Expected: capped at Array.MaxLength.
    /// </summary>
    [Fact]
    public void EffectiveMaxMessageSize_AboveArrayLimit_IsCappedAtArrayMaxLength()
    {
        Assert.Equal(Array.MaxLength, WebSocketFlow.EffectiveMaxMessageSize(int.MaxValue));
        Assert.Equal(Array.MaxLength, WebSocketFlow.EffectiveMaxMessageSize(Array.MaxLength));
        Assert.Equal(1024, WebSocketFlow.EffectiveMaxMessageSize(1024));
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: max message size above the array limit. Expected: a small message is still received.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_MaxMessageSizeAboveArrayLimit_StillReceivesSmallMessage()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(16, int.MaxValue, TestContext.Current.CancellationToken);

        Assert.Equal("abc", msg.GetString());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close sent while receive pending. Expected: premature close ends quietly.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseSentWhileReceivePending_PrematureCloseEndsQuietly()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("msg"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        // Another task sends the close frame while the second receive is pending.
        var receives = 0;
        ws.OnReceive = () =>
        {
            if (++receives == 2)
            {
                    ws.StateOverride = WebSocketState.CloseSent;
            }
        };

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(1, count);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Receive message async leading empty fragment counts toward limit.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_LeadingEmptyFragmentCountsTowardLimit()
    {
        var ws = new FakeWebSocket();
        for (var i = 0; i < 1024; i++)
        {
                ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: false);
        }
        ws.Enqueue("z"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        // 1024 empty fragments in total (the first handled before the loop) are still tolerated.
        var message = await ws.ReceiveMessageAsync(16, TestContext.Current.CancellationToken);
        Assert.Equal(1, message.Data.Length);

        for (var i = 0; i < 1025; i++)
        {
                ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: false);
        }
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: too many empty fragments. Expected: throws and aborts.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_TooManyEmptyFragments_ThrowsAndAborts()
    {
        var ws = new FakeWebSocket();
        for (var i = 0; i < 1100; i++)
        {
                ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: false);
        }

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, ws.LastCloseOutputStatus);
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: empty fragments interleaved with data. Expected: are not consecutive.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_EmptyFragmentsInterleavedWithData_AreNotConsecutive()
    {
        var ws = new FakeWebSocket();
        for (var i = 0; i < 1500; i++)
        {
            ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: false);
            ws.Enqueue("a"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        }
        ws.Enqueue("z"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var message = await ws.ReceiveMessageAsync(16, TestContext.Current.CancellationToken);

        Assert.Equal(1501, message.Data.Length);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: buffer size out of range. Expected: throws eagerly.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1024 * 1024 + 1)]
    public void ReadAllMessagesAsync_BufferSizeOutOfRange_ThrowsEagerly(int bufferSize)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new FakeWebSocket().ReadAllMessagesAsync(bufferSize, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too many empty fragments. Expected: throws and aborts.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooManyEmptyFragments_ThrowsAndAborts()
    {
        var ws = new FakeWebSocket();
        for (var i = 0; i < 1100; i++)
        {
                ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: false);
        }

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, ws.LastCloseOutputStatus);
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: buffer size out of range. Expected: throws eagerly.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1024 * 1024 + 1)]
    public void ReceiveMessageAsync_BufferSizeOutOfRange_ThrowsEagerly(int bufferSize)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new FakeWebSocket().ReceiveMessageAsync(bufferSize, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Read all messages async yields all messages.
    /// </summary>
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

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: binary message. Expected: preserves bytes.
    /// </summary>
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

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: buffer size zero. Expected: throws argument out of range exception.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_BufferSizeZero_ThrowsArgumentOutOfRangeException()
    {
        var ws = new FakeWebSocket();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ws.ReceiveMessageAsync(bufferSize: 0, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: close frame. Expected: sets close received.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_CloseFrame_SetsCloseReceived()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueClose();

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.True(msg.CloseReceived);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: empty payload. Expected: returns empty message.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_EmptyPayload_ReturnsEmptyMessage()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue([], WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, msg.Data.Length);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: fragment smaller than buffer. Expected: works.
    /// </summary>
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

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: multiple fragments. Expected: assembles correctly.
    /// </summary>
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

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: negative buffer size. Expected: throws argument out of range exception.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_NegativeBufferSize_ThrowsArgumentOutOfRangeException()
    {
        var ws = new FakeWebSocket();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ws.ReceiveMessageAsync(bufferSize: -1, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: null web socket. Expected: throws argument null exception.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_NullWebSocket_ThrowsArgumentNullException()
    {
        WebSocket ws = null!;
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ws.ReceiveMessageAsync(TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: single fragment. Expected: returns complete message.
    /// </summary>
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

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: other web socket exception. Expected: propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_OtherWebSocketException_Propagates()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new WebSocketException(WebSocketError.InvalidState));

        await Assert.ThrowsAsync<WebSocketException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: socket aborted during receive. Expected: stops enumeration.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_SocketAbortedDuringReceive_StopsEnumeration()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new OperationCanceledException());

        var count = 0;
        // The state only becomes Aborted once the receive is under way.
        ws.OnReceive = () => ws.StateOverride = WebSocketState.Aborted;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close sent state. Expected: still reads messages.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseSentState_StillReadsMessages()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.Enqueue("x"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(1, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: closed socket. Expected: yields nothing.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ClosedSocket_YieldsNothing()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.Closed };

        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            Assert.Fail("Should not yield.");
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: invalid arguments. Expected: throw eagerly.
    /// </summary>
    [Fact]
    public void ReadAllMessagesAsync_InvalidArguments_ThrowEagerly()
    {
        Assert.Throws<ArgumentNullException>(() => ((WebSocket)null!).ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FakeWebSocket().ReadAllMessagesAsync(bufferSize: 0, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FakeWebSocket().ReadAllMessagesAsync(16, 0, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: message too large. Expected: throws.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_MessageTooLarge_Throws()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(16, 2, TestContext.Current.CancellationToken))
            {
            }
        });

        Assert.Equal(WebSocketCloseStatus.MessageTooBig, ws.LastCloseOutputStatus);
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: buffer size. Expected: is honored and large fragments are split.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_BufferSize_IsHonoredAndLargeFragmentsAreSplit()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("0123456789"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(bufferSize: 4, TestContext.Current.CancellationToken);

        Assert.Equal(4, ws.LastBufferLength);
        Assert.Equal("0123456789", msg.GetString());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: fragment larger than buffer. Expected: is reassembled.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_FragmentLargerThanBuffer_IsReassembled()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("0123456789"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.Enqueue("abcdefghij"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        var messages = new List<string>();
        await foreach (var msg in ws.ReadAllMessagesAsync(bufferSize: 3, cancellationToken: TestContext.Current.CancellationToken))
            messages.Add(msg.GetString());

        Assert.Equal(["0123456789", "abcdefghij"], messages);
        Assert.Equal(3, ws.LastBufferLength);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: invalid arguments. Expected: throw synchronously.
    /// </summary>
    [Fact]
    public void ReceiveMessageAsync_InvalidArguments_ThrowSynchronously()
    {
        Assert.Throws<ArgumentNullException>(() => ((WebSocket)null!).ReceiveMessageAsync(TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FakeWebSocket().ReceiveMessageAsync(0, TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FakeWebSocket().ReceiveMessageAsync(16, 0, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: single fragment too large. Expected: throws.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_SingleFragmentTooLarge_Throws()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 2, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: fragments exceed limit. Expected: throws.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_FragmentsExceedLimit_Throws()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("def"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 5, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: fragments exactly at limit. Expected: succeeds.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_FragmentsExactlyAtLimit_Succeeds()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("def"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(16, 6, TestContext.Current.CancellationToken);

        Assert.Equal("abcdef", msg.GetString());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: max message size zero. Expected: throws argument out of range exception.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_MaxMessageSizeZero_ThrowsArgumentOutOfRangeException()
    {
        var ws = new FakeWebSocket();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ws.ReceiveMessageAsync(16, 0, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: many fragments. Expected: grows buffer and preserves order.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_ManyFragments_GrowsBufferAndPreservesOrder()
    {
        var ws = new FakeWebSocket();
        var expected = new StringBuilder();
        for (var i = 0; i < 2000; i++)
        {
            var chunk = $"{i:D4}";
            expected.Append(chunk);
            ws.Enqueue(Encoding.UTF8.GetBytes(chunk), WebSocketMessageType.Text, endOfMessage: i == 1999);
        }

        var msg = await ws.ReceiveMessageAsync(bufferSize: 4, TestContext.Current.CancellationToken);

        Assert.Equal(expected.ToString(), msg.GetString());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too large. Expected: sends message too big and aborts socket.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooLarge_SendsMessageTooBigAndAbortsSocket()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("def"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 5, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(WebSocketCloseStatus.MessageTooBig, ws.LastCloseOutputStatus);
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests DefaultMaxMessageSize. Scenario: constant. Expected: is16 mi b.
    /// </summary>
    [Fact]
    public void DefaultMaxMessageSize_Constant_Is16MiB()
    {
        Assert.Equal(16 * 1024 * 1024, global::WebSocketFlow.WebSocketFlow.DefaultMaxMessageSize);
    }

    /// <summary>
    /// Tests WebSocketMessage. Scenario: get string. Expected: decodes utf8.
    /// </summary>
    [Fact]
    public void WebSocketMessage_GetString_DecodesUtf8()
    {
        var msg = new WebSocketMessage(WebSocketMessageType.Text, Encoding.UTF8.GetBytes("héllo 你好"));

        Assert.Equal("héllo 你好", msg.GetString());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too large. Expected: close frame fails still throws invalid data and aborts.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooLarge_CloseFrameFails_StillThrowsInvalidDataAndAborts()
    {
        var ws = new FakeWebSocket
        {
            OnCloseOutput = _ => Task.FromException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely)),
        };
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 2, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too large. Expected: peer not reading close times out and aborts.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooLarge_PeerNotReading_CloseTimesOutAndAborts()
    {
        var ws = new FakeWebSocket
        {
            // Behaves like a send that never completes because the peer stopped reading.
            OnCloseOutput = ct => Task.Delay(Timeout.Infinite, ct),
        };
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() =>
            global::WebSocketFlow.WebSocketFlow.ReceiveMessageCoreAsync(
                ws, 16, 2, TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken).AsTask())
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);

        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too large. Expected: caller cancelled throws operation cancelled and aborts.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooLarge_CallerCancelled_ThrowsOperationCanceledAndAborts()
    {
        var ws = new FakeWebSocket
        {
            OnCloseOutput = ct => Task.Delay(Timeout.Infinite, ct),
        };
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        using var cts = new CancellationTokenSource();
        ws.OnReceive = () => cts.Cancel();

        // The receive itself completes (fake ignores the token); the close frame then sees a cancelled token.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ws.ReceiveMessageAsync(16, 2, cts.Token).AsTask());

        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: too large. Expected: unexpected close error does not mask oversize failure.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_TooLarge_UnexpectedCloseError_DoesNotMaskOversizeFailure()
    {
        var ws = new FakeWebSocket
        {
            OnCloseOutput = _ => Task.FromException(new InvalidOperationException("bug")),
        };
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 2, TestContext.Current.CancellationToken).AsTask());

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: receive fails mid message. Expected: exception propagates.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_ReceiveFailsMidMessage_ExceptionPropagates()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

        await Assert.ThrowsAsync<WebSocketException>(() =>
            ws.ReceiveMessageAsync(16, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close sent and peer silent. Expected: ends after handshake timeout.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseSentAndPeerSilent_EndsAfterHandshakeTimeout()
    {
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.BeforeReceive = ct => ElapseSilentPeerTimeout(clock, ct);

        var count = 0;
        await foreach (var _ in ReadAllWithClock(ws, clock))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: aborted by another thread. Expected: ends quietly.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReadAllMessagesAsync_AbortedByAnotherThread_EndsQuietly(bool objectDisposed)
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(objectDisposed
            ? new ObjectDisposedException(nameof(WebSocket))
            : new WebSocketException(WebSocketError.InvalidState));
        ws.OnReceive = () => ws.StateOverride = WebSocketState.Aborted;

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: socket disposed by another thread during a receive. Expected: ends quietly.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_DisposedByAnotherThread_EndsQuietly()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new ObjectDisposedException(nameof(WebSocket)));
        ws.OnReceive = () => ws.StateOverride = WebSocketState.Closed;

        var count = 0;
        await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: object disposed while the socket is still open. Expected: propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ObjectDisposedWhileOpen_Propagates()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new ObjectDisposedException(nameof(WebSocket)));

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: premature disconnect leaving aborted state. Expected: still propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_PrematureDisconnectLeavingAbortedState_StillPropagates()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new WebSocketException(WebSocketError.ConnectionClosedPrematurely));
        ws.OnReceive = () => ws.StateOverride = WebSocketState.Aborted;

        await Assert.ThrowsAsync<WebSocketException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Receive message async buffer is capped by max message size.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_BufferIsCappedByMaxMessageSize()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(bufferSize: 1024, maxMessageSize: 3, TestContext.Current.CancellationToken);

        Assert.Equal(3, ws.LastBufferLength);
        Assert.Equal("abc", msg.GetString());
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: capped buffer. Expected: still rejects oversized message.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_CappedBuffer_StillRejectsOversizedMessage()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abcd"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await ws.ReceiveMessageAsync(bufferSize: 1024, maxMessageSize: 3, TestContext.Current.CancellationToken));
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close sent. Expected: slow message in flight is not truncated by handshake timeout.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseSent_SlowMessageInFlight_IsNotTruncatedByHandshakeTimeout()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.Enqueue("ab"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: false);
        ws.Enqueue("cd"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);
        ws.EnqueueClose();

        // The reply starts at once but its second fragment arrives long after the timeout: a poll tick
        // that fires then must not cancel the read.
        var clock = new ManualTimeProvider();
        var receives = 0;
        ws.BeforeReceive = ct =>
        {
            if (++receives == 1)
            {
                clock.Tick();
            }
            else if (receives == 2)
            {
                clock.Advance(HandshakeTimeout * 4);
                clock.Tick();
            }

            return Task.CompletedTask;
        };

        var messages = new List<string>();
        await foreach (var msg in ReadAllWithClock(ws, clock))
            messages.Add(msg.GetString());

        Assert.Equal(["abcd"], messages);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: concurrent receive misuse. Expected: while aborted propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_ConcurrentReceiveMisuse_WhileAborted_Propagates()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new InvalidOperationException("already one outstanding ReceiveAsync"));
        ws.OnReceive = () => ws.StateOverride = WebSocketState.Aborted;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(cancellationToken: TestContext.Current.CancellationToken))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: close timeout zero or negative. Expected: throws eagerly.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void ReceiveMessageAsync_CloseTimeoutZeroOrNegative_ThrowsEagerly(int milliseconds)
    {
        var ws = new FakeWebSocket();
        var timeout = TimeSpan.FromMilliseconds(milliseconds);

        Assert.Throws<ArgumentOutOfRangeException>(() => ws.ReceiveMessageAsync(16, 16, timeout, TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => ws.ReadAllMessagesAsync(16, 16, timeout, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: close timeout too long. Expected: throws eagerly.
    /// </summary>
    [Fact]
    public void ReceiveMessageAsync_CloseTimeoutTooLong_ThrowsEagerly()
    {
        var ws = new FakeWebSocket();

        Assert.Throws<ArgumentOutOfRangeException>(() => ws.ReceiveMessageAsync(16, 16, TimeSpan.FromDays(60), TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentOutOfRangeException>(() => ws.ReadAllMessagesAsync(16, 16, TimeSpan.FromDays(60), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: close timeout overload. Expected: bounds close frame.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_CloseTimeoutOverload_BoundsCloseFrame()
    {
        var ws = new FakeWebSocket { OnCloseOutput = ct => Task.Delay(Timeout.Infinite, ct) };
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() =>
            ws.ReceiveMessageAsync(16, 2, TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken).AsTask())
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.IsAssignableFrom<OperationCanceledException>(ex.InnerException);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close timeout overload. Expected: ends when peer silent after close sent.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseTimeoutOverload_EndsWhenPeerSilentAfterCloseSent()
    {
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        ws.BeforeReceive = ct => ElapseSilentPeerTimeout(clock, ct);

        var count = 0;
        await foreach (var _ in ReadAllWithClock(ws, clock))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReceiveMessageAsync. Scenario: infinite close timeout. Expected: is accepted.
    /// </summary>
    [Fact]
    public async Task ReceiveMessageAsync_InfiniteCloseTimeout_IsAccepted()
    {
        var ws = new FakeWebSocket();
        ws.Enqueue("abc"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        var msg = await ws.ReceiveMessageAsync(16, 16, Timeout.InfiniteTimeSpan, TestContext.Current.CancellationToken);

        Assert.Equal("abc", msg.GetString());
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: close sent while receive pending. Expected: silent peer ends after timeout.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_CloseSentWhileReceivePending_SilentPeer_EndsAfterTimeout()
    {
        // Another task sends the close frame while the first receive is already waiting.
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket();
        ws.OnReceive = () => ws.StateOverride = WebSocketState.CloseSent;
        ws.BeforeReceive = ct => ElapseSilentPeerTimeout(clock, ct);

        var count = 0;
        await foreach (var _ in ReadAllWithClock(ws, clock))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: infinite close timeout. Expected: waits for peer until cancelled.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_InfiniteCloseTimeout_WaitsForPeerUntilCancelled()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent, BlockWhenEmpty = true };
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        var receiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ws.OnReceive = () => receiveStarted.TrySetResult();

        var reading = Task.Run(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(16, 1024, Timeout.InfiniteTimeSpan, cts.Token))
            {
            }
        }, TestContext.Current.CancellationToken);

        await receiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.False(reading.IsCompleted);

        await cts.CancelAsync();
        await reading.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: silent peer timeout. Expected: transport throws web socket exception still ends quietly.
    /// </summary>
    [Theory]
    [InlineData(WebSocketError.InvalidState)]
    [InlineData(WebSocketError.ConnectionClosedPrematurely)]
    public async Task ReadAllMessagesAsync_SilentPeerTimeout_TransportThrowsWebSocketException_StillEndsQuietly(
        WebSocketError error)
    {
        // A transport that reports the cancelled read as a WebSocketException instead of an OCE.
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket
        {
            BeforeReceive = async ct =>
            {
                try
                {
                    await ElapseSilentPeerTimeout(clock, ct);
                }
                catch (OperationCanceledException)
                {
                    throw new WebSocketException(error);
                }
            },
        };
        ws.StateOverride = WebSocketState.CloseSent;

        var count = 0;
        await foreach (var _ in ReadAllWithClock(ws, clock))
            count++;

        Assert.Equal(0, count);
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: the silent-peer timeout cancels the read but the transport throws a WebSocketException with an unrelated code. Expected: the exception propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_SilentPeerTimeout_TransportThrowsUnrelatedWebSocketException_Propagates()
    {
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket
        {
            BeforeReceive = async ct =>
            {
                try
                {
                    await ElapseSilentPeerTimeout(clock, ct);
                }
                catch (OperationCanceledException)
                {
                    throw new WebSocketException(WebSocketError.Faulted);
                }
            },
        };
        ws.StateOverride = WebSocketState.CloseSent;

        await Assert.ThrowsAsync<WebSocketException>(async () =>
        {
            await foreach (var _ in ReadAllWithClock(ws, clock))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: the silent-peer timeout cancels the read but the transport throws an unrelated exception. Expected: the exception propagates.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_SilentPeerTimeout_TransportThrowsIOException_Propagates()
    {
        var clock = new ManualTimeProvider();
        var ws = new FakeWebSocket
        {
            BeforeReceive = async ct =>
            {
                try
                {
                    await ElapseSilentPeerTimeout(clock, ct);
                }
                catch (OperationCanceledException)
                {
                    throw new IOException("transport failure");
                }
            },
        };
        ws.StateOverride = WebSocketState.CloseSent;

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var _ in ReadAllWithClock(ws, clock))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: the first read fails before any bytes arrive. Expected: the failure propagates and the enumeration can be disposed.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_FirstReadFails_PropagatesAndStopsPolling()
    {
        var ws = new FakeWebSocket();
        ws.EnqueueException(new InvalidOperationException("boom"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(16, 1024, TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken))
            {
            }
        });
    }

    /// <summary>
    /// Tests ReadAllMessagesAsync. Scenario: the caller cancels while the rejection close frame is sent. Expected: the cancellation propagates instead of ending quietly.
    /// </summary>
    [Fact]
    public async Task ReadAllMessagesAsync_OversizedMessage_CallerCancelsDuringCloseFrame_Propagates()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var ws = new FakeWebSocket
        {
            OnCloseOutput = async ct =>
            {
                await cts.CancelAsync();
                await Task.Delay(Timeout.Infinite, ct);
            },
        };
        ws.Enqueue("abcd"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in ws.ReadAllMessagesAsync(16, 3, cts.Token))
            {
            }
        });
        Assert.Equal(WebSocketState.Aborted, ws.State);
    }

    /// <summary>
    /// The silent-peer timeout used by the clock-driven tests.
    /// </summary>
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Enumerates messages with a clock the test drives by hand instead of waiting in real time.
    /// </summary>
    /// <param name="ws">The socket to read from.</param>
    /// <param name="clock">The clock behind the silent-peer timeout.</param>
    /// <returns>The messages received until the stream ends.</returns>
    private static IAsyncEnumerable<WebSocketMessage> ReadAllWithClock(FakeWebSocket ws, ManualTimeProvider clock)
        => global::WebSocketFlow.WebSocketFlow.ReadAllMessagesCoreAsync(
            ws, 16, 1024, HandshakeTimeout, clock, TestContext.Current.CancellationToken);

    /// <summary>
    /// Simulates a peer that stays silent: lets the poll notice CloseSent, moves the clock past the
    /// timeout and polls again, then waits until the resulting cancellation arrives.
    /// </summary>
    /// <param name="clock">The clock behind the silent-peer timeout.</param>
    /// <param name="cancellationToken">The token of the pending read.</param>
    /// <returns>A task that only ends by cancellation.</returns>
    private static Task ElapseSilentPeerTimeout(ManualTimeProvider clock, CancellationToken cancellationToken)
    {
        clock.Tick();
        clock.Advance(HandshakeTimeout);
        clock.Tick();
        return Task.Delay(Timeout.Infinite, cancellationToken);
    }
}
