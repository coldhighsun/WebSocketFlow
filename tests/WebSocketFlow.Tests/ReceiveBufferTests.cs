namespace WebSocketFlow.Tests;

/// <summary>
/// Tests for <see cref="ReceiveBuffer"/>.
/// </summary>
public class ReceiveBufferTests
{
    /// <summary>
    /// Tests BeginReceive. Begin receive has exact requested length.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(1000)]
    [InlineData(4096)]
    public void BeginReceive_HasExactRequestedLength(int size)
    {
        using var buffer = new ReceiveBuffer(size);

        Assert.Equal(size, buffer.BeginReceive().Length);
    }

    /// <summary>
    /// Tests EndReceive. End receive returns written bytes.
    /// </summary>
    [Fact]
    public void EndReceive_ReturnsWrittenBytes()
    {
        using var buffer = new ReceiveBuffer(8);
        "abcd"u8.CopyTo(buffer.BeginReceive().Span);

        Assert.Equal("abcd"u8.ToArray(), buffer.EndReceive(4).ToArray());
    }

    /// <summary>
    /// Tests Dispose. Dispose wipes only received bytes before returning to pool.
    /// </summary>
    [Fact]
    public void Dispose_WipesOnlyReceivedBytesBeforeReturningToPool()
    {
        var pool = new RecordingPool();
        var buffer = new ReceiveBuffer(16, pool);
        buffer.BeginReceive().Span.Fill(0xAB);
        buffer.EndReceive(8);
        buffer.BeginReceive();
        buffer.EndReceive(4);

        buffer.Dispose();

        Assert.Equal(1, pool.ReturnCount);
        Assert.All(pool.Returned![..8], b => Assert.Equal(0, b));
        Assert.All(pool.Returned![8..], b => Assert.Equal(0xAB, b));
    }

    /// <summary>
    /// Tests Dispose. Scenario: after unfinished receive. Expected: wipes whole buffer and keeps it out of the pool.
    /// </summary>
    [Fact]
    public void Dispose_AfterUnfinishedReceive_WipesWholeBufferAndDoesNotReturnIt()
    {
        var pool = new RecordingPool();
        var buffer = new ReceiveBuffer(16, pool);
        var memory = buffer.BeginReceive();
        memory.Span.Fill(0xAB);

        buffer.Dispose();

        Assert.Equal(0, pool.ReturnCount);
        Assert.All(memory.ToArray(), b => Assert.Equal(0, b));
    }

    /// <summary>
    /// Tests Dispose. Dispose returns to pool only once.
    /// </summary>
    [Fact]
    public void Dispose_ReturnsToPoolOnlyOnce()
    {
        var pool = new RecordingPool();
        var buffer = new ReceiveBuffer(16, pool);

        buffer.Dispose();
        buffer.Dispose();

        Assert.Equal(1, pool.ReturnCount);
    }

    /// <summary>
    /// Tests BeginReceive. Scenario: after dispose. Expected: throws object disposed exception.
    /// </summary>
    [Fact]
    public void BeginReceive_AfterDispose_ThrowsObjectDisposedException()
    {
        var buffer = new ReceiveBuffer(16);
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.BeginReceive());
        Assert.Throws<ObjectDisposedException>(() => buffer.EndReceive(0));
    }
}
