namespace WebSocketFlow.Tests;

/// <summary>
/// Tests for <see cref="PooledMessageBuffer"/>.
/// </summary>
public class PooledMessageBufferTests
{
    /// <summary>
    /// Tests Append. Scenario: across growth. Expected: preserves order.
    /// </summary>
    [Fact]
    public void Append_AcrossGrowth_PreservesOrder()
    {
        using var buffer = new PooledMessageBuffer();
        var expected = new List<byte>();
        for (var i = 0; i < 5000; i++)
        {
            var chunk = new[] { (byte)i, (byte)(i >> 8), (byte)(i * 7) };
            expected.AddRange(chunk);
            buffer.Append(chunk);
        }

        Assert.Equal(expected.Count, buffer.Length);
        Assert.Equal(expected, buffer.ToArray());
    }

    /// <summary>
    /// Tests Append. Scenario: empty. Expected: is no op.
    /// </summary>
    [Fact]
    public void Append_Empty_IsNoOp()
    {
        using var buffer = new PooledMessageBuffer();
        buffer.Append([]);

        Assert.Equal(0, buffer.Length);
        Assert.Empty(buffer.ToArray());
    }

    /// <summary>
    /// Tests Dispose. Scenario: called twice. Expected: is idempotent.
    /// </summary>
    [Fact]
    public void Dispose_CalledTwice_IsIdempotent()
    {
        var buffer = new PooledMessageBuffer();
        buffer.Append([1, 2, 3]);

        buffer.Dispose();
        buffer.Dispose();
    }

    /// <summary>
    /// Tests ToArray. Scenario: after dispose. Expected: throws.
    /// </summary>
    [Fact]
    public void ToArray_AfterDispose_Throws()
    {
        var buffer = new PooledMessageBuffer();
        buffer.Append([1]);
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.ToArray());
    }

    /// <summary>
    /// Tests Append. Scenario: after dispose. Expected: throws.
    /// </summary>
    [Fact]
    public void Append_AfterDispose_Throws()
    {
        var buffer = new PooledMessageBuffer();
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.Append([1]));
    }

    /// <summary>
    /// Tests Dispose. Dispose wipes used bytes before returning to pool.
    /// </summary>
    [Fact]
    public void Dispose_WipesUsedBytesBeforeReturningToPool()
    {
        var pool = new RecordingPool();
        var buffer = new PooledMessageBuffer(pool);
        buffer.Append([0xAB, 0xAB, 0xAB]);

        buffer.Dispose();

        Assert.Equal(1, pool.ReturnCount);
        Assert.All(pool.Returned!.Take(3), b => Assert.Equal(0, b));
    }
}
