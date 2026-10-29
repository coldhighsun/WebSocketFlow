using System.Buffers;

namespace WebSocketFlow.Tests;

/// <summary>
/// Captures the state of each array at the moment it is returned.
/// </summary>
internal sealed class RecordingPool : ArrayPool<byte>
{
    /// <summary>
    /// Gets a copy of the last array returned to the pool.
    /// </summary>
    public byte[]? Returned { get; private set; }

    /// <summary>
    /// Gets how many times an array was returned.
    /// </summary>
    public int ReturnCount { get; private set; }

    /// <inheritdoc />
    public override byte[] Rent(int minimumLength) => new byte[minimumLength];

    /// <inheritdoc />
    public override void Return(byte[] array, bool clearArray = false)
    {
        ReturnCount++;
        Returned = array.ToArray();
    }
}
