using System.Buffers;

namespace WebSocketFlow;

/// <summary>
/// A write-once, append-only buffer backed by pooled arrays.
/// </summary>
/// <remarks>
/// This type is intended for scenarios such as fragmented WebSocket message assembly,
/// where payload chunks are appended over time and materialized once via <see cref="ToArray"/>.
/// </remarks>
internal sealed class SegmentedBuffer : IDisposable
{
    /// <summary>
    /// Each segment is a tuple of (rented array, valid length) to support partial fills of rented arrays.
    /// </summary>
    private readonly List<(byte[] Array, int Length)> _segments = [];

    /// <summary>
    /// Tracks whether the buffer has been disposed to prevent double-returning rented arrays.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Tracks the total number of bytes appended across all segments for efficient allocation in <see cref="ToArray"/>.
    /// </summary>
    private int _totalLength;

    /// <summary>
    /// Gets the total number of bytes appended across all segments.
    /// </summary>
    /// <remarks>
    /// This value increases with each successful <see cref="Append(ReadOnlySpan{byte})"/> call
    /// and is not reset by <see cref="Dispose"/>.
    /// </remarks>
    public int TotalLength => _totalLength;

    /// <summary>
    /// Appends data to the buffer by renting an array from <see cref="ArrayPool{T}.Shared"/>
    /// and copying the provided span into it.
    /// </summary>
    /// <param name="data">The data to append. If empty, the call is a no-op.</param>
    /// <remarks>
    /// The appended data is copied into an internal rented array, so callers may safely reuse
    /// or modify the source buffer after this method returns.
    /// </remarks>
    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return;

        var rented = ArrayPool<byte>.Shared.Rent(data.Length);
        data.CopyTo(rented);
        _segments.Add((rented, data.Length));
        _totalLength += data.Length;
    }

    /// <summary>
    /// Returns all rented arrays to <see cref="ArrayPool{T}.Shared"/> and clears buffered segments.
    /// </summary>
    /// <remarks>
    /// This method is idempotent; subsequent calls are ignored.
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var (array, _) in _segments)
        {
            ArrayPool<byte>.Shared.Return(array);
        }

        _segments.Clear();
    }

    /// <summary>
    /// Copies all buffered segments into a single contiguous <see cref="byte"/> array.
    /// </summary>
    /// <returns>
    /// A newly allocated array containing the concatenated segment contents in append order.
    /// </returns>
    /// <remarks>
    /// This operation always allocates a new array of length <see cref="TotalLength"/>.
    /// </remarks>
    public byte[] ToArray()
    {
        var result = new byte[_totalLength];
        var offset = 0;
        foreach (var (array, length) in _segments)
        {
            array.AsSpan(0, length).CopyTo(result.AsSpan(offset));
            offset += length;
        }
        return result;
    }
}