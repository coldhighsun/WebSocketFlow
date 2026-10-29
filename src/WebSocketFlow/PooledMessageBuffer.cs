using System.Buffers;

namespace WebSocketFlow;

/// <summary>
/// An append-only buffer backed by a single pooled array that grows geometrically.
/// </summary>
/// <remarks>
/// This type is intended for fragmented WebSocket message assembly, where payload chunks are
/// appended over time and materialized once via <see cref="ToArray"/>. Used bytes are cleared
/// before the array is returned to the pool so message contents do not linger in shared memory.
/// </remarks>
internal sealed class PooledMessageBuffer : IDisposable
{
    /// <summary>
    /// Smallest array requested from the pool when the buffer first grows.
    /// </summary>
    private const int MinimumGrowth = 4096;

    /// <summary>
    /// The pool the array is rented from and returned to.
    /// </summary>
    private readonly ArrayPool<byte> _pool;

    /// <summary>
    /// The current rented array, or <see langword="null"/> before the first append and after disposal.
    /// </summary>
    private byte[]? _buffer;

    /// <summary>
    /// Whether <see cref="Dispose"/> has run.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledMessageBuffer"/> class.
    /// </summary>
    /// <param name="pool">The pool to rent from; defaults to <see cref="ArrayPool{T}.Shared"/>.</param>
    public PooledMessageBuffer(ArrayPool<byte>? pool = null)
    {
        _pool = pool ?? ArrayPool<byte>.Shared;
    }

    /// <summary>
    /// Gets the number of bytes appended so far.
    /// </summary>
    public int Length
    {
        get; private set;
    }

    /// <summary>
    /// Appends a copy of <paramref name="data"/> to the buffer.
    /// </summary>
    /// <param name="data">The bytes to append.</param>
    /// <exception cref="ObjectDisposedException">The buffer has been disposed.</exception>
    public void Append(ReadOnlySpan<byte> data)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (data.IsEmpty)
        {
            return;
        }

        EnsureCapacity(data.Length);
        data.CopyTo(_buffer.AsSpan(Length));
        Length += data.Length;
    }

    /// <summary>
    /// Returns the rented array to the pool. Subsequent calls are ignored.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_buffer is not null)
        {
            _pool.ReturnCleared(_buffer, Length);
        }

        _buffer = null;
    }

    /// <summary>
    /// Copies the buffered content into a newly allocated array of exactly <see cref="Length"/> bytes.
    /// </summary>
    /// <returns>A new array holding the appended bytes.</returns>
    /// <exception cref="ObjectDisposedException">The buffer has been disposed.</exception>
    public byte[] ToArray()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _buffer is null ? [] : _buffer.AsSpan(0, Length).ToArray();
    }

    /// <summary>
    /// Makes room for <paramref name="additional"/> more bytes, growing geometrically.
    /// </summary>
    /// <param name="additional">The number of bytes about to be appended.</param>
    /// <exception cref="OutOfMemoryException">The required size exceeds the maximum array length.</exception>
    private void EnsureCapacity(int additional)
    {
        var required = (long)Length + additional;
        if (_buffer is not null && required <= _buffer.Length)
        {
            return;
        }

        var current = _buffer?.Length ?? 0;
        var newSize = (int)Math.Min(
            Math.Max(required, Math.Max((long)current * 2, MinimumGrowth)),
            Array.MaxLength);
        if (newSize < required)
        {
            throw new OutOfMemoryException("The message exceeds the maximum supported array size.");
        }

        var next = _pool.Rent(newSize);
        if (_buffer is not null)
        {
            _buffer.AsSpan(0, Length).CopyTo(next);
            _pool.ReturnCleared(_buffer, Length);
        }

        _buffer = next;
    }
}
