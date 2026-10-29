using System.Buffers;

namespace WebSocketFlow;

/// <summary>
/// A pooled receive buffer of an exact requested size. Every exit path goes through
/// <see cref="Dispose"/>, which wipes the bytes reported by <see cref="EndReceive"/> before the array
/// goes back to the pool. When a receive was begun but never ended (it failed, so a late write may
/// still be pending), the whole buffer is wiped and the array is not returned to the pool.
/// </summary>
internal sealed class ReceiveBuffer : IDisposable
{
    /// <summary>
    /// The pool the array was rented from and goes back to.
    /// </summary>
    private readonly ArrayPool<byte> _pool;

    /// <summary>
    /// The exact size handed out by <see cref="BeginReceive"/>.
    /// </summary>
    private readonly int _size;

    /// <summary>
    /// The rented array, or <see langword="null"/> once disposed.
    /// </summary>
    private byte[]? _array;

    /// <summary>
    /// The largest number of bytes any completed receive has written.
    /// </summary>
    private int _used;

    /// <summary>
    /// Whether a receive has begun and not yet ended.
    /// </summary>
    private bool _receiving;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReceiveBuffer"/> class.
    /// </summary>
    /// <param name="size">The exact size of the buffer handed out for each receive.</param>
    /// <param name="pool">The pool to rent from; defaults to <see cref="ArrayPool{T}.Shared"/>.</param>
    public ReceiveBuffer(int size, ArrayPool<byte>? pool = null)
    {
        _pool = pool ?? ArrayPool<byte>.Shared;
        _size = size;
        _array = _pool.Rent(size);
    }

    /// <summary>
    /// Returns the buffer to pass to <c>ReceiveAsync</c>; its length is exactly the requested size.
    /// </summary>
    /// <returns>The buffer to receive into.</returns>
    /// <exception cref="ObjectDisposedException">The buffer has been disposed.</exception>
    public Memory<byte> BeginReceive()
    {
        ObjectDisposedException.ThrowIf(_array is null, this);

        _receiving = true;
        return _array.AsMemory(0, _size);
    }

    /// <summary>
    /// Completes a receive and returns the first <paramref name="count"/> bytes that were written.
    /// </summary>
    /// <param name="count">The number of bytes the receive reported.</param>
    /// <returns>The received bytes.</returns>
    /// <exception cref="ObjectDisposedException">The buffer has been disposed.</exception>
    public ReadOnlySpan<byte> EndReceive(int count)
    {
        ObjectDisposedException.ThrowIf(_array is null, this);

        _receiving = false;
        _used = Math.Max(_used, count);
        return _array.AsSpan(0, count);
    }

    /// <summary>
    /// Wipes the bytes that may have been written and returns the array to the pool. Subsequent calls
    /// are ignored.
    /// </summary>
    public void Dispose()
    {
        var array = _array;
        if (array is null)
        {
            return;
        }

        _array = null;

        // A receive that never ended may still be pending in a transport that ignored cancellation and
        // could write into the array later. Returning it would hand that array to another renter, so
        // it is wiped and dropped for the GC instead.
        if (_receiving)
        {
            array.AsSpan(0, _size).Clear();
            return;
        }

        _pool.ReturnCleared(array, _used);
    }
}
