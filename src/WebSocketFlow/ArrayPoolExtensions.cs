using System.Buffers;

namespace WebSocketFlow;

/// <summary>
/// Helpers shared by the buffers that rent message data from an <see cref="ArrayPool{T}"/>.
/// </summary>
internal static class ArrayPoolExtensions
{
    /// <summary>
    /// Wipes the leading bytes of <paramref name="array"/> that may contain message data, then returns it
    /// to the pool, so payloads do not linger in shared memory.
    /// </summary>
    /// <param name="pool">The pool the array was rented from.</param>
    /// <param name="array">The array to return.</param>
    /// <param name="usedLength">How many leading bytes may contain message data.</param>
    public static void ReturnCleared(this ArrayPool<byte> pool, byte[] array, int usedLength)
    {
        array.AsSpan(0, usedLength).Clear();
        pool.Return(array);
    }
}
