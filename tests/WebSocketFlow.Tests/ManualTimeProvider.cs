namespace WebSocketFlow.Tests;

/// <summary>
/// A clock that only moves when told to; its timers never fire, so tests drive polling by hand.
/// </summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    /// <summary>
    /// The current fake time in milliseconds.
    /// </summary>
    private long _milliseconds;

    /// <inheritdoc />
    public override long TimestampFrequency => 1000;

    /// <inheritdoc />
    public override long GetTimestamp() => _milliseconds;

    /// <summary>
    /// Moves the clock forward by <paramref name="by"/>.
    /// </summary>
    public void Advance(TimeSpan by) => _milliseconds += (long)by.TotalMilliseconds;

    /// <summary>
    /// Gets a value indicating whether the most recently created timer is started.
    /// </summary>
    public bool TimerRunning => _timer?.Running == true;

    /// <summary>
    /// Gets how many times the most recently created timer was changed.
    /// </summary>
    public int TimerChangeCount => _timer?.ChangeCount ?? 0;

    /// <summary>
    /// The most recently created timer.
    /// </summary>
    private NoopTimer? _timer;

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        => _timer = new NoopTimer(callback, state, dueTime != Timeout.InfiniteTimeSpan);

    /// <summary>
    /// Runs the callback of the most recently created timer once, on the calling thread, like a poll tick.
    /// It runs even while the timer is stopped, which simulates a tick that raced with the stop.
    /// </summary>
    public void Tick() => _timer?.Fire();

    /// <summary>
    /// A timer that never fires by itself but remembers whether it is started.
    /// </summary>
    private sealed class NoopTimer(TimerCallback callback, object? state, bool running) : ITimer
    {
        /// <summary>
        /// Runs the callback once.
        /// </summary>
        public void Fire() => callback(state);

        /// <summary>
        /// Gets a value indicating whether the timer is started.
        /// </summary>
        public bool Running { get; private set; } = running;

        /// <summary>
        /// Gets how many times <see cref="Change"/> was called.
        /// </summary>
        public int ChangeCount { get; private set; }

        /// <inheritdoc />
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            ChangeCount++;
            Running = dueTime != Timeout.InfiniteTimeSpan;
            return true;
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
