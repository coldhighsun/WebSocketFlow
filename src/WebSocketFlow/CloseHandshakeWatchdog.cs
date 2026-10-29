using System.Net.WebSockets;

namespace WebSocketFlow;

/// <summary>
/// Bounds how long a receive may wait for the first bytes of a message once this side has sent its
/// own close frame. The socket state is polled, so a close frame sent by another thread while a
/// receive is already pending is noticed too; the timeout then runs from the first poll that sees
/// <see cref="WebSocketState.CloseSent"/>, so it is accurate to within one poll interval (at most 1 s).
/// </summary>
/// <remarks>
/// One instance serves a whole enumeration. <see cref="Begin"/> returns the token for the first read of
/// the next message and makes sure the poll timer runs; <see cref="End"/> stops the watch as soon as
/// the first bytes arrive, so a message that is already in flight is never interrupted. The timer only runs
/// while a watch is active and is released by <see cref="Dispose"/>.
/// All state changes, including the cancellation
/// itself, happen under one lock: a timeout that races with the arrival of the first bytes is either
/// seen by <see cref="End"/>, which then hands out a fresh token, or does not happen at all.
/// </remarks>
internal sealed class CloseHandshakeWatchdog : IDisposable
{
    /// <summary>
    /// Shortest interval between two polls of the socket state.
    /// </summary>
    private static readonly TimeSpan MinimumPollInterval = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Longest interval between two polls of the socket state.
    /// </summary>
    private static readonly TimeSpan MaximumPollInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The stages a single watch goes through.
    /// </summary>
    private enum Phase
    {
        /// <summary>
        /// Not waiting for the first bytes of a message.
        /// </summary>
        Idle,

        /// <summary>
        /// Waiting, and the socket has not been seen in <see cref="WebSocketState.CloseSent"/> yet.
        /// </summary>
        Waiting,

        /// <summary>
        /// Waiting, and the socket entered <see cref="WebSocketState.CloseSent"/> at <c>_closeSentAt</c>.
        /// </summary>
        CloseSentSeen,

        /// <summary>
        /// The peer stayed silent for the timeout and the token was cancelled.
        /// </summary>
        TimedOut,
    }

    /// <summary>
    /// The socket whose state is polled.
    /// </summary>
    private readonly WebSocket _webSocket;

    /// <summary>
    /// How long the peer may stay silent after our close frame.
    /// </summary>
    private readonly TimeSpan _timeout;

    /// <summary>
    /// The caller's token, which every handed-out token is linked to.
    /// </summary>
    private readonly CancellationToken _callerToken;

    /// <summary>
    /// The clock used for measuring the silence.
    /// </summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// The poll timer, started by <see cref="Begin"/>.
    /// </summary>
    private readonly PollTimer _timer;

    /// <summary>
    /// Guards every mutable field below.
    /// </summary>
    private readonly object _gate = new();

    /// <summary>
    /// The source behind the token of the current watch.
    /// </summary>
    private CancellationTokenSource _cts;

    /// <summary>
    /// The current stage of the watch.
    /// </summary>
    private Phase _phase;

    /// <summary>
    /// The timestamp at which <see cref="WebSocketState.CloseSent"/> was first observed.
    /// </summary>
    private long _closeSentAt;

    /// <summary>
    /// Whether <see cref="Dispose"/> has run.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="CloseHandshakeWatchdog"/> class.
    /// </summary>
    /// <param name="webSocket">The socket whose state is polled.</param>
    /// <param name="timeout">How long the peer may stay silent after our close frame.</param>
    /// <param name="callerToken">The caller's token; cancelling it cancels every handed-out token.</param>
    /// <param name="timeProvider">The clock to use; defaults to <see cref="TimeProvider.System"/>.</param>
    public CloseHandshakeWatchdog(
        WebSocket webSocket, TimeSpan timeout, CancellationToken callerToken, TimeProvider? timeProvider = null)
    {
        _webSocket = webSocket;
        _timeout = timeout;
        _callerToken = callerToken;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(callerToken);

        _timer = new PollTimer(this, _timeProvider, Clamp(timeout / 4, MinimumPollInterval, MaximumPollInterval));
    }

    /// <summary>
    /// Gets a value indicating whether the current watch ended because the peer stayed silent.
    /// </summary>
    public bool TimedOut
    {
        get
        {
            lock (_gate)
            {
                return _phase == Phase.TimedOut;
            }
        }
    }

    /// <summary>
    /// Starts watching for the first bytes of the next message and returns the token to use for that read.
    /// It is cancelled when the caller's token is, or when the peer stays silent for the timeout.
    /// </summary>
    /// <returns>The token for the first read of the next message.</returns>
    public CancellationToken Begin()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _phase = Phase.Waiting;
            _timer.Start();
            return _cts.Token;
        }
    }

    /// <summary>
    /// Stops the watch because the first bytes arrived. If the timeout expired at the very same moment,
    /// the peer did answer in time as far as the caller is concerned, so the cancelled source is replaced.
    /// </summary>
    public void End()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            if (_phase == Phase.TimedOut)
            {
                _cts.Dispose();
                _cts = CancellationTokenSource.CreateLinkedTokenSource(_callerToken);
            }

            // Nothing to watch while the consumer handles the message: stop the timer so an idle
            // enumeration does not wake a thread-pool thread every interval. Begin restarts it.
            _phase = Phase.Idle;
            _timer.Stop();
        }
    }

    /// <summary>
    /// Stops the poll timer without ending the watch, for when the first read failed before any bytes
    /// arrived. A timeout that already happened stays visible through <see cref="TimedOut"/>.
    /// </summary>
    public void StopPolling()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _timer.Stop();
        }
    }

    /// <summary>
    /// Stops polling and releases the cancellation source. Subsequent calls are ignored.
    /// </summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _timer.Dispose();
            _cts.Dispose();
        }
    }

    /// <summary>
    /// Checks the socket state once and cancels the current token if the peer has been silent for too long.
    /// </summary>
    internal void Poll()
    {
        // Read before taking the lock: a WebSocket subclass's State getter is foreign code. This runs on a
        // timer thread, where any exception would be unhandled and end the process, so a getter that
        // throws (typically a disposed socket) is treated as "no state to act on". The receive itself
        // surfaces the real failure on the caller's thread.
        WebSocketState state;
        try
        {
            state = _webSocket.State;
        }
        catch (Exception)
        {
            return;
        }

        // Nearly every tick finds a socket that is not closing: leave without touching the lock.
        if (state != WebSocketState.CloseSent)
        {
            return;
        }

        lock (_gate)
        {
            if (_disposed || _phase is Phase.Idle or Phase.TimedOut)
            {
                return;
            }

            if (_phase == Phase.Waiting)
            {
                _phase = Phase.CloseSentSeen;
                _closeSentAt = _timeProvider.GetTimestamp();
            }

            if (_timeProvider.GetElapsedTime(_closeSentAt) < _timeout)
            {
                return;
            }

            _phase = Phase.TimedOut;
            _timer.Stop();

            // The token is flagged immediately, while the registered callbacks run on another thread,
            // so no receive continuation executes inside this lock.
            // A failing callback belongs to the receive that registered it and surfaces there, so the
            // task is only observed to keep that failure from becoming an unobserved task exception.
            _cts.CancelAsync().ContinueWith(
                static task => _ = task.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Polls the watchdog without keeping it alive: the timer queue roots this object, not the watchdog,
    /// so an enumerator that is dropped without being disposed lets the watchdog (and the socket it
    /// references) be collected, and the timer then stops itself.
    /// </summary>
    private sealed class PollTimer : IDisposable
    {
        /// <summary>
        /// The watchdog to poll, held weakly.
        /// </summary>
        private readonly WeakReference<CloseHandshakeWatchdog> _target;

        /// <summary>
        /// The underlying timer; created stopped.
        /// </summary>
        private readonly ITimer _timer;

        /// <summary>
        /// The interval between two polls while the timer runs.
        /// </summary>
        private readonly TimeSpan _interval;

        /// <summary>
        /// Whether the timer is started; only touched under the watchdog's lock, so a repeated
        /// start or stop costs no timer change.
        /// </summary>
        private bool _running;

        /// <summary>
        /// Initializes a new instance of the <see cref="PollTimer"/> class, stopped.
        /// </summary>
        /// <param name="target">The watchdog to poll.</param>
        /// <param name="timeProvider">The clock that creates the timer.</param>
        /// <param name="interval">The interval between two polls while the timer runs.</param>
        public PollTimer(CloseHandshakeWatchdog target, TimeProvider timeProvider, TimeSpan interval)
        {
            _target = new WeakReference<CloseHandshakeWatchdog>(target);
            _interval = interval;
            _timer = timeProvider.CreateTimer(
                static state => ((PollTimer)state!).Tick(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        /// <summary>
        /// Starts polling every interval.
        /// </summary>
        public void Start()
        {
            if (_running)
            {
                return;
            }

            _running = true;
            _timer.Change(_interval, _interval);
        }

        /// <summary>
        /// Stops polling until <see cref="Start"/> is called again.
        /// </summary>
        public void Stop()
        {
            if (!_running)
            {
                return;
            }

            _running = false;
            _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        /// <summary>
        /// Releases the underlying timer.
        /// </summary>
        public void Dispose() => _timer.Dispose();

        /// <summary>
        /// Polls the watchdog, or disposes the timer if the watchdog has been collected.
        /// </summary>
        private void Tick()
        {
            if (_target.TryGetTarget(out var watchdog))
            {
                watchdog.Poll();
            }
            else
            {
                _timer.Dispose();
            }
        }
    }

    /// <summary>
    /// Limits <paramref name="value"/> to the range from <paramref name="min"/> to <paramref name="max"/>.
    /// </summary>
    /// <param name="value">The value to limit.</param>
    /// <param name="min">The lower bound.</param>
    /// <param name="max">The upper bound.</param>
    /// <returns>The limited value.</returns>
    private static TimeSpan Clamp(TimeSpan value, TimeSpan min, TimeSpan max)
        => value < min ? min : value > max ? max : value;
}
