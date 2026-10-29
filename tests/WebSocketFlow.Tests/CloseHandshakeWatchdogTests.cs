using System.Net.WebSockets;

namespace WebSocketFlow.Tests;

/// <summary>
/// Tests for <see cref="CloseHandshakeWatchdog"/>.
/// </summary>
public class CloseHandshakeWatchdogTests
{
    /// <summary>
    /// The close timeout used by most tests.
    /// </summary>
    private static readonly TimeSpan Timeout5s = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Creates a watchdog bound to the test cancellation token.
    /// </summary>
    private static CloseHandshakeWatchdog Create(FakeWebSocket ws, ManualTimeProvider clock)
        => CreateWithCaller(ws, clock, TestContext.Current.CancellationToken);

    /// <summary>
    /// Creates a watchdog bound to the given caller token.
    /// </summary>
    private static CloseHandshakeWatchdog CreateWithCaller(FakeWebSocket ws, ManualTimeProvider clock, CancellationToken callerToken)
        => new(ws, Timeout5s, callerToken, clock);

    /// <summary>
    /// Tests Poll. Scenario: state getter throws. Expected: no exception escapes and no timeout occurs.
    /// </summary>
    [Fact]
    public void Poll_StateGetterThrows_DoesNotThrowAndNeverTimesOut()
    {
        var ws = new FakeWebSocket { ThrowOnStateRead = true };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        watchdog.Begin();
        clock.Advance(Timeout5s * 2);

        watchdog.Poll();

        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests Poll. Scenario: while socket open. Expected: never times out.
    /// </summary>
    [Fact]
    public void Poll_WhileSocketOpen_NeverTimesOut()
    {
        var ws = new FakeWebSocket();
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        var token = watchdog.Begin();

        clock.Advance(TimeSpan.FromHours(1));
        watchdog.Poll();

        Assert.False(token.IsCancellationRequested);
        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests Poll. Scenario: close sent appears later. Expected: times out from first observation.
    /// </summary>
    [Fact]
    public void Poll_CloseSentAppearsLater_TimesOutFromFirstObservation()
    {
        var ws = new FakeWebSocket();
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        var token = watchdog.Begin();

        clock.Advance(TimeSpan.FromHours(1));
        ws.StateOverride = WebSocketState.CloseSent;
        watchdog.Poll();
        Assert.False(token.IsCancellationRequested);

        clock.Advance(TimeSpan.FromSeconds(4.9));
        watchdog.Poll();
        Assert.False(token.IsCancellationRequested);

        clock.Advance(TimeSpan.FromSeconds(0.1));
        watchdog.Poll();
        Assert.True(token.IsCancellationRequested);
        Assert.True(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests Poll. Scenario: sub millisecond timeout. Expected: is honored without truncation.
    /// </summary>
    [Fact]
    public void Poll_SubMillisecondTimeout_IsHonoredWithoutTruncation()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = new CloseHandshakeWatchdog(ws, TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken, clock);
        var token = watchdog.Begin();

        watchdog.Poll();
        Assert.False(token.IsCancellationRequested);

        clock.Advance(TimeSpan.FromMilliseconds(5));
        watchdog.Poll();
        Assert.True(token.IsCancellationRequested);
    }

    /// <summary>
    /// Tests Begin. Scenario: then end. Expected: the timer runs only while a watch is active.
    /// </summary>
    [Fact]
    public void Begin_ThenEnd_RunsPollTimerOnlyWhileWatching()
    {
        var clock = new ManualTimeProvider();
        using var watchdog = Create(new FakeWebSocket(), clock);
        Assert.False(clock.TimerRunning);

        watchdog.Begin();
        Assert.True(clock.TimerRunning);
        Assert.Equal(1, clock.TimerChangeCount);

        watchdog.End();
        Assert.False(clock.TimerRunning);

        watchdog.Begin();
        Assert.True(clock.TimerRunning);
        watchdog.End();
        Assert.False(clock.TimerRunning);
    }

    /// <summary>
    /// Tests Poll. Scenario: the poll callback throws something other than ObjectDisposedException. Expected: it does not escape the timer thread.
    /// </summary>
    [Fact]
    public void Poll_StateGetterThrows_DoesNotThrow()
    {
        var ws = new FakeWebSocket { StateException = new InvalidOperationException("boom") };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        watchdog.Begin();

        watchdog.Poll();

        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests Poll. Scenario: idle after end although the socket is in CloseSent. Expected: never times out.
    /// </summary>
    [Fact]
    public void Poll_IdleAfterEnd_NeverTimesOut()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        watchdog.Begin();
        watchdog.End();

        clock.Advance(TimeSpan.FromHours(1));
        watchdog.Poll();

        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests StopPolling. Scenario: called while waiting. Expected: the timer stops and the watch stays armed.
    /// </summary>
    [Fact]
    public void StopPolling_WhileWaiting_StopsTimerButKeepsWatch()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        var token = watchdog.Begin();

        watchdog.StopPolling();
        Assert.False(clock.TimerRunning);

        watchdog.Poll();
        clock.Advance(Timeout5s);
        watchdog.Poll();
        Assert.True(token.IsCancellationRequested);
        Assert.True(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests End. Scenario: after begin. Expected: stops the watch.
    /// </summary>
    [Fact]
    public void End_AfterBegin_StopsTheWatch()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        var token = watchdog.Begin();
        watchdog.Poll();

        watchdog.End();
        clock.Advance(TimeSpan.FromHours(1));
        watchdog.Poll();

        Assert.False(token.IsCancellationRequested);
        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests End. Scenario: after timeout raced with first bytes. Expected: hands out afresh token.
    /// </summary>
    [Fact]
    public void End_AfterTimeoutRacedWithFirstBytes_HandsOutAFreshToken()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        var stale = watchdog.Begin();
        watchdog.Poll();
        clock.Advance(Timeout5s);
        watchdog.Poll();
        Assert.True(stale.IsCancellationRequested);

        // The first bytes arrived although the deadline just passed.
        watchdog.End();
        var next = watchdog.Begin();

        Assert.False(watchdog.TimedOut);
        Assert.False(next.IsCancellationRequested);
    }

    /// <summary>
    /// Tests Begin. Scenario: after end. Expected: restarts the clock.
    /// </summary>
    [Fact]
    public void Begin_AfterEnd_RestartsTheClock()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var clock = new ManualTimeProvider();
        using var watchdog = Create(ws, clock);
        watchdog.Begin();
        watchdog.Poll();
        clock.Advance(TimeSpan.FromSeconds(4));
        watchdog.End();

        var token = watchdog.Begin();
        watchdog.Poll();
        clock.Advance(TimeSpan.FromSeconds(4));
        watchdog.Poll();

        Assert.False(token.IsCancellationRequested);
    }

    /// <summary>
    /// Tests Begin. Scenario: caller token cancelled. Expected: cancels the watch token.
    /// </summary>
    [Fact]
    public void Begin_CallerTokenCancelled_CancelsTheWatchToken()
    {
        var ws = new FakeWebSocket();
        using var cts = new CancellationTokenSource();
        using var watchdog = CreateWithCaller(ws, new ManualTimeProvider(), cts.Token);
        var token = watchdog.Begin();

        cts.Cancel();

        Assert.True(token.IsCancellationRequested);
        Assert.False(watchdog.TimedOut);
    }

    /// <summary>
    /// Tests Dispose. Scenario: then begin and poll. Expected: begin throws and poll is harmless.
    /// </summary>
    [Fact]
    public void Dispose_ThenBeginAndPoll_BeginThrowsAndPollIsHarmless()
    {
        var ws = new FakeWebSocket { StateOverride = WebSocketState.CloseSent };
        var watchdog = new CloseHandshakeWatchdog(ws, Timeout5s, TestContext.Current.CancellationToken, new ManualTimeProvider());
        watchdog.Begin();
        watchdog.Dispose();
        watchdog.Dispose();

        watchdog.Poll();
        watchdog.End();

        Assert.Throws<ObjectDisposedException>(() => watchdog.Begin());
    }

    /// <summary>
    /// Tests Dispose. Scenario: not called. Expected: abandoned watchdog is collected.
    /// </summary>
    [Fact]
    public void Dispose_NotCalled_AbandonedWatchdogIsCollected()
    {
        var reference = CreateAbandoned();

        for (var attempt = 0; attempt < 10 && IsAlive(reference); attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(IsAlive(reference));
    }

    // Separate frame: a debug-build temporary for the target must not outlive the check.
    /// <summary>
    /// Reports whether the referenced watchdog has not been collected yet.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static bool IsAlive(WeakReference<CloseHandshakeWatchdog> reference) => reference.TryGetTarget(out _);

    /// <summary>
    /// Creates a watchdog with a real timer that is never disposed and returns a weak reference to it.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference<CloseHandshakeWatchdog> CreateAbandoned()
    {
        // Real timer, never disposed: only a weakly referenced poll callback may stay behind.
        var watchdog = new CloseHandshakeWatchdog(
            new FakeWebSocket(), TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        watchdog.Begin();
        return new WeakReference<CloseHandshakeWatchdog>(watchdog);
    }
}
