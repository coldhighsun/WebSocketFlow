# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build all projects
dotnet build

# Build release (no .pdb output)
dotnet build -c Release

# Run all tests (net10.0)
dotnet test

# Run a single test by name
dotnet test --filter "FullyQualifiedName~ReceiveMessageAsync_SingleFragment"

# Build library only
dotnet build src/WebSocketFlow/WebSocketFlow.csproj

# Pack NuGet package
dotnet pack src/WebSocketFlow/WebSocketFlow.csproj -c Release -o ./artifacts
```

## CI/CD

CI runs on `push`/`PR` to `main` (Ubuntu, .NET 10): `dotnet restore` → `dotnet build` → `dotnet test`. The publish job (tag-triggered) packs the library with .NET 10.

NuGet publish triggers automatically on `v*` tags (e.g. `v1.0.0`) via NuGet Trusted Publishing (OIDC): the `publish` job runs in the `release` environment with `id-token: write` and exchanges the `NUGET_USER` environment secret (nuget.org username) for a short-lived key through `NuGet/login@v1`. The nuget.org policy is bound to the workflow file name `ci.yml`.

## Architecture

This is a single-purpose library with no package dependencies.

**Public surface** (`src/WebSocketFlow/`):
- `WebSocketFlow` — static class with two extension methods on `WebSocket`:
  - `ReceiveMessageAsync` — receives one complete message, assembling fragments via the fast path (single allocation) or slow path (`PooledMessageBuffer`)
    - enforces `maxMessageSize` (default 16 MiB): on overflow it sends a best-effort `MessageTooBig` close frame, aborts the socket (the rest of the message is still in flight, so it must not be reused) and throws `InvalidDataException`
    - the close frame is bounded by `closeTimeout` (default 5 s) so an unresponsive peer cannot stall the rejection
    - arguments are validated eagerly
  - `ReadAllMessagesAsync` — `IAsyncEnumerable<WebSocketMessage>` that reuses one receive buffer; arguments are validated eagerly
    - ends quietly on a close frame, cancellation or abort
    - after we sent our own close frame it also ends quietly on `ConnectionClosedPrematurely`, and when the peer stays silent for `closeTimeout` (default 5 s) without starting to send; the timer stops once the peer starts sending, so in-flight messages are never truncated
    - any other failure, including a premature disconnect without our close frame, propagates
    - `bufferSize` must be 1 byte to 1 MiB; more than 1024 consecutive empty non-final fragments are rejected like an oversized message (`PolicyViolation` close frame, abort, `InvalidDataException`)
- `WebSocketMessage` — immutable result type: `MessageType`, `Data` (`ReadOnlyMemory<byte>`), `CloseReceived`, `GetString()`

**Internal**:
- `ReceiveBuffer` — pooled per-call/per-enumeration fragment read buffer of exact size; `BeginReceive`/`EndReceive` track written bytes, and `Dispose` wipes them on every exit path (the whole buffer if a receive never ended)
- `CloseHandshakeWatchdog` — per-enumeration helper for `ReadAllMessagesAsync`
  - polls the socket state (also notices a close frame sent by another thread during a pending receive) and cancels the token of a message's *first* read when the peer stays silent for `closeTimeout` after `CloseSent`; `End()` disarms it once the first bytes arrive
  - one shared `CancellationTokenSource`, all transitions under one lock; `TimeProvider`-injectable for tests
  - its poll timer only holds a weak reference, so an abandoned enumerator does not stay rooted
- `PooledMessageBuffer` — append-only accumulator backed by one geometrically-growing `ArrayPool` array, used only in the fragmented slow path; must be disposed (arrays are cleared on return)

**Testing**:
- `FakeWebSocket` — a queue-based `WebSocket` subclass; pre-enqueue fragments with `Enqueue()`/`EnqueueClose()` to drive tests without a real socket. Each `Enqueue` call is one frame; like a real socket it is split across several `ReceiveAsync` calls when the buffer is smaller than the frame. `CHANGELOG.md` lists user-visible and breaking changes; update it with API changes.

**Target framework**: `net10.0` only, set once in `Directory.Build.props` (library and tests). Uses the `Memory<byte>` overload of `WebSocket.ReceiveAsync`.

**Package management**: Central via `Directory.Packages.props`. `Directory.Build.props` sets `TargetFramework`, `LangVersion`, `ImplicitUsings`, `Nullable`, `UseArtifactsOutput`, and `DebugType=embedded` (symbols are embedded in the DLL; no separate pdb/snupkg is shipped).

**Versioning**: MinVer derives the package version from git tags (e.g. `v1.0.0`). No version is set in the project file; the version is `0.0.0-alpha.0` until a tag exists.
