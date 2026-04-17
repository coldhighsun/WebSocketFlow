# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build all projects
dotnet build

# Build release (no .pdb output)
dotnet build -c Release

# Run all tests (net8.0, net9.0, net10.0)
dotnet test

# Run a single test by name
dotnet test --filter "FullyQualifiedName~ReceiveMessageAsync_SingleFragment"

# Build library only
dotnet build src/WebSocketFlow/WebSocketFlow.csproj

# Pack NuGet package
dotnet pack src/WebSocketFlow/WebSocketFlow.csproj -c Release -o ./artifacts
```

## CI/CD

CI runs on `push`/`PR` to `main` (Ubuntu, .NET 8/9/10): `dotnet restore` → `dotnet build` → `dotnet test`.

NuGet publish triggers automatically on `v*` tags (e.g. `v1.0.0`) via the `NUGET_API_KEY` repository secret.

## Architecture

This is a single-purpose library with no dependencies beyond polyfills for `netstandard2.0`.

**Public surface** (`src/WebSocketFlow/`):
- `WebSocketFlow` — static class with two extension methods on `WebSocket`:
  - `ReceiveMessageAsync` — receives one complete message, assembling fragments via the fast path (single allocation) or slow path (`SegmentedBuffer`)
  - `ReadAllMessagesAsync` — `IAsyncEnumerable<WebSocketMessage>` wrapper around `ReceiveMessageAsync`
- `WebSocketMessage` — immutable result type: `MessageType`, `Data` (`ReadOnlyMemory<byte>`), `CloseReceived`

**Internal**:
- `SegmentedBuffer` — append-only, `ArrayPool`-backed accumulator used only in the fragmented slow path; must be disposed to return rented arrays

**Multi-targeting**: `netstandard2.0;net8.0;net9.0;net10.0`. The `netstandard2.0` target pulls in `Microsoft.Bcl.AsyncInterfaces`, `System.Memory`, and `System.Threading.Tasks.Extensions`. All targets use the `ArraySegment<byte>` overload of `WebSocket.ReceiveAsync` for uniformity. `LangVersion=latest` is set globally so C# modern syntax works on all TFMs.

**Package management**: Central via `Directory.Packages.props`. `Directory.Build.props` sets `ImplicitUsings`, `Nullable`, `ArtifactsPath`, and the Release-mode no-symbols config (`DebugType=none`, `DebugSymbols=false`).

**Versioning**: MinVer derives the package version from git tags (e.g. `v1.0.0`). No version is set in the project file; the version is `0.0.0-alpha.0` until a tag exists.
