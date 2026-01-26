# roam-dotnet

.NET Client SDK for ROAM. This library provides a C# wrapper around the native OAM shared library.

## Architecture

This SDK uses P/Invoke to call into the `roam-public` Rust core compiled as a dynamic library (`oam.dll` / `liboam.so`).

- **Core Logic:** Implemented in Rust (safe, high performance).
- **Interop:** `src/NativeMethods.cs` maps C-ABI exports.
- **Safety:** .NET handles memory management of strings via `SafeHandle` or explicit free calls.

## Usage

```csharp
using Roam.DotNet;

var agent = new Agent("agent-123");
agent.Connect();
```
