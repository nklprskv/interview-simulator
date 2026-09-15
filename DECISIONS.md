# Decisions

Assumptions made instead of asking questions, per the exercise's instructions.

## Architecture

- `Listen()`/`Serve()` split (like `net/http.Server`): binds synchronously,
  so tests run against a real ephemeral port instead of a fixed one + sleep.
- `Config`/`DefaultConfig()`: plain struct, no config files/env/flags (per scope).
- `cmd/simulator` (entrypoint, signals) + `internal/scheme` (server, protocol,
  tests) — `internal` because this isn't a library meant for import.

## Graceful shutdown

- "Accepted" = request line fully read (`Scanner.Scan()` returned). Anything
  before that (idle, or mid-line) is fair game to drop when the grace period ends.
- One shared `context.Context` cancels all in-flight delays at once, not per connection.
- After responding, a connection closes immediately if that context is
  already canceled — otherwise it'd sit waiting for a request that never
  comes, since the idle sweep only runs once.
- `Accept` and `Shutdown` share a mutex around `wg.Add`/`closing`, so a
  connection accepted right as shutdown starts can't register concurrently
  with `wg.Wait`. Found via `go test -race`, not theoretical.
- `Shutdown()` is idempotent (`sync.Once`) and returns early if nothing's in flight.

## Protocol

- `amount <= 0` → `Invalid amount`. Spec says "positive integer"; the
  prototype didn't enforce it — treated as a bug fix.
- A request line over `bufio.Scanner`'s 64KB default limit closes the
  connection with no response. Satisfies the "can't be safely handled" rule
  via the standard library's default, not a custom limit.

## Testing

- Ephemeral ports (`:0`), no shared fixed-port `TestMain` server.
- ~50ms timing tolerance, same as the provided `main_test.go`.
- Delay/grace-period tests use short `Config` values instead of the real
  10s/3s defaults, to stay fast.
- `go test -race` is required, not optional — it caught the mutex issue above.

## Out of scope

Metrics/tracing, health checks, deploy artifacts, config files/env/flags,
rate limiting, half-open/"ghost session" handling — per the exercise's scope
section. Logging is start/stop/errors only.
