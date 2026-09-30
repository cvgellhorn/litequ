# Changelog

## 3.0.0 (unreleased)

This release changes some defaults. Read the breaking changes before upgrading from `litequ` 1.x or `@sturmfrei/litequu` 2.x.

### ⚠ BREAKING CHANGES

- **`dbPath` defaults to `':memory:'`** instead of `'./queue.db'`, for both `Queue` and `Database`. A queue without a `dbPath` no longer creates a file in the working directory, and its tasks don't survive a restart. Pass a file path to keep tasks.
- **Interrupted tasks are restarted by default.** When a file database is opened, tasks left in `processing` by a process that stopped are set back to `pending` and run again. Set `recoverInterrupted: false` to keep the 2.x behavior, where they stayed stuck.

- **`add()` after `close()` throws** a clear error instead of reopening the database. A closed `Database` also throws instead of silently opening a new connection.

### Behavior changes

- `new Queue()` opens the database, creates the table and runs migrations immediately, instead of on the first `add()` or processing call.
- The schema version is tracked in `PRAGMA user_version`. Existing 2.x files (version 0) are migrated in place the first time they're opened. Their rows are kept.
- File databases get `PRAGMA busy_timeout` (default 5000 ms, see `busyTimeout`). In-memory databases no longer try to switch to WAL, which never applied to them.
- Log output goes through the new `logger` option instead of `console.error`.
- `close()` waits for running tasks through an internal idle event instead of polling every 100 ms, stops all timers, and returns the same promise when called again. A task that finishes after the connection closed (see `close({ timeout })`) keeps its `processing` status and is restarted later.
- An `error` event with no `error` listener is now logged through `logger.error`. Before, Node's `EventEmitter` threw it, which surfaced as an unhandled rejection from background processing.

### Features

- `queue.processOnce()` processes every ready task for jobs with a handler and resolves with the number of tasks attempted. It works with `autoProcess: false`. Passing a handler, as the removed 2.x `processOnce(handler)` did, throws a migration error.
- `busyTimeout` option.
- `logger` option: any object with `error`, `warn` and `info` methods.
- `recoverInterrupted` option (default `true`).
- `queue.pause()`, `queue.resume()` and `queue.whenIdle()`.
- `queue.close({ timeout })`.
- Public `idle` event.
- `status.paused` and `status.closed`.
- `queue.switchDatabase(dbPath, { moveOpenTasks, recoverInterrupted })` and the `database-switched` event.
- Read-only replica support: `writable`, `readOnlyDbPath`, `whenReadOnly` and `roleCheckInterval` options, the `role-change` event, `status.writable` and `status.dbPath`.
- `QueueReadOnlyError`, thrown by `add()` on a read-only queue with `whenReadOnly: 'throw'`.
- `litefsWritable(dir)` and `sqliteWritable(dbPath)` role checks.
