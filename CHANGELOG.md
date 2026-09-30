# Changelog

## 2.0.0 (unreleased)

This release changes some defaults. Read the breaking changes before upgrading from `litequ` 1.x or from `@sturmfrei/litequu`.

### ⚠ BREAKING CHANGES

- **`dbPath` defaults to `':memory:'`** instead of `'./queue.db'`, for both `Queue` and `Database`. A queue without a `dbPath` no longer creates a file in the working directory, and its tasks don't survive a restart. Pass a file path to keep tasks.
- **Interrupted tasks are restarted by default.** When a file database is opened, tasks left in `processing` by a process that stopped (no lease, or an expired one) are set back to `pending` and run again. Set `recoverInterrupted: false` to skip this step. Expired tasks are still claimed during normal processing; before, they stayed stuck forever.

- **Leases, and a schema migration for existing files.** Claiming a task now records a lease (`locked_by`, `locked_until`) in one atomic `UPDATE ... RETURNING`, so two processes on one file never run the same task. Opening a file created by `litequ` 1.x or `@sturmfrei/litequu` adds the lease columns and, for throttling, a `dedupe_key` column (schema version 3). `recoverInterrupted` and `switchDatabase` only restart `processing` tasks whose lease is missing or expired, and expired tasks are claimed again without a restart. Once migrated, don't point an older version at the file: it ignores leases.
- **`add()` after `close()` throws** a clear error instead of reopening the database. A closed `Database` also throws instead of silently opening a new connection.

### Behavior changes

- `new Queue()` opens the database, creates the table and runs migrations immediately, instead of on the first `add()` or processing call.
- The schema version is tracked in `PRAGMA user_version`. Existing unversioned files (version 0, from `litequ` 1.x or `@sturmfrei/litequu`) are migrated in place the first time they're opened. Their rows are kept.
- File databases get `PRAGMA busy_timeout` (default 5000 ms, see `busyTimeout`). In-memory databases no longer try to switch to WAL, which never applied to them.
- Log output goes through the new `logger` option instead of `console.error`.
- `close()` waits for running tasks through an internal idle event instead of polling every 100 ms, stops all timers, and returns the same promise when called again. A task that finishes after the connection closed (see `close({ timeout })`) keeps its `processing` status and is picked up again once its lease expires.
- Delivery is at least once. A task can run twice if its lease expires while its worker is still running it. If that worker finishes afterwards, its result isn't saved: it logs a warning and emits no `completed`, `retried` or `failed` event.
- New tasks store `created_at` from the JavaScript clock with millisecond precision (`YYYY-MM-DD HH:MM:SS.SSS`, UTC) instead of SQLite's second-precision `CURRENT_TIMESTAMP`. The values sort correctly next to older rows.
- `Database.getPendingTasks()` also returns `processing` tasks with an expired lease, and orders by `created_at, id`. The queue now uses the new `Database.claimTasks()`. `Database.updateTaskStatus()` clears the lease and accepts `{ lockedBy }` to update only while that worker holds the lease.
- An `error` event with no `error` listener is now logged through `logger.error`. Before, Node's `EventEmitter` threw it, which surfaced as an unhandled rejection from background processing.

### Bug fixes

- `cleanup(hours)` compared SQLite `updated_at` values against an ISO cutoff. Whenever the cutoff fell on the same UTC day, for example `cleanup(1)` in the afternoon, it deleted tasks that had completed moments earlier. It now compares in SQLite's format.
- A due retry for a job without a handler no longer makes the wake-up timer fire again and again with a 0 ms delay. Wake-ups now only consider jobs that have a handler.

### Features

- `queue.processOnce()` processes every ready task for jobs with a handler and resolves with the number of tasks attempted. It works with `autoProcess: false`. Passing a handler, as the `processOnce(handler)` removed from `@sturmfrei/litequu` 2.x did, throws a migration error.
- `busyTimeout` option.
- `logger` option: any object with `error`, `warn` and `info` methods.
- `recoverInterrupted` option (default `true`).
- `queue.pause()`, `queue.resume()` and `queue.whenIdle()`.
- `queue.close({ timeout })`.
- Public `idle` event.
- `status.paused` and `status.closed`.
- `queue.switchDatabase(dbPath, { moveOpenTasks, recoverInterrupted })` and the `database-switched` event.
- Read-only replica support: a `writable` callback you supply (litequ doesn't detect roles itself), plus `readOnlyDbPath`, `whenReadOnly` and `roleCheckInterval` options, the `role-change` event, `status.writable` and `status.dbPath`.
- `QueueReadOnlyError`, thrown by `add()` on a read-only queue with `whenReadOnly: 'throw'`.
- `sqliteWritable(dbPath)`, a generic `writable` callback that probes the database file.
- `leaseMs` option, `queue.instanceId`, lease heartbeats.
- `Queue.shared(options)`: one queue per key per process, shared across module copies through `globalThis[Symbol.for('litequ.registry')]`.
- `job.add(data, { dedupeKey, throttleMs })` throttles in the database and returns `null` for a dropped task. It uses schema version 3, which adds the `dedupe_key` column and an index on `(job_name, dedupe_key, created_at)`.
- `queue.defineJob(name, { handler, onCompleted, onFailed, onRetried })`, which replaces its earlier handler and callbacks instead of adding duplicates.
- `Database.claimTasks()`, `Database.extendLease()` and `Database.getNextWakeTime()`.
- TypeScript declarations (`types/`), generated from JSDoc at pack time, plus a `typecheck` script.
