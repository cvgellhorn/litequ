# Changelog

## [2.1.1](https://github.com/cvgellhorn/litequ/compare/v2.1.0...v2.1.1) (2026-10-02)


### Bug Fixes

* target ES2022 so typecheck accepts Error cause ([c600173](https://github.com/cvgellhorn/litequ/commit/c600173eb893175670e91384d9a36a3fe13035ec))

## [2.1.0](https://github.com/cvgellhorn/litequ/compare/v2.0.0...v2.1.0) (2026-10-02)


### Features

* **deps:** bump all dependencies to latest ([ec3fa76](https://github.com/cvgellhorn/litequ/commit/ec3fa7602a24fe3007083432b6778b584ce52247))
* **deps:** bump all dependencies to latest ([0dc6e34](https://github.com/cvgellhorn/litequ/commit/0dc6e3434384f62070f2bbab317fe261a939e925))

## [2.0.0](https://github.com/cvgellhorn/litequ/compare/v1.0.0...v2.0.0) (2026-09-30)


### ⚠ BREAKING CHANGES

* existing database files are migrated to schema version 2 (new lease columns). Interrupted tasks with a live lease are no longer restarted on open. Delivery is at least once.
* add() after close() throws, and a closed Database no longer reopens its connection on the next call.
* tasks left in 'processing' are restarted by default instead of staying stuck. Set recoverInterrupted: false for the old behavior.
* dbPath defaults to ':memory:' instead of './queue.db'. Pass a file path to persist tasks.

### Features

* add pause, resume, whenIdle and close({ timeout }) ([df1add3](https://github.com/cvgellhorn/litequ/commit/df1add393c4d068a6d94026a79a6269f30b9fd55))
* add public queue.processOnce() ([8098ecd](https://github.com/cvgellhorn/litequ/commit/8098ecd84b4c35e481a6d788d62873dd3e6a1906))
* add Queue.shared() and queue.defineJob() ([ce239f4](https://github.com/cvgellhorn/litequ/commit/ce239f4da88823e56c2b1e51fda1f0a6538c0778))
* claim tasks with leases instead of a blanket reset ([33f8631](https://github.com/cvgellhorn/litequ/commit/33f8631619a20d6a169c3120ff2facc7d4213ae1))
* default to in-memory db, add busyTimeout, logger and schema versioning ([5a16802](https://github.com/cvgellhorn/litequ/commit/5a1680289afafbd1e8944398295c259714da2026))
* restart interrupted tasks when a file database is opened ([74f4730](https://github.com/cvgellhorn/litequ/commit/74f4730730faed0176db64de820a231298de58bc))
* switch databases and follow a read-only role ([5cf2ea9](https://github.com/cvgellhorn/litequ/commit/5cf2ea9cd5dc81b2d712a16c4939808fd98a4548))
* throttle tasks in the database with dedupeKey and throttleMs ([3ac9f3d](https://github.com/cvgellhorn/litequ/commit/3ac9f3d05c94ea8c720cc7d8f7999ba1d65db44e))


### Bug Fixes

* compare the cleanup cutoff in SQLite's timestamp format ([d5db3a3](https://github.com/cvgellhorn/litequ/commit/d5db3a31fc9d302d19f72f9535248521a5ad6594))

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
- Read-only replica support: `writable`, `readOnlyDbPath`, `whenReadOnly` and `roleCheckInterval` options, the `role-change` event, `status.writable` and `status.dbPath`.
- `QueueReadOnlyError`, thrown by `add()` on a read-only queue with `whenReadOnly: 'throw'`.
- `litefsWritable(dir)` and `sqliteWritable(dbPath)` role checks.
- `leaseMs` option, `queue.instanceId`, lease heartbeats.
- `Queue.shared(options)`: one queue per key per process, shared across module copies through `globalThis[Symbol.for('litequ.registry')]`.
- `job.add(data, { dedupeKey, throttleMs })` throttles in the database and returns `null` for a dropped task. It uses schema version 3, which adds the `dedupe_key` column and an index on `(job_name, dedupe_key, created_at)`.
- `queue.defineJob(name, { handler, onCompleted, onFailed, onRetried })`, which replaces its earlier handler and callbacks instead of adding duplicates.
- `Database.claimTasks()`, `Database.extendLease()` and `Database.getNextWakeTime()`.
- TypeScript declarations (`types/`), generated from JSDoc at pack time, plus a `typecheck` script.
