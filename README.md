# LiteQu

A simple, persistent task queue for Node.js using SQLite as storage. Tasks are processed in the main thread with configurable concurrency, automatic retries with exponential backoff, and comprehensive event handling.

## Features

- ✅ **Persistent Storage**: Uses SQLite for reliable task persistence
- 🎯 **Multi-Job Support**: Create named jobs with dedicated handlers for different task types
- ⚡ **Same-Thread Processing**: Runs in the main Node.js thread (perfect for I/O-bound tasks)
- 🔄 **Automatic Retries**: Exponential backoff with configurable retry limits
- 🚦 **Concurrency Control**: Configurable maximum concurrent task processing
- 📊 **Event-Driven**: Comprehensive event system with job-level and queue-level events
- 🔍 **Task Management**: Query task status, statistics, and cleanup utilities
- 🕐 **Auto-Processing**: Wakes when tasks are added and schedules retries when due
- 🔒 **Leases**: Several processes can share one file without running a task twice at the same time, and tasks from crashed workers are picked up again
- 🪞 **Read-Only Replicas**: Follows a writable role, for example the LiteFS primary, and switches databases when it changes
- ⏱️ **Throttling**: Drop duplicate tasks per key within a time window, stored in the database
- ⏸️ **Lifecycle Control**: `pause()`, `resume()`, `whenIdle()` and `close({ timeout })`
- 🧩 **Typed**: Ships TypeScript declarations generated from JSDoc
- 📦 **Zero Config**: Works out of the box with sensible defaults

## Installation

```bash
npm i litequ
```

Upgrading from `litequ` 1.x or `@sturmfrei/litequu` 2.x? Some defaults changed in 2.0.0, see the [changelog](CHANGELOG.md).

## Quick Start

### Multi-Job API

Create named jobs for different task types with dedicated handlers:

```javascript
import Queue from 'litequ';

// Create a queue
const queue = new Queue({
  dbPath: './my-queue.db',
  maxConcurrent: 5,
  maxRetries: 3,
  baseRetryDelay: 1000,
});

// Create different jobs for different types of work
const emailJob = queue.createJob('email');
const smsJob = queue.createJob('sms');
const webhookJob = queue.createJob('webhook');

// Set up handlers for each job
await emailJob.process(async (taskData) => {
  await sendEmail(taskData.to, taskData.subject, taskData.body);
  return `Email sent to ${taskData.to}`;
});

await smsJob.process(async (taskData) => {
  await sendSMS(taskData.phone, taskData.message);
  return `SMS sent to ${taskData.phone}`;
});

await webhookJob.process(async (taskData) => {
  await callWebhook(taskData.url, taskData.payload);
  return `Webhook called: ${taskData.url}`;
});

// Add tasks to specific jobs
emailJob.add({
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Thanks for signing up',
});

smsJob.add({
  phone: '+1234567890',
  message: 'Your verification code is 123456',
});

// Listen to job-specific events
emailJob.on('completed', (info) => {
  console.log(`Email task ${info.taskId} completed:`, info.result);
});

// Listen to all events from all jobs at the queue level
queue.on('completed', (info) => {
  console.log(`[${info.jobName}] Task ${info.taskId} completed`);
});

queue.on('failed', (info) => {
  console.log(`[${info.jobName}] Task ${info.taskId} failed:`, info.error);
});
```

## Configuration Options

```javascript
const queue = new Queue({
  // Database file path (default: ':memory:')
  // Without a file path, tasks live in memory and are lost on restart.
  dbPath: './my-app-queue.db',

  // Maximum concurrent tasks (default: 5)
  maxConcurrent: 3,

  // Maximum retry attempts (default: 15)
  maxRetries: 5,

  // Base retry delay in milliseconds (default: 15_000ms)
  baseRetryDelay: 2000,

  // Enable automatic processing (default: true)
  autoProcess: true,

  // Add jitter to retry delays (default: true)
  jitter: true,

  // How long to wait for another connection's lock before failing with
  // SQLITE_BUSY, in milliseconds (default: 5000)
  busyTimeout: 5000,

  // Where litequ logs errors, warnings and info messages (default: console)
  logger: console,

  // Restart tasks left in 'processing' by a process that stopped, when a
  // file database is opened (default: true)
  recoverInterrupted: true,

  // Read-only replicas (see "Read-only replicas" below)
  writable: () => true, // default: none, always writable
  readOnlyDbPath: ':memory:', // database used while read-only
  whenReadOnly: 'memory', // or 'throw'
  roleCheckInterval: 10_000, // ms between writable() checks

  // How long a claimed task stays reserved for this queue, renewed every
  // leaseMs / 3 while it runs (default: 60_000)
  leaseMs: 60_000,
});
```

### Persistence

The default database is in memory (`':memory:'`), so nothing is written to disk and tasks disappear when the process exits. Pass a file path as `dbPath` to keep tasks across restarts. File databases use WAL mode.

### Leases and interrupted tasks

When a queue starts a task it claims it: in one atomic `UPDATE ... RETURNING` statement it marks the task `processing` and records a lease with its instance id (`queue.instanceId`, `${hostname}:${pid}:${random}`) and an expiry `leaseMs` in the future (default 60 s). While the handler runs, a heartbeat renews the lease every `leaseMs / 3`. Completing or failing the task clears the lease.

This is what makes it safe to run several processes, or several `Queue` instances, on one database file:

- Two queues never claim the same task, because the claim is a single statement.
- A task whose lease has expired counts as abandoned: its worker crashed, was killed, or hung without renewing. Any queue with a handler for that job claims it again, with no restart needed. The queue schedules a wake-up for the moment another worker's lease runs out.
- If a worker finishes a task after another worker took it over, its result isn't saved. It logs a warning and emits no event for that task.

With `recoverInterrupted: true` (the default), opening a file database also sets tasks left in `processing` back to `pending`, but only if their lease is missing or expired. A task with a live lease belongs to another worker that is still running it, and it's left alone. Rows from files written by older versions have no lease and are restarted. The count is logged through `logger.info`. Set `recoverInterrupted: false` to skip this step. Expired tasks are still claimed during normal processing.

#### Delivery is at least once

A task can run more than once:

- Its worker stopped halfway (a crash, deploy or restart), so it runs again from the start.
- Its lease expired while the original worker was still running it, for example because the event loop was blocked for longer than `leaseMs`. Another worker then starts it while the first is still busy.

Handlers should be safe to repeat. Choose a `leaseMs` well above your longest event-loop stall.

### Read-only replicas

Some deployments have one writable primary and read-only replicas sharing a replicated database file, for example LiteFS on Fly.io. Pass a `writable` function and the queue follows the node's role:

- At construction the queue opens `dbPath` if `writable()` returns true, and `readOnlyDbPath` (default `':memory:'`) otherwise. If `writable()` throws at construction, the queue starts read-only and logs the error.
- Every `roleCheckInterval` ms (default 10 s) it calls `writable()` again. The timer doesn't keep the process alive and stops on `close()`.
- When the node becomes writable, the queue switches to `dbPath` and moves its open tasks (from the in-memory database) into the file.
- When the node becomes read-only, the queue switches to `readOnlyDbPath` and moves nothing. The file's tasks stay there for the new primary.
- After a switch caused by a role change, the queue emits `role-change` with `{ writable }`.
- If `writable()` throws during a check, the error is logged and the current role is kept.

`whenReadOnly` decides what `job.add()` does while the node is read-only:

- `'memory'` (default): store the task in `readOnlyDbPath` and process it there. It moves into the file if the node later becomes writable. If the process stops first, the task is lost.
- `'throw'`: throw a `QueueReadOnlyError` (exported) and store nothing.

`queue.writable` and `status.writable` report the current role, and `status.dbPath` the database in use.

#### Built-in `writable` checks

**`litefsWritable(dir = process.env.LITEFS_DIR)`** reads LiteFS's `.primary` file. LiteFS writes `<dir>/.primary` only on replicas, containing the primary's hostname. The check returns true when the file can't be read (this node is the primary) or when its trimmed contents equal `os.hostname()`. This is the same rule `litefs-js` uses. When `dir` is unset, the check always returns true, so local development and tests work unchanged.

**`sqliteWritable(dbPath)`** probes the file itself. On every call it opens a short-lived connection and creates and drops a scratch table inside a savepoint that is rolled back. It returns false when SQLite answers `SQLITE_READONLY` or `SQLITE_CANTOPEN`. Things to know:

- It takes the write lock briefly on every check. With another writer active it waits up to its own `busyTimeout` (default 1000 ms, `sqliteWritable(dbPath, { busyTimeout })`) and throws on `SQLITE_BUSY`, which keeps the current role.
- A missing file is created if its directory is writable.
- It does a real write because `BEGIN IMMEDIATE` alone succeeds on read-only files.
- It has **not** been verified against a LiteFS replica. LiteFS users should use `litefsWritable`.
- Neither check uses file permission bits. On FUSE mounts such as LiteFS, they don't reflect whether writes are refused.

#### `switchDatabase(dbPath, { moveOpenTasks, recoverInterrupted })`

The role check uses `switchDatabase`, and you can call it directly:

```javascript
await queue.switchDatabase('/data/queue.db', {
  moveOpenTasks: true, // default
  recoverInterrupted: true, // default
});
```

1. It pauses the queue and waits for `whenIdle()`, because a running task writes its result back to the database it came from.
2. It opens the new database and, with `recoverInterrupted`, restarts its interrupted tasks.
3. With `moveOpenTasks`, it copies open tasks from the old database: `pending`, `processing` (inserted as `pending`) and `failed` with a scheduled retry. It keeps `job_name`, `task_data`, `retry_count`, `next_retry_at` and `created_at`, and preserves their order. Copied tasks get new ids. The old database keeps its rows.
4. It closes the old connection, swaps, and resumes, unless the queue was already paused before the call.
5. It emits `database-switched` with `{ from, to, moved, recovered }`.

Concurrent calls run one after another. Calling it after `close()` rejects.

### LiteFS

This setup runs litequ on Fly.io with LiteFS. There is one writable primary and several read-only replicas, and the queue file sits on the replicated mount:

```js
import Queue, { litefsWritable } from 'litequ';

const queue = Queue.shared({
  dbPath: process.env.QUEUE_DATABASE_PATH,
  writable: litefsWritable(process.env.LITEFS_DIR),
  whenReadOnly: 'memory',
  busyTimeout: 5000,
  logger,
});

queue.defineJob('verify_email', {
  handler: sendVerificationEmail,
  onFailed: (event) => report(event),
});

process.once('SIGINT', () => queue.close({ timeout: 4000 }));
```

What each piece does:

- **`Queue.shared`** gives every bundle in the server build the same queue, so the file has one connection and one worker loop per process.
- **`litefsWritable`** checks LiteFS's `.primary` file every `roleCheckInterval` (10 s).
  - The primary works on the file.
  - A replica keeps new tasks in memory and processes them there.
  - When a replica is promoted, its open tasks move into the file.
  - When the primary is demoted, its file tasks stay for the new primary.
  - Without `LITEFS_DIR` (local development, tests), the queue is always writable.
- **`defineJob`** can run once per bundle without stacking duplicate `onFailed` listeners.
- **`close({ timeout })`** lets running tasks finish before the machine stops. A task that is still running when the timeout hits keeps its lease. It's picked up again after the lease expires or when the next process opens the file.

### Logging

litequ never writes to `console` directly. Pass any object with `error`, `warn` and `info` methods, such as a pino or winston logger. Methods are called on your object, so loggers that depend on `this` work. A missing method falls back to the matching `console` method.

```javascript
const queue = new Queue({ dbPath: './queue.db', logger: pinoLogger });
```

If nothing listens for the queue's `error` event, errors from background processing are logged through `logger.error` instead of being thrown.

### Schema versions

litequ stores its schema version in SQLite's `PRAGMA user_version` and migrates older files the first time it opens them. Files created by `@sturmfrei/litequu` 2.x or `litequ` 1.x are upgraded in place and keep their tasks.

## API Reference

### Queue Methods

#### `createJob(name)`

Create a named job for a specific type of work. Jobs have their own handlers and emit their own events.

```javascript
const emailJob = queue.createJob('email');
const smsJob = queue.createJob('sms');

// Each job can have its own handler
await emailJob.process(async (taskData) => {
  // Process email tasks
});

await smsJob.process(async (taskData) => {
  // Process SMS tasks
});
```

#### `defineJob(name, { handler, onCompleted, onFailed, onRetried })`

Create or update a job in one call. Calling `defineJob` again for the same name **replaces** the handler and the callbacks the previous call registered, instead of adding more. That makes it safe to run the same definition from several copies of a module, for example one per server bundle. Listeners you add with plain `job.on(...)` are left alone. Callbacks receive the job-level event payload plus `jobName`. Returns the `Job`.

```javascript
const job = queue.defineJob('verify_email', {
  handler: sendVerificationEmail,
  onCompleted: (event) => log(event.jobName, event.taskId),
  onFailed: (event) => report(event),
  onRetried: (event) => log(`retry ${event.retryCount}`),
});
```

#### `Queue.shared(options)`

Return one queue per key for the whole process, creating it on the first call. The key is `options.key` if given, otherwise the resolved `dbPath`. An in-memory database needs an explicit `key`.

```javascript
const queue = Queue.shared({ dbPath: '/data/queue.db' });
```

- The registry lives on `globalThis[Symbol.for('litequ.registry')]`. Every copy of litequ loaded in the process uses the same one, even copies bundled separately, so they all get the same instance. That means one connection and one worker loop.
- If a later call passes different options for the same key, the existing instance is returned and a warning is logged. Only primitive values are compared. Functions and objects such as `writable` and `logger` are not, because each module copy creates its own.
- Closing a shared queue removes it from the registry. The next `Queue.shared()` call creates a new one.

#### `processOnce()`

Process every task that is ready now, for all jobs with a registered handler, and resolve with the number of tasks attempted. Tasks run in batches of up to `maxConcurrent` until none are ready. Retries that aren't due yet are left for a later call. This is the way to process tasks when `autoProcess` is `false`, for example from a cron job:

```javascript
const queue = new Queue({ autoProcess: false });
const emailJob = queue.createJob('email');

await emailJob.process(async (taskData) => {
  await sendEmail(taskData);
});

const processed = await queue.processOnce();
console.log(`Processed ${processed} tasks`);
```

> Migrating from `@sturmfrei/litequu`: `processOnce()` no longer takes a handler and throws if one is passed. Register handlers with `createJob(name).process(handler)` instead.

#### `getStats()`

Get queue statistics grouped by job name and status.

```javascript
const stats = queue.getStats();
// Returns: [
//   { job_name: 'email', status: 'pending', count: 5 },
//   { job_name: 'email', status: 'completed', count: 10 },
//   { job_name: 'sms', status: 'pending', count: 3 }
// ]
```

#### `getTask(id)`

Get a specific task by ID.

```javascript
const task = queue.getTask(123);
console.log(task.job_name, task.status, task.retry_count);
```

#### `cleanup(olderThanHours)`

Remove completed tasks older than specified hours.

```javascript
await queue.cleanup(24); // Remove completed tasks older than 24 hours
```

#### `pause()`

Stop starting new batches and clear the retry wake-up timer. Tasks that are already running finish normally. `job.add()` still stores tasks while the queue is paused, and `processOnce()` does nothing.

```javascript
queue.pause();
```

#### `resume()`

Clear a pause. With `autoProcess` enabled, processing starts right away, including tasks added while the queue was paused.

```javascript
queue.resume();
```

#### `whenIdle()`

Returns a promise that resolves once no batch or task is running and no follow-up batch is scheduled. It resolves immediately if the queue is already idle. Combine it with `pause()` to wait for the queue to settle:

```javascript
queue.pause();
await queue.whenIdle(); // running tasks have finished; nothing new starts
```

#### `close({ timeout })`

Close the queue: pause it, stop its timers, wait for running tasks to finish, then close the database connection.

- `timeout` (ms, default: no limit) caps the wait. If it runs out, the connection is closed anyway and a warning names how many tasks were still running. Those tasks stay `processing` in the database and are restarted later (see [Leases and interrupted tasks](#leases-and-interrupted-tasks)).
- Calling `close()` again returns the same promise.
- After `close()`, `job.add()` throws.

```javascript
await queue.close({ timeout: 4000 });
```

### Job Methods

#### `job.add(taskData, { dedupeKey, throttleMs })`

Add a task to a specific job. Returns the new task's id.

```javascript
const emailJob = queue.createJob('email');
const taskId = emailJob.add({
  to: 'user@example.com',
  subject: 'Welcome!',
});
```

To throttle, pass both `dedupeKey` (a string) and `throttleMs`. If a task of the same job with the same `dedupeKey` was created within the last `throttleMs` milliseconds, the new task is dropped, `add()` returns `null`, and no `added` event fires.

```javascript
// At most one digest per user per hour
const id = digestJob.add(
  { userId },
  { dedupeKey: `user:${userId}`, throttleMs: 60 * 60 * 1000 }
);
if (id === null) {
  // throttled
}
```

- The check runs against the database, in the same write transaction as the insert. Throttling therefore survives restarts and holds across processes that share the file.
- Tasks of any status count, including completed ones. Throttling windows longer than your `cleanup()` age can't see tasks that were already deleted.
- `dedupeKey` alone just stores the key, which later throttled adds compare against. `throttleMs` alone is ignored.
- Timestamps have millisecond precision, so windows under a second work.

#### `job.process(handler)`

Register a handler function for processing tasks in this job.

```javascript
await emailJob.process(async (taskData) => {
  // Process email task
  await sendEmail(taskData.to, taskData.subject);
  return 'Email sent';
});
```

### Properties

#### `status`

Get current queue status, including information about registered jobs.

```javascript
const status = queue.status;
console.log(status.currentRunning); // Currently processing tasks
console.log(status.maxConcurrent); // Maximum concurrent tasks
console.log(status.isProcessing); // Whether queue is actively processing
console.log(status.paused); // Whether pause() is in effect
console.log(status.closed); // Whether close() has been called
console.log(status.writable); // Whether this process may write to dbPath
console.log(status.dbPath); // The database currently in use
console.log(status.jobs); // Object with job names and their handler status
```

### Events

Events can be listened to at two levels:

1. **Job-level events** - Specific to a single job (no `jobName` in payload)
2. **Queue-level events** - All events from all jobs (includes `jobName` in payload)

#### Job-Level Events

Listen to events from a specific job:

```javascript
const emailJob = queue.createJob('email');

emailJob.on('added', (info) => {
  // No jobName in payload - this is job-specific
  console.log(`Task ${info.taskId} added:`, info.taskData);
});

emailJob.on('completed', (info) => {
  console.log(`Task ${info.taskId} completed:`, info.result);
});

emailJob.on('retried', (info) => {
  console.log(
    `Task ${info.taskId} retry ${info.retryCount} in ${info.delay}ms`
  );
  console.log(`Error: ${info.error}`);
});

emailJob.on('failed', (info) => {
  console.log(`Task ${info.taskId} permanently failed:`, info.error);
  console.log(`Total attempts: ${info.retryCount}`);
});
```

#### Queue-Level Events

Listen to events from all jobs at the queue level. Queue-level events include the `jobName` field:

```javascript
// Listen to all completed tasks across all jobs
queue.on('completed', (info) => {
  console.log(`[${info.jobName}] Task ${info.taskId} completed:`, info.result);
});

// Listen to all failures across all jobs
queue.on('failed', (info) => {
  console.log(`[${info.jobName}] Task ${info.taskId} failed:`, info.error);

  // Handle different jobs differently
  if (info.jobName === 'critical-job') {
    sendAlert(info);
  }
});

// Listen to all retries
queue.on('retried', (info) => {
  console.log(`[${info.jobName}] Task ${info.taskId} retry ${info.retryCount}`);
});

// Error events (queue operations)
queue.on('error', (info) => {
  console.error(`Queue error in ${info.operation}:`, info.error);
});

// switchDatabase() swapped the connection
queue.on('database-switched', ({ from, to, moved, recovered }) => {
  console.log(
    `Switched ${from} -> ${to}, moved ${moved}, restarted ${recovered}`
  );
});

// The writable role changed and the queue switched databases
queue.on('role-change', ({ writable }) => {
  console.log(writable ? 'Now the writer' : 'Now read-only');
});

// The queue has finished its work: no batch or task is running.
// Fires only if at least one task ran since the last 'idle'.
queue.on('idle', () => {
  console.log('Queue is idle');
});
```

#### Event Payload Differences

**Job-level events:**

```javascript
{
  taskId: 123,
  taskData: { ... },
  result: 'success'
  // No jobName
}
```

**Queue-level events:**

```javascript
{
  jobName: 'email',  // <-- Added at queue level
  taskId: 123,
  taskData: { ... },
  result: 'success'
}
```

## Retry Mechanism

Tasks that fail are automatically retried with exponential backoff. The delay is roughly calculated as follows:

| Attempt | Next backoff                 | Total wait                        |
| ------- | ---------------------------- | --------------------------------- |
| 1       | 0d 0h 0m 7.5s – 0d 0h 0m 15s | 0d 0h 0m 7.5s – 0d 0h 0m 15s      |
| 2       | 0d 0h 0m 15s – 0d 0h 0m 30s  | 0d 0h 0m 22.5s – 0d 0h 0m 45s     |
| 3       | 0d 0h 0m 30s – 0d 0h 1m 0s   | 0d 0h 0m 52.5s – 0d 0h 1m 45s     |
| 4       | 0d 0h 1m 0s – 0d 0h 2m 0s    | 0d 0h 1m 52.5s – 0d 0h 3m 45s     |
| 5       | 0d 0h 2m 0s – 0d 0h 4m 0s    | 0d 0h 3m 52.5s – 0d 0h 7m 45s     |
| 6       | 0d 0h 4m 0s – 0d 0h 8m 0s    | 0d 0h 7m 52.5s – 0d 0h 15m 45s    |
| 7       | 0d 0h 8m 0s – 0d 0h 16m 0s   | 0d 0h 15m 52.5s – 0d 0h 31m 45s   |
| 8       | 0d 0h 16m 0s – 0d 0h 32m 0s  | 0d 0h 31m 52.5s – 0d 1h 3m 45s    |
| 9       | 0d 0h 32m 0s – 0d 1h 4m 0s   | 0d 1h 3m 52.5s – 0d 2h 7m 45s     |
| 10      | 0d 1h 4m 0s – 0d 2h 8m 0s    | 0d 2h 7m 52.5s – 0d 4h 15m 45s    |
| 11      | 0d 2h 8m 0s – 0d 4h 16m 0s   | 0d 4h 15m 52.5s – 0d 8h 31m 45s   |
| 12      | 0d 4h 16m 0s – 0d 8h 32m 0s  | 0d 8h 31m 52.5s – 0d 17h 3m 45s   |
| 13      | 0d 8h 32m 0s – 0d 17h 4m 0s  | 0d 17h 3m 52.5s – 1d 10h 7m 45s   |
| 14      | 0d 17h 4m 0s – 1d 10h 8m 0s  | 1d 10h 7m 52.5s – 2d 20h 15m 45s  |
| 15      | 1d 10h 8m 0s – 2d 20h 16m 0s | 2d 20h 15m 52.5s – 5d 16h 31m 45s |

The formula for the delay is:

`floor(baseRetryDelay * 2^(retryCount - 1) * (jitter ? (0.5 + Math.random() * 0.5) : 1))`.

With jitter enabled (default), actual delays will vary by ±50% to prevent thundering herd effects.

## Examples

### Multi-Service Background Jobs

```javascript
import Queue from 'litequ';

const queue = new Queue({
  dbPath: './jobs.db',
  maxConcurrent: 5,
  autoProcess: true,
});

// Create jobs for different services
const emailJob = queue.createJob('email');
const imageJob = queue.createJob('image-processing');
const backupJob = queue.createJob('backup');

// Set up handlers
await emailJob.process(async (task) => {
  await sendEmail(task.to, task.subject, task.body);
  return `Email sent to ${task.to}`;
});

await imageJob.process(async (task) => {
  const resized = await resizeImage(task.imageUrl, task.dimensions);
  await uploadToS3(resized, task.destination);
  return `Image processed: ${task.imageUrl}`;
});

await backupJob.process(async (task) => {
  await backupDatabase(task.database);
  return `Backup completed for ${task.database}`;
});

// Add tasks - they'll be processed automatically
emailJob.add({
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Thanks for signing up',
});

imageJob.add({
  imageUrl: 'https://example.com/photo.jpg',
  dimensions: { width: 800, height: 600 },
  destination: 's3://bucket/photos/thumb.jpg',
});

backupJob.add({
  database: 'production',
});

// Monitor specific job types
imageJob.on('failed', (info) => {
  console.error(`Image processing failed:`, info.error);
  // Could re-queue with different parameters or alert admins
});
```

### Event-Driven Auto-Processing

```javascript
const queue = new Queue({
  autoProcess: true,
});

const workJob = queue.createJob('work');

// Registering the job handler enables automatic processing
await workJob.process(async (task) => {
  return await handleTask(task);
});

// Adding a task wakes the queue immediately. Failed tasks schedule a
// one-shot wake-up for their next retry rather than using interval polling.
workJob.add({ work: 'to_do' });
```

### Error Handling and Retries

```javascript
const queue = new Queue({
  maxRetries: 3,
  baseRetryDelay: 1000,
});
const unreliableJob = queue.createJob('unreliable');

queue.on('retried', (info) => {
  console.log(
    `[${info.jobName}] Retry ${info.retryCount} for task ${info.taskId}`
  );
});

queue.on('failed', (info) => {
  console.log(
    `[${info.jobName}] Task ${info.taskId} gave up after ${info.retryCount} attempts`
  );
  // Handle permanent failures (e.g., dead letter queue, alerting)
});

await unreliableJob.process(async (task) => {
  // This might fail and trigger retries
  if (Math.random() < 0.5) {
    throw new Error('Simulated failure');
  }
  return 'success';
});

unreliableJob.add({ work: 'to_do' });
```

## Best Practices

### 1. Keep Tasks Lightweight

Since tasks run in the main thread, avoid CPU-intensive operations:

```javascript
const emailJob = queue.createJob('email');

// ✅ Good - I/O bound tasks
await emailJob.process(async (task) => {
  await sendEmail(task.email);
  await uploadFile(task.filePath);
  await callWebhook(task.url);
});

// ❌ Avoid - CPU intensive tasks
const reportJob = queue.createJob('report');
await reportJob.process(async (task) => {
  // This will block the event loop
  return heavyComputation(task.data);
});
```

### 2. Handle Errors Gracefully

```javascript
const importJob = queue.createJob('import');

await importJob.process(async (task) => {
  try {
    return await processTask(task);
  } catch (error) {
    // Add context to errors for better debugging
    throw new Error(`Failed to process ${task.type}: ${error.message}`);
  }
});
```

### 3. Use Jobs for Organization

Instead of a single handler with switches, use named jobs:

```javascript
// ✅ Good - separate jobs for different task types
const emailJob = queue.createJob('email');
const webhookJob = queue.createJob('webhook');
const uploadJob = queue.createJob('file-upload');

await emailJob.process(async (task) => sendEmail(task));
await webhookJob.process(async (task) => callWebhook(task));
await uploadJob.process(async (task) => uploadFile(task));
```

### 4. Monitor Queue Health

```javascript
// Set up monitoring with job-specific metrics
setInterval(() => {
  const stats = queue.getStats();

  // Group stats by job
  const statsByJob = {};
  stats.forEach((stat) => {
    if (!statsByJob[stat.job_name]) {
      statsByJob[stat.job_name] = { pending: 0, failed: 0, completed: 0 };
    }
    statsByJob[stat.job_name][stat.status] = stat.count;
  });

  // Check each job's health
  Object.entries(statsByJob).forEach(([jobName, jobStats]) => {
    if (jobStats.pending > 1000) {
      console.warn(`[${jobName}] Backlog growing:`, jobStats.pending);
    }
    if (jobStats.failed > 100) {
      console.error(`[${jobName}] High failure rate:`, jobStats.failed);
    }
  });
}, 60000); // Check every minute
```

### 5. Graceful Shutdown

```javascript
process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  // Wait up to 4 s for current tasks, then close anyway
  await queue.close({ timeout: 4000 });
  process.exit(0);
});
```

## Limitations

- **Same Machine**: Several processes can share one database file (see [Leases](#leases-and-interrupted-tasks)), but it must be on a filesystem where SQLite locking works, such as a local disk or LiteFS. Network filesystems like NFS aren't supported.
- **At-Least-Once**: A task can run more than once (see [Delivery is at least once](#delivery-is-at-least-once))
- **Main Thread**: Not suitable for CPU-intensive tasks
- **SQLite Concurrency**: Write operations are serialized by SQLite
- **Memory Usage**: Large task payloads are stored in the database

## TypeScript

The package ships declaration files (`types/`), generated from the JSDoc in `src/` when the package is packed. They work in TypeScript projects and in JavaScript projects with `checkJs`. `Queue`, `Job`, `Database`, `QueueReadOnlyError`, `litefsWritable`, `sqliteWritable` and the `QueueOptions` type are exported. The declarations need `@types/node`, but not better-sqlite3's types.

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests for any improvements.

## License

MIT License - see LICENSE file for details.
