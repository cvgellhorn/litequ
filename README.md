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
- 📦 **Zero Config**: Works out of the box with sensible defaults

## Installation

```bash
npm i litequ
```

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
  // Database file path (default: './queue.db')
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
});
```

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

#### `close()`

Close the queue and database connection.

```javascript
await queue.close();
```

### Job Methods

#### `job.add(taskData)`

Add a task to a specific job.

```javascript
const emailJob = queue.createJob('email');
const taskId = emailJob.add({
  to: 'user@example.com',
  subject: 'Welcome!',
});
```

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
  await queue.close(); // Wait for current tasks to finish
  process.exit(0);
});
```

## Limitations

- **Single Process**: Designed for single-process applications
- **Main Thread**: Not suitable for CPU-intensive tasks
- **SQLite Concurrency**: Write operations are serialized by SQLite
- **Memory Usage**: Large task payloads are stored in the database

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests for any improvements.

## License

MIT License - see LICENSE file for details.
