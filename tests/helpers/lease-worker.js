// Runs a Queue in a worker thread so two queues can race on one file for
// real; within one thread better-sqlite3 calls never interleave.
import { parentPort, workerData } from 'node:worker_threads';
import Queue from '../../src/queue.js';

const silent = { error() {}, warn() {}, info() {} };
const queue = new Queue({
  dbPath: workerData.file,
  autoProcess: false,
  maxConcurrent: workerData.maxConcurrent,
  logger: silent,
});
const handled = [];
await queue.createJob('work').process(async (data) => {
  handled.push(data.n);
  // Real handlers do async work, which frees the write lock between claims.
  // Without it one worker can hold the lock for the whole run, because
  // SQLite's busy handler doesn't take turns fairly.
  await new Promise((resolve) => setTimeout(resolve, 1));
});

parentPort.once('message', async () => {
  await queue.processOnce();
  await queue.close();
  parentPort.postMessage({ type: 'done', handled });
});
parentPort.postMessage({ type: 'ready' });
