import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import os from 'node:os';
import { Worker } from 'node:worker_threads';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';
import { createLegacyDatabase, openRaw } from './helpers/legacy.js';
import { deferred, flush } from './helpers/async.js';

describe('Leases', () => {
  const tmp = createTempDirs();
  const queues = [];
  let logger;
  let file;

  /** @param {Object} [options] */
  function makeQueue(options) {
    const queue = new Queue({
      dbPath: file,
      autoProcess: false,
      logger,
      ...options,
    });
    queues.push(queue);
    return queue;
  }

  function readTask(id) {
    const raw = openRaw(file);
    try {
      return raw.prepare('SELECT * FROM queue WHERE id = ?').get(id);
    } finally {
      raw.close();
    }
  }

  function updateTask(id, fields) {
    const raw = openRaw(file);
    try {
      const sets = Object.keys(fields).map((key) => `${key} = @${key}`);
      raw
        .prepare(`UPDATE queue SET ${sets.join(', ')} WHERE id = @id`)
        .run({ ...fields, id });
    } finally {
      raw.close();
    }
  }

  const inSeconds = (seconds) =>
    new Date(Date.now() + seconds * 1000).toISOString();

  beforeEach(() => {
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
    file = tmp.dbFile();
  });

  afterEach(async () => {
    vi.useRealTimers();
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    tmp.cleanup();
  });

  it('should give each queue an instance id', () => {
    const first = makeQueue();
    const second = makeQueue();

    expect(first.instanceId).toMatch(
      new RegExp(`^${os.hostname()}:${process.pid}:[0-9a-f]+$`)
    );
    expect(second.instanceId).not.toBe(first.instanceId);
  });

  it('should hold a lease while a task runs and clear it afterwards', async () => {
    const queue = makeQueue({ leaseMs: 30_000 });
    const job = queue.createJob('work');
    const gate = deferred();
    await job.process(() => gate.promise);
    const id = job.add({ n: 1 });

    const processing = queue.processOnce();
    await flush();
    const running = readTask(id);
    expect(running.status).toBe('processing');
    expect(running.locked_by).toBe(queue.instanceId);
    expect(Date.parse(running.locked_until)).toBeGreaterThan(Date.now());

    gate.resolve();
    await processing;
    expect(readTask(id)).toMatchObject({
      status: 'completed',
      locked_by: null,
      locked_until: null,
    });
  });

  it('should clear the lease when a task fails', async () => {
    const queue = makeQueue({ baseRetryDelay: 60_000 });
    const job = queue.createJob('work');
    await job.process(async () => {
      throw new Error('fail');
    });
    const id = job.add({ n: 1 });

    await queue.processOnce();

    expect(readTask(id)).toMatchObject({
      status: 'failed',
      locked_by: null,
      locked_until: null,
    });
  });

  it('should never run the same task in two processes on one file', async () => {
    const setup = makeQueue();
    const job = setup.createJob('work');
    const total = 300;
    for (let n = 0; n < total; n++) {
      job.add({ n });
    }
    await setup.close();

    const workers = [0, 1].map(
      () =>
        new Worker(new URL('./helpers/lease-worker.js', import.meta.url), {
          workerData: { file, maxConcurrent: 1 },
        })
    );
    const next = (worker, type) =>
      new Promise((resolve, reject) => {
        worker.on('error', reject);
        worker.on('message', (message) => {
          if (message.type === type) resolve(message);
        });
      });
    await Promise.all(workers.map((worker) => next(worker, 'ready')));
    const results = workers.map((worker) => next(worker, 'done'));
    workers.forEach((worker) => worker.postMessage('start'));
    const handled = (await Promise.all(results)).map((r) => r.handled);
    await Promise.all(workers.map((worker) => worker.terminate()));

    const all = handled.flat().sort((a, b) => a - b);
    expect(all).toEqual([...Array(total).keys()]);
    expect(handled.every((ids) => ids.length > 0)).toBe(true);
  });

  it('should pick up a task whose lease expired, without a restart', async () => {
    createLegacyDatabase(file, [
      { job_name: 'work', task_data: '{"n":1}', status: 'processing' },
    ]);
    const queue = makeQueue({ recoverInterrupted: false });
    updateTask(1, { locked_by: 'crashed:1:abc', locked_until: inSeconds(-1) });
    const handled = [];
    await queue.createJob('work').process(async (data) => handled.push(data.n));

    await expect(queue.processOnce()).resolves.toBe(1);

    expect(handled).toEqual([1]);
    expect(readTask(1)).toMatchObject({ status: 'completed', locked_by: null });
  });

  it('should not restart a task with a live lease when a new instance opens', async () => {
    createLegacyDatabase(file, [
      { job_name: 'work', task_data: '{"n":1}', status: 'processing' },
    ]);
    const lease = { locked_by: 'other:1:abc', locked_until: inSeconds(60) };
    // Stamp the lease through a first instance, which also migrates the file.
    makeQueue({ recoverInterrupted: false });
    updateTask(1, lease);

    const queue = makeQueue();
    const handled = [];
    await queue.createJob('work').process(async (data) => handled.push(data.n));

    await expect(queue.processOnce()).resolves.toBe(0);
    expect(handled).toEqual([]);
    expect(readTask(1)).toMatchObject({ status: 'processing', ...lease });
    expect(logger.info).not.toHaveBeenCalled();
  });

  it('should restart a task with an expired lease when a new instance opens', () => {
    createLegacyDatabase(file, [
      { job_name: 'work', task_data: '{"n":1}', status: 'processing' },
    ]);
    makeQueue({ recoverInterrupted: false });
    updateTask(1, { locked_by: 'crashed:1:abc', locked_until: inSeconds(-1) });

    makeQueue();

    expect(readTask(1)).toMatchObject({
      status: 'pending',
      locked_by: null,
      locked_until: null,
    });
  });

  it('should wake up and take over a task when its lease expires', async () => {
    vi.useFakeTimers({
      toFake: ['setTimeout', 'clearTimeout', 'Date'],
    });
    createLegacyDatabase(file, [
      { job_name: 'work', task_data: '{"n":1}', status: 'processing' },
    ]);
    makeQueue({ recoverInterrupted: false });
    updateTask(1, { locked_by: 'crashed:1:abc', locked_until: inSeconds(5) });
    const queue = makeQueue({ autoProcess: true });
    const handled = [];
    await queue.createJob('work').process(async (data) => handled.push(data.n));
    await flush();
    expect(handled).toEqual([]);

    await vi.advanceTimersByTimeAsync(5000);
    await flush();
    await queue.whenIdle();

    expect(handled).toEqual([1]);
    expect(readTask(1).status).toBe('completed');
  });

  it('should not schedule wake-ups for jobs without a handler', async () => {
    createLegacyDatabase(file, [
      {
        job_name: 'orphan',
        task_data: '{}',
        status: 'failed',
        retry_count: 1,
        next_retry_at: inSeconds(-60),
      },
    ]);
    const queue = makeQueue({ autoProcess: true });
    await queue.createJob('work').process(async () => 'ok');
    await flush();
    await queue.whenIdle();

    expect(queue.pollingTimer).toBeNull();
  });

  it('should keep a long task leased with a heartbeat', async () => {
    vi.useFakeTimers({
      toFake: [
        'setTimeout',
        'clearTimeout',
        'setInterval',
        'clearInterval',
        'Date',
      ],
    });
    const worker = makeQueue({ leaseMs: 3000 });
    const job = worker.createJob('work');
    const gate = deferred();
    await job.process(() => gate.promise);
    const id = job.add({ n: 1 });

    const processing = worker.processOnce();
    await flush();
    const firstLease = Date.parse(readTask(id).locked_until);

    await vi.advanceTimersByTimeAsync(10_000);
    const laterLease = Date.parse(readTask(id).locked_until);
    expect(laterLease).toBeGreaterThan(firstLease);
    expect(laterLease).toBeGreaterThan(Date.now());

    // Another instance opening now must neither restart nor claim the task.
    const other = makeQueue();
    await other.createJob('work').process(async () => 'stolen');
    await expect(other.processOnce()).resolves.toBe(0);
    expect(readTask(id).locked_by).toBe(worker.instanceId);

    gate.resolve();
    await processing;
    expect(readTask(id).status).toBe('completed');
  });

  it('should not overwrite a task another worker took over after the lease expired', async () => {
    const queue = makeQueue();
    const job = queue.createJob('work');
    const gate = deferred();
    await job.process(() => gate.promise);
    const id = job.add({ n: 1 });

    const processing = queue.processOnce();
    await flush();
    updateTask(id, { locked_by: 'other:1:abc', locked_until: inSeconds(60) });
    gate.resolve('done');
    await processing;

    expect(readTask(id)).toMatchObject({
      status: 'processing',
      locked_by: 'other:1:abc',
    });
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('lease'));
  });
});
