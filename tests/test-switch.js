import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import { once } from 'node:events';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';
import { createLegacyDatabase, openRaw } from './helpers/legacy.js';
import { deferred, flush } from './helpers/async.js';

describe('switchDatabase()', () => {
  const tmp = createTempDirs();
  const queues = [];
  let logger;

  /** @param {Object} [options] */
  function makeQueue(options) {
    const queue = new Queue({ logger, ...options });
    queues.push(queue);
    return queue;
  }

  function readRows(file) {
    const raw = openRaw(file);
    try {
      return raw.prepare('SELECT * FROM queue ORDER BY id').all();
    } finally {
      raw.close();
    }
  }

  beforeEach(() => {
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
  });

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    tmp.cleanup();
  });

  it('should copy open tasks to the new database and report the switch', async () => {
    const queue = makeQueue();
    const retryAt = new Date(Date.now() + 60_000).toISOString();
    const insert = (status, data, extra = {}) =>
      queue.db.run(
        `INSERT INTO queue (job_name, task_data, status, retry_count, next_retry_at)
         VALUES (?, ?, ?, ?, ?)`,
        [
          'email',
          JSON.stringify(data),
          status,
          extra.retry_count ?? 0,
          extra.next_retry_at ?? null,
        ]
      );
    insert('pending', { n: 1 });
    insert('completed', { n: 2 });
    insert('processing', { n: 3 });
    insert('failed', { n: 4 }, { retry_count: 2, next_retry_at: retryAt });
    insert('failed', { n: 5 }, { retry_count: 15 });

    const file = tmp.dbFile();
    const switched = once(queue, 'database-switched');
    await queue.switchDatabase(file);
    const [event] = await switched;

    expect(event).toEqual({
      from: ':memory:',
      to: file,
      moved: 3,
      recovered: 0,
    });
    expect(queue.dbPath).toBe(file);
    const rows = readRows(file);
    expect(rows.map((row) => JSON.parse(row.task_data).n)).toEqual([1, 3, 4]);
    expect(rows.map((row) => row.status)).toEqual([
      'pending',
      'pending',
      'failed',
    ]);
    expect(rows[2]).toMatchObject({
      job_name: 'email',
      retry_count: 2,
      next_retry_at: retryAt,
    });
  });

  it('should restart interrupted tasks in the new database', async () => {
    const file = tmp.dbFile();
    createLegacyDatabase(file, [
      { job_name: 'email', task_data: '{}', status: 'processing' },
    ]);
    const queue = makeQueue();

    const switched = once(queue, 'database-switched');
    await queue.switchDatabase(file);

    expect((await switched)[0]).toMatchObject({ moved: 0, recovered: 1 });
    expect(queue.getTask(1).status).toBe('pending');
  });

  it('should stay paused when the queue was paused before the call', async () => {
    const queue = makeQueue();
    const job = queue.createJob('work');
    const handled = [];
    job.add({ n: 1 });
    queue.pause();
    const file = tmp.dbFile();

    await queue.switchDatabase(file);
    expect(queue.status.paused).toBe(true);
    queue.resume();
    await job.process(async (data) => handled.push(data.n));
    job.add({ n: 2 });
    await flush();
    await queue.whenIdle();

    expect(handled).toEqual([1, 2]);
    expect(readRows(file).map((row) => row.status)).toEqual([
      'completed',
      'completed',
    ]);
  });

  it('should resume automatically when the queue was not paused before', async () => {
    const queue = makeQueue();
    const job = queue.createJob('work');
    const handled = [];
    await job.process(async (data) => handled.push(data.n));

    await queue.switchDatabase(tmp.dbFile());
    expect(queue.status.paused).toBe(false);
    job.add({ n: 1 });
    await flush();
    await queue.whenIdle();

    expect(handled).toEqual([1]);
  });

  it('should wait for a running task before swapping', async () => {
    const queue = makeQueue();
    const job = queue.createJob('work');
    const gate = deferred();
    await job.process(async () => gate.promise);
    const oldDb = queue.db;
    const id = job.add({ n: 1 });
    await flush();
    expect(queue.status.currentRunning).toBe(1);

    const file = tmp.dbFile();
    let done = false;
    const switching = queue.switchDatabase(file).then(() => (done = true));
    await flush();
    expect(done).toBe(false);
    expect(queue.db).toBe(oldDb);

    gate.resolve('ok');
    await switching;

    expect(queue.dbPath).toBe(file);
    // The task finished in the old database, so it's no longer open.
    expect(readRows(file)).toHaveLength(0);
    expect(oldDb.closed).toBe(true);
    expect(oldDb.dbPath).toBe(':memory:');
    expect(id).toBeGreaterThan(0);
  });

  it('should not move tasks when moveOpenTasks is false', async () => {
    const queue = makeQueue();
    queue.createJob('work').add({ n: 1 });
    const file = tmp.dbFile();

    await queue.switchDatabase(file, { moveOpenTasks: false });

    expect(readRows(file)).toHaveLength(0);
  });

  it('should serialize concurrent calls', async () => {
    const queue = makeQueue();
    queue.createJob('work').add({ n: 1 });
    const first = tmp.dbFile('a.db');
    const second = tmp.dbFile('b.db');
    const events = [];
    queue.on('database-switched', (event) => events.push(event));

    await Promise.all([
      queue.switchDatabase(first),
      queue.switchDatabase(second),
    ]);

    expect(events.map((event) => [event.from, event.to, event.moved])).toEqual([
      [':memory:', first, 1],
      [first, second, 1],
    ]);
    expect(queue.dbPath).toBe(second);
  });

  it('should reject after the queue is closed', async () => {
    const queue = makeQueue();
    await queue.close();

    await expect(queue.switchDatabase(tmp.dbFile())).rejects.toThrow(/closed/);
  });
});
