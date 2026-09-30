import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import path from 'node:path';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';

const REGISTRY = Symbol.for('litequ.registry');

describe('Queue.shared()', () => {
  const tmp = createTempDirs();
  let logger;

  beforeEach(() => {
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
  });

  afterEach(async () => {
    const registry = globalThis[REGISTRY];
    await Promise.all([...(registry?.values() ?? [])].map((q) => q.close()));
    tmp.cleanup();
  });

  it('should return one instance per database file', () => {
    const file = tmp.dbFile();
    const relative = path.relative(process.cwd(), file);

    const first = Queue.shared({ dbPath: file, logger });
    const second = Queue.shared({ dbPath: relative, logger });

    expect(second).toBe(first);
    expect(globalThis[REGISTRY].get(path.resolve(file))).toBe(first);
  });

  it('should share instances across separately loaded copies of the module', async () => {
    const file = tmp.dbFile();
    vi.resetModules();
    const copyA = await import('../src/index.js');
    vi.resetModules();
    const copyB = await import('../src/index.js');
    expect(copyA.Queue).not.toBe(copyB.Queue);

    const fromA = copyA.Queue.shared({ dbPath: file, logger });
    const fromB = copyB.Queue.shared({ dbPath: file, logger });

    expect(fromB).toBe(fromA);
  });

  it('should require a key for an in-memory database', () => {
    expect(() => Queue.shared({ logger })).toThrow(/key/);
    expect(() => Queue.shared({ dbPath: ':memory:', logger })).toThrow(/key/);

    const first = Queue.shared({ key: 'jobs', logger });
    expect(Queue.shared({ key: 'jobs', logger })).toBe(first);
    expect(Queue.shared({ key: 'other', logger })).not.toBe(first);
  });

  it('should warn and return the existing instance when options differ', () => {
    const file = tmp.dbFile();
    const first = Queue.shared({ dbPath: file, maxConcurrent: 2, logger });

    const second = Queue.shared({ dbPath: file, maxConcurrent: 8, logger });

    expect(second).toBe(first);
    expect(second.maxConcurrent).toBe(2);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('maxConcurrent')
    );
  });

  it('should not compare function and object options', () => {
    const file = tmp.dbFile();
    Queue.shared({ dbPath: file, writable: () => true, logger });

    Queue.shared({
      dbPath: file,
      writable: () => true,
      logger: { ...logger },
    });

    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('should remove a closed queue from the registry', async () => {
    const file = tmp.dbFile();
    const first = Queue.shared({ dbPath: file, logger });

    await first.close();
    const second = Queue.shared({ dbPath: file, logger });

    expect(second).not.toBe(first);
    expect(second.status.closed).toBe(false);
  });
});

describe('queue.defineJob()', () => {
  let queue;

  beforeEach(() => {
    queue = new Queue({
      autoProcess: false,
      maxRetries: 1,
      baseRetryDelay: 1,
      jitter: false,
      logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
    });
  });

  afterEach(async () => {
    await queue.close();
  });

  /** Runs the queue until the task has failed permanently. */
  async function failPermanently() {
    await queue.processOnce();
    await new Promise((resolve) => setTimeout(resolve, 10));
    await queue.processOnce();
  }

  it('should fire onFailed once when defined twice', async () => {
    const onFailed = vi.fn();
    const define = () =>
      queue.defineJob('verify_email', {
        handler: async () => {
          throw new Error('smtp down');
        },
        onFailed,
      });
    define();
    const job = define();
    job.add({ to: 'a@example.com' });

    await failPermanently();

    expect(onFailed).toHaveBeenCalledTimes(1);
    expect(onFailed).toHaveBeenCalledWith(
      expect.objectContaining({
        jobName: 'verify_email',
        error: 'smtp down',
        taskData: { to: 'a@example.com' },
      })
    );
  });

  it('should replace the handler and the listeners it registered', async () => {
    const firstHandler = vi.fn(async () => 'first');
    const firstCompleted = vi.fn();
    const secondHandler = vi.fn(async () => 'second');
    const secondCompleted = vi.fn();
    queue.defineJob('work', {
      handler: firstHandler,
      onCompleted: firstCompleted,
    });
    const job = queue.defineJob('work', {
      handler: secondHandler,
      onCompleted: secondCompleted,
    });
    job.add({ n: 1 });

    await queue.processOnce();

    expect(firstHandler).not.toHaveBeenCalled();
    expect(firstCompleted).not.toHaveBeenCalled();
    expect(secondHandler).toHaveBeenCalledTimes(1);
    expect(secondCompleted).toHaveBeenCalledWith(
      expect.objectContaining({ jobName: 'work', result: 'second' })
    );
  });

  it('should remove a callback that a later definition leaves out', async () => {
    const onRetried = vi.fn();
    queue.defineJob('work', {
      handler: async () => {
        throw new Error('fail');
      },
      onRetried,
    });
    const job = queue.defineJob('work', {
      handler: async () => {
        throw new Error('fail');
      },
    });
    job.add({ n: 1 });

    await queue.processOnce();

    expect(onRetried).not.toHaveBeenCalled();
  });

  it('should leave listeners added with job.on() alone', async () => {
    const plain = vi.fn();
    const job = queue.createJob('work');
    job.on('failed', plain);

    queue.defineJob('work', {
      handler: async () => {
        throw new Error('fail');
      },
      onFailed: vi.fn(),
    });
    queue.defineJob('work', {
      handler: async () => {
        throw new Error('fail');
      },
      onFailed: vi.fn(),
    });
    job.add({ n: 1 });
    await failPermanently();

    expect(plain).toHaveBeenCalledTimes(1);
    expect(job.listenerCount('failed')).toBe(2);
  });

  it('should return the job, the same one createJob() returns', () => {
    const job = queue.defineJob('work', { handler: async () => 'ok' });

    expect(queue.createJob('work')).toBe(job);
    expect(job.handler).toBeTypeOf('function');
  });

  it('should reject callbacks that are not functions', () => {
    expect(() => queue.defineJob('work', { handler: 'nope' })).toThrow(
      TypeError
    );
    expect(() => queue.defineJob('work', { onFailed: 42 })).toThrow(TypeError);
  });
});
