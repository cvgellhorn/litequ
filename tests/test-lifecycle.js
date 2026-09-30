import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';
import { openRaw } from './helpers/legacy.js';
import { deferred, flush } from './helpers/async.js';

describe('Pause, resume, idle and close', () => {
  const tmp = createTempDirs();
  let queue;
  let logger;
  let file;

  beforeEach(() => {
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
    file = tmp.dbFile();
    queue = new Queue({ dbPath: file, maxConcurrent: 1, logger });
  });

  afterEach(async () => {
    vi.useRealTimers();
    await queue.close();
    tmp.cleanup();
  });

  /**
   * Registers a handler whose tasks finish only when the test releases them.
   * @returns {Promise<{ job: any, started: Array<any>, release: (data: any) => void }>}
   */
  async function controlledJob(name = 'work') {
    const job = queue.createJob(name);
    const gates = new Map();
    const started = [];
    const gate = (data) => {
      if (!gates.has(data.n)) gates.set(data.n, deferred());
      return gates.get(data.n);
    };
    await job.process(async (data) => {
      started.push(data.n);
      await gate(data).promise;
      return data.n;
    });
    return { job, started, release: (n) => gate({ n }).resolve() };
  }

  function rawStatus(id) {
    const raw = openRaw(file);
    try {
      return raw.prepare('SELECT status FROM queue WHERE id = ?').get(id)
        .status;
    } finally {
      raw.close();
    }
  }

  describe('pause()', () => {
    it('should stop new work but let running work finish', async () => {
      const { job, started, release } = await controlledJob();
      const first = job.add({ n: 1 });
      await flush();
      expect(started).toEqual([1]);

      queue.pause();
      const second = job.add({ n: 2 });
      release(1);
      await queue.whenIdle();
      await flush();

      expect(queue.getTask(first).status).toBe('completed');
      expect(queue.getTask(second).status).toBe('pending');
      expect(started).toEqual([1]);
    });

    it('should still insert tasks while paused', async () => {
      const { job, started } = await controlledJob();
      queue.pause();

      const id = job.add({ n: 1 });
      await flush();

      expect(queue.getTask(id).status).toBe('pending');
      expect(started).toEqual([]);
    });

    it('should make processOnce() a no-op while paused', async () => {
      const { job } = await controlledJob();
      queue.pause();
      job.add({ n: 1 });

      await expect(queue.processOnce()).resolves.toBe(0);
    });
  });

  describe('resume()', () => {
    it('should process tasks added while paused right away', async () => {
      const { job, started, release } = await controlledJob();
      queue.pause();
      const ids = [job.add({ n: 1 }), job.add({ n: 2 })];
      await flush();
      expect(started).toEqual([]);

      queue.resume();
      expect(queue.status.isProcessing).toBe(true);
      release(1);
      release(2);
      await vi.waitFor(() => expect(started).toEqual([1, 2]));
      await queue.whenIdle();

      for (const id of ids) {
        expect(queue.getTask(id).status).toBe('completed');
      }
    });
  });

  describe('whenIdle()', () => {
    it('should resolve immediately when nothing is running', async () => {
      await expect(queue.whenIdle()).resolves.toBeUndefined();
    });

    it('should resolve once the running task has finished', async () => {
      const { job, release } = await controlledJob();
      const id = job.add({ n: 1 });
      await flush();

      let statusWhenIdle = null;
      const idle = queue.whenIdle().then(() => {
        statusWhenIdle = queue.getTask(id).status;
      });
      await flush();
      expect(statusWhenIdle).toBeNull();

      release(1);
      await idle;
      expect(statusWhenIdle).toBe('completed');
    });

    it('should wait for follow-up batches, not just the current one', async () => {
      const { job, started, release } = await controlledJob();
      job.add({ n: 1 });
      job.add({ n: 2 });
      await flush();
      expect(started).toEqual([1]);

      let idle = false;
      const done = queue.whenIdle().then(() => (idle = true));
      release(1);
      await vi.waitFor(() => expect(started).toEqual([1, 2]));
      expect(idle).toBe(false);

      release(2);
      await done;
      expect(idle).toBe(true);
    });

    it('should emit a public idle event after work finishes', async () => {
      const { job, release } = await controlledJob();
      const onIdle = vi.fn();
      queue.on('idle', onIdle);

      job.add({ n: 1 });
      await flush();
      expect(onIdle).not.toHaveBeenCalled();
      release(1);
      await queue.whenIdle();

      expect(onIdle).toHaveBeenCalledTimes(1);
    });

    it('should keep working when user code removes idle listeners', async () => {
      const { job, release } = await controlledJob();
      job.add({ n: 1 });
      await flush();

      const idle = queue.whenIdle();
      queue.removeAllListeners('idle');
      release(1);

      await expect(idle).resolves.toBeUndefined();
    });
  });

  describe('close()', () => {
    it('should wait for running tasks by default', async () => {
      const { job, release } = await controlledJob();
      const id = job.add({ n: 1 });
      await flush();

      let closed = false;
      const closing = queue.close().then(() => (closed = true));
      await flush();
      expect(closed).toBe(false);

      release(1);
      await closing;
      expect(rawStatus(id)).toBe('completed');
    });

    it('should give up after the timeout and log the running tasks', async () => {
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
      const { job, release } = await controlledJob();
      const id = job.add({ n: 1 });
      await flush();

      const closing = queue.close({ timeout: 1000 });
      await vi.advanceTimersByTimeAsync(1000);
      await closing;

      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringMatching(/timed out.*1 task/)
      );
      expect(queue.db.closed).toBe(true);
      expect(rawStatus(id)).toBe('processing');

      // The task finishing later must not reopen the database or throw.
      release(1);
      await flush();
      expect(queue.db.closed).toBe(true);
      expect(rawStatus(id)).toBe('processing');
    });

    it('should be idempotent', async () => {
      const first = queue.close();
      const second = queue.close();

      expect(second).toBe(first);
      await expect(second).resolves.toBeUndefined();
      await expect(queue.close()).resolves.toBeUndefined();
    });

    it('should make add() throw a clear error', async () => {
      const job = queue.createJob('late');
      await queue.close();

      expect(() => job.add({ n: 1 })).toThrow(/queue is closed/);
    });

    it('should stop the retry wake-up timer', async () => {
      const job = queue.createJob('flaky');
      await job.process(async () => {
        throw new Error('fail');
      });
      job.add({ n: 1 });
      await queue.whenIdle();
      await flush();
      expect(queue.pollingTimer).not.toBeNull();

      await queue.close();

      expect(queue.pollingTimer).toBeNull();
    });
  });

  describe('status', () => {
    it('should report paused and closed', async () => {
      expect(queue.status).toMatchObject({ paused: false, closed: false });

      queue.pause();
      expect(queue.status.paused).toBe(true);
      queue.resume();
      expect(queue.status.paused).toBe(false);

      await queue.close();
      expect(queue.status.closed).toBe(true);
    });
  });
});
