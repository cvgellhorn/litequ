import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Queue from '../src/queue.js';

describe('Queue', () => {
  let queue;
  const testDbPath = ':memory:';

  beforeEach(() => {
    queue = new Queue({
      dbPath: testDbPath,
      autoProcess: false,
      maxRetries: 2,
      baseRetryDelay: 100,
    });
  });

  afterEach(async () => {
    await queue.close();
  });

  describe('constructor', () => {
    it('should create queue with default options', () => {
      const defaultQueue = new Queue();
      expect(defaultQueue.maxConcurrent).toBe(5);
      expect(defaultQueue.maxRetries).toBe(15);
      expect(defaultQueue.baseRetryDelay).toBe(15_000);
      defaultQueue.close();
    });

    it('should create queue with custom options', () => {
      const customQueue = new Queue({
        maxConcurrent: 10,
        maxRetries: 5,
        baseRetryDelay: 2000,
      });
      expect(customQueue.maxConcurrent).toBe(10);
      expect(customQueue.maxRetries).toBe(5);
      expect(customQueue.baseRetryDelay).toBe(2000);
      customQueue.close();
    });
  });

  describe('createJob / job.add', () => {
    it('should add a task and return task ID', () => {
      const job = queue.createJob('test');
      const taskId = job.add({ type: 'test', data: 123 });

      expect(taskId).toBeTypeOf('number');
      expect(taskId).toBeGreaterThan(0);
    });

    it('should emit added event on job and queue', async () => {
      const job = queue.createJob('test');
      const taskData = { type: 'test', data: 123 };

      const addedPromise = new Promise((resolve) => {
        queue.on('added', (info) => {
          expect(info.jobName).toBe('test');
          expect(info.taskId).toBeTypeOf('number');
          expect(info.taskData).toEqual(taskData);
          resolve();
        });
      });

      job.add(taskData);
      await addedPromise;
    });

    it('should handle complex task data', () => {
      const job = queue.createJob('test');
      const complexData = {
        user: { id: 1, name: 'John' },
        actions: ['create', 'update'],
        metadata: { timestamp: Date.now() },
      };

      const taskId = job.add(complexData);
      const task = queue.getTask(taskId);

      expect(JSON.parse(task.task_data)).toEqual(complexData);
    });
  });

  describe('job processing', () => {
    it('should process a single task successfully', async () => {
      const job = queue.createJob('calc');
      job.add({ value: 42 });

      const results = [];
      await job.process(async (data) => {
        results.push(data.value * 2);
        return data.value * 2;
      });
      await queue._processNextBatch();

      expect(results).toHaveLength(1);
      expect(results[0]).toBe(84);
    });

    it('should emit completed event on success with jobName', async () => {
      const job = queue.createJob('calc');
      const completedPromise = new Promise((resolve) => {
        queue.on('completed', (info) => {
          expect(info.jobName).toBe('calc');
          expect(info.taskId).toBeTypeOf('number');
          expect(info.result).toBe(20);
          expect(info.taskData).toEqual({ value: 10 });
          resolve();
        });
      });

      job.add({ value: 10 });
      await job.process(async (data) => data.value * 2);
      await queue._processNextBatch();
      await completedPromise;
    });

    it('should retry failed tasks', async () => {
      const job = queue.createJob('flaky');
      let attempts = 0;

      job.add({ shouldFail: true });
      await job.process(async () => {
        attempts++;
        if (attempts < 2) {
          throw new Error('temporary failure');
        }
        return 'ok';
      });

      await queue._processNextBatch();
      expect(attempts).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 150));
      await queue._processNextBatch();
      expect(attempts).toBe(2);
    });

    it('should emit failed after max retries', async () => {
      const job = queue.createJob('doomed');
      const failedPromise = new Promise((resolve) => {
        queue.on('failed', (info) => {
          expect(info.jobName).toBe('doomed');
          expect(info.error).toBeTruthy();
          resolve();
        });
      });

      job.add({ alwaysFail: true });
      await job.process(async () => {
        throw new Error('always fails');
      });

      for (let i = 0; i < 5; i++) {
        await queue._processNextBatch();
        await new Promise((resolve) => setTimeout(resolve, 120));
      }

      await failedPromise;
    });
  });

  describe('status / stats / cleanup', () => {
    it('should report job handler status', async () => {
      const job = queue.createJob('status');
      expect(queue.status.jobs.status?.hasHandler).toBeFalsy();

      await job.process(async () => 'ok');
      expect(queue.status.jobs.status.hasHandler).toBe(true);
      expect(queue.status.autoProcess).toBe(false);
    });

    it('should return stats and cleanup completed tasks', async () => {
      const job = queue.createJob('stats');
      job.add({ n: 1 });
      await job.process(async () => 'done');
      await queue._processNextBatch();

      const stats = queue.getStats();
      expect(Array.isArray(stats)).toBe(true);

      const result = queue.cleanup(0);
      expect(result).toBeTruthy();
    });
  });

  describe('processOnce', () => {
    it('should process ready tasks across jobs and resolve with the count', async () => {
      const emailJob = queue.createJob('email');
      const reportJob = queue.createJob('report');
      const handled = [];

      await emailJob.process(async (data) => handled.push(data.id));
      await reportJob.process(async (data) => handled.push(data.id));
      emailJob.add({ id: 1 });
      reportJob.add({ id: 2 });

      const processed = await queue.processOnce();

      expect(processed).toBe(2);
      expect(handled.sort()).toEqual([1, 2]);
    });

    it('should drain more tasks than maxConcurrent without exceeding it', async () => {
      const smallQueue = new Queue({
        dbPath: ':memory:',
        autoProcess: false,
        maxConcurrent: 2,
      });
      const job = smallQueue.createJob('bulk');
      let running = 0;
      let peak = 0;

      await job.process(async () => {
        running++;
        peak = Math.max(peak, running);
        await new Promise((resolve) => setTimeout(resolve, 5));
        running--;
      });
      const ids = [1, 2, 3, 4, 5].map((n) => job.add({ n }));

      const processed = await smallQueue.processOnce();

      expect(processed).toBe(5);
      expect(peak).toBeLessThanOrEqual(2);
      for (const id of ids) {
        expect(smallQueue.getTask(id).status).toBe('completed');
      }
      await smallQueue.close();
    });

    it('should leave tasks for jobs without a handler pending', async () => {
      const handledJob = queue.createJob('handled');
      const orphanJob = queue.createJob('orphan');
      await handledJob.process(async () => 'ok');
      const orphanId = orphanJob.add({ n: 1 });

      const processed = await queue.processOnce();

      expect(processed).toBe(0);
      expect(queue.getTask(orphanId).status).toBe('pending');
    });

    it('should not run retries before they are due', async () => {
      const job = queue.createJob('flaky');
      let attempts = 0;
      await job.process(async () => {
        attempts++;
        throw new Error('fail');
      });
      job.add({ n: 1 });

      expect(await queue.processOnce()).toBe(1);
      expect(await queue.processOnce()).toBe(0);
      expect(attempts).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(await queue.processOnce()).toBe(1);
      expect(attempts).toBe(2);
    });

    it('should not double-process tasks while auto-processing is running', async () => {
      const autoQueue = new Queue({ dbPath: ':memory:', maxConcurrent: 2 });
      const job = autoQueue.createJob('auto');
      const seen = new Map();

      await job.process(async (data) => {
        seen.set(data.n, (seen.get(data.n) || 0) + 1);
        await new Promise((resolve) => setTimeout(resolve, 10));
      });
      const ids = [1, 2, 3, 4].map((n) => job.add({ n }));

      await new Promise((resolve) => setImmediate(resolve));
      await autoQueue.processOnce();

      expect([...seen.values()].every((count) => count === 1)).toBe(true);
      for (const id of ids) {
        expect(autoQueue.getTask(id).status).toBe('completed');
      }
      await autoQueue.close();
    });

    it('should reject a legacy handler argument with a migration hint', async () => {
      await expect(queue.processOnce(async () => 'ok')).rejects.toThrow(
        /createJob\(name\)\.process\(handler\)/
      );
    });
  });

  describe('legacy API removed', () => {
    it('should not expose queue.add or queue.process', () => {
      expect(queue.add).toBeUndefined();
      expect(queue.process).toBeUndefined();
    });

    it('should not expose queue-level handler state', () => {
      expect(queue.handler).toBeUndefined();
      expect(
        Object.prototype.hasOwnProperty.call(queue.status, 'hasHandler')
      ).toBe(false);
    });
  });
});
