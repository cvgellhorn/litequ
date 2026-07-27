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

  describe('legacy API removed', () => {
    it('should not expose queue.add, queue.process, or queue.processOnce', () => {
      expect(queue.add).toBeUndefined();
      expect(queue.process).toBeUndefined();
      expect(queue.processOnce).toBeUndefined();
    });

    it('should not expose queue-level handler state', () => {
      expect(queue.handler).toBeUndefined();
      expect(
        Object.prototype.hasOwnProperty.call(queue.status, 'hasHandler')
      ).toBe(false);
    });
  });
});
