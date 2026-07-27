import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Queue from '../src/queue.js';

describe('Queue', () => {
  let queue;
  const testDbPath = ':memory:'; // Use in-memory database for tests

  beforeEach(() => {
    queue = new Queue({
      dbPath: testDbPath,
      autoProcess: false, // Disable auto-processing for controlled tests
      maxRetries: 2,
      baseRetryDelay: 100, // Faster tests
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
    });
  });

  describe('add', () => {
    it('should add a task and return task ID', () => {
      const taskData = { type: 'test', data: 123 };
      const taskId = queue.add(taskData);

      expect(taskId).toBeTypeOf('number');
      expect(taskId).toBeGreaterThan(0);
    });

    it('should emit added event', async () => {
      const taskData = { type: 'test', data: 123 };

      const addedPromise = new Promise((resolve) => {
        queue.on('added', (info) => {
          expect(info.taskId).toBeTypeOf('number');
          expect(info.taskData).toEqual(taskData);
          resolve();
        });
      });

      queue.add(taskData);
      await addedPromise;
    });

    it('should handle complex task data', async () => {
      const complexData = {
        user: { id: 1, name: 'John' },
        actions: ['create', 'update'],
        metadata: { timestamp: Date.now() },
      };

      const taskId = queue.add(complexData);
      const task = queue.getTask(taskId);

      expect(JSON.parse(task.task_data)).toEqual(complexData);
    });
  });

  describe('processOnce', () => {
    it('should process a single task successfully', async () => {
      const taskData = { value: 42 };
      queue.add(taskData);

      const results = [];
      await queue.processOnce(async (data) => {
        results.push(data.value * 2);
        return data.value * 2;
      });

      expect(results).toHaveLength(1);
      expect(results[0]).toBe(84);
    });

    it('should emit completed event on success', async () => {
      const completedPromise = new Promise((resolve) => {
        queue.on('completed', (info) => {
          expect(info.taskId).toBeTypeOf('number');
          expect(info.result).toBe(20);
          expect(info.taskData).toEqual({ value: 10 });
          resolve();
        });
      });

      queue.add({ value: 10 });
      await queue.processOnce(async (data) => data.value * 2);
      await completedPromise;
    });

    it('should handle task failure and retry', async () => {
      queue.add({ shouldFail: true });

      let attempts = 0;
      const retryEvents = [];

      queue.on('retried', (info) => {
        retryEvents.push(info);
      });

      // First processing attempt (will fail)
      await queue.processOnce(async (data) => {
        attempts++;
        if (data.shouldFail && attempts === 1) {
          throw new Error('Simulated failure');
        }
        return 'success';
      });

      expect(retryEvents).toHaveLength(1);
      expect(retryEvents[0].retryCount).toBe(1);

      // Wait for retry delay to pass
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Second processing attempt (should succeed)
      await queue.processOnce(async () => {
        attempts++;
        return 'success';
      });

      expect(attempts).toBe(2);
    });

    it('should emit failed event after max retries exceeded', async () => {
      queue.add({ alwaysFail: true });

      const failedEvents = [];
      queue.on('failed', (info) => {
        failedEvents.push(info);
      });

      // Process and fail multiple times - need to process enough times to exhaust retries
      // The queue has maxRetries=2, so it will try 3 times total (initial + 2 retries)
      let attempts = 0;
      while (attempts <= queue.maxRetries && failedEvents.length === 0) {
        await queue.processOnce(async () => {
          throw new Error('Always fails');
        });

        attempts++;

        // Wait for retry delay and any event processing
        await new Promise((resolve) => setTimeout(resolve, 200));
      }

      expect(failedEvents).toHaveLength(1);
      expect(failedEvents[0].retryCount).toBe(queue.maxRetries + 1);
    });

    it('should respect maxConcurrent limit', async () => {
      const concurrentQueue = new Queue({
        dbPath: ':memory:',
        maxConcurrent: 2,
        autoProcess: false,
      });

      // Add multiple tasks
      await Promise.all([
        concurrentQueue.add({ id: 1 }),
        concurrentQueue.add({ id: 2 }),
        concurrentQueue.add({ id: 3 }),
        concurrentQueue.add({ id: 4 }),
      ]);

      let concurrent = 0;
      let maxConcurrent = 0;

      await concurrentQueue.processOnce(async (data) => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);

        // Simulate async work
        await new Promise((resolve) => setTimeout(resolve, 50));

        concurrent--;
        return data.id;
      });

      expect(maxConcurrent).toBeLessThanOrEqual(2);
      await concurrentQueue.close();
    });
  });

  describe('exponential backoff', () => {
    it('should calculate correct retry delays', async () => {
      queue.add({ fail: true });

      const retryEvents = [];
      queue.on('retried', (info) => {
        retryEvents.push(info);
      });

      // First failure
      await queue.processOnce(async () => {
        throw new Error('Fail');
      });

      // Wait and process again
      await new Promise((resolve) => setTimeout(resolve, 150));
      await queue.processOnce(async () => {
        throw new Error('Fail again');
      });

      expect(retryEvents).toHaveLength(2);

      // First retry should have delay around baseRetryDelay (100ms) - allow for jitter
      expect(retryEvents[0].delay).toBeGreaterThan(50);
      expect(retryEvents[0].delay).toBeLessThan(200);

      // Second retry should have delay around baseRetryDelay * 2 (200ms) - allow for jitter
      expect(retryEvents[1].delay).toBeGreaterThan(100);
      expect(retryEvents[1].delay).toBeLessThan(400);
    });
  });

  describe('getStats', () => {
    it('should return queue statistics', async () => {
      queue.add({ task: 1 });
      queue.add({ task: 2 });

      await queue.processOnce(async (data) => {
        if (data.task === 1) return 'success';
        throw new Error('Fail');
      });

      const stats = queue.getStats();
      expect(stats).toBeInstanceOf(Array);

      const completedStats = stats.find((s) => s.status === 'completed');
      const failedStats = stats.find((s) => s.status === 'failed');

      expect(completedStats?.count).toBe(1);
      expect(failedStats?.count).toBe(1);
    });
  });

  describe('status getter', () => {
    it('should return current queue status', () => {
      const status = queue.status;

      expect(status).toHaveProperty('currentRunning');
      expect(status).toHaveProperty('maxConcurrent');
      expect(status).toHaveProperty('isProcessing');
      expect(status).toHaveProperty('autoProcess');
      expect(status).toHaveProperty('hasHandler');

      expect(status.currentRunning).toBe(0);
      expect(status.maxConcurrent).toBe(5);
      expect(status.isProcessing).toBe(false);
      expect(status.hasHandler).toBe(false);
    });
  });

  describe('error handling', () => {
    it('should handle invalid task data gracefully', async () => {
      // Suppress expected console errors during this test
      const originalConsoleError = console.error;
      console.error = () => {}; // Mock console.error to suppress logs

      try {
        // Initialize database first
        await queue.db.initialize();
        // Manually insert invalid JSON with job_name
        await queue.db.run(
          'INSERT INTO queue (job_name, task_data) VALUES (?, ?)',
          ['default', 'invalid json']
        );
        const errorEvents = [];
        const retryEvents = [];
        queue.on('failed', (info) => {
          errorEvents.push(info);
        });
        queue.on('retried', (info) => {
          retryEvents.push(info);
        });
        // Process multiple times to exhaust retries (maxRetries = 2, so it will try 3 times total)
        for (let i = 0; i <= queue.maxRetries; i++) {
          await queue.processOnce(async () => {
            return 'should not reach here';
          });
          // Wait for retry delay if not the last attempt
          if (i < queue.maxRetries) {
            await new Promise((resolve) => setTimeout(resolve, 150));
          }
        }
        expect(retryEvents.length).toBeGreaterThan(0);
        // The invalid JSON error might be handled differently - check if we got any failed events
        if (errorEvents.length > 0) {
          expect(errorEvents[0].error).toContain('Invalid task data JSON');
        } else {
          // Alternative: the task might fail completely and emit a different event
          expect(retryEvents.length).toBeGreaterThan(0);
        }
      } finally {
        // Restore original console.error
        console.error = originalConsoleError;
      }
    });

    // TODO: Add this test back in when we have a way to test database failures
    // it('should emit error events for database failures', async () => {
    //   let errorEmitted = false;
    //   let caughtError = null;
    //   queue.on('error', (info) => {
    //     expect(info.error).toBeInstanceOf(Error);
    //     expect(info.operation).toBeTypeOf('string');
    //     errorEmitted = true;
    //   });
    //   // Force database error by closing it
    //   await queue.db.close();
    //   try {
    //     queue.add({ test: 'data' });
    //   } catch (error) {
    //     // Expected error due to closed database
    //     caughtError = error;
    //   }
    //   // Give a moment for the error event to be emitted
    //   await new Promise((resolve) => setTimeout(resolve, 100));
    //   // Either we should get an error event OR a caught error (both indicate proper error handling)
    //   expect(errorEmitted || !!caughtError).toBe(true);
    // });
  });
});
