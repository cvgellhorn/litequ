import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Queue from '../src/queue.js';

describe('Event Bubbling - Job to Queue', () => {
  let queue;
  const testDbPath = ':memory:';

  beforeEach(() => {
    queue = new Queue({
      dbPath: testDbPath,
      autoProcess: false,
      maxRetries: 2,
      baseRetryDelay: 50,
    });
  });

  afterEach(async () => {
    await queue.close();
  });

  describe('added event', () => {
    it('should emit on both job and queue when task is added', async () => {
      const job = queue.createJob('test-job');

      const jobEvents = [];
      const queueEvents = [];

      job.on('added', (data) => {
        jobEvents.push(data);
      });

      queue.on('added', (data) => {
        queueEvents.push(data);
      });

      const taskData = { test: 'data' };
      job.add(taskData);

      // Verify job event
      expect(jobEvents).toHaveLength(1);
      expect(jobEvents[0]).toMatchObject({
        taskId: expect.any(Number),
        taskData,
      });
      expect(jobEvents[0]).not.toHaveProperty('jobName');

      // Verify queue event includes jobName
      expect(queueEvents).toHaveLength(1);
      expect(queueEvents[0]).toMatchObject({
        jobName: 'test-job',
        taskId: expect.any(Number),
        taskData,
      });
    });

    it('should emit queue events for multiple different jobs', async () => {
      const emailJob = queue.createJob('email');
      const smsJob = queue.createJob('sms');

      const queueEvents = [];
      queue.on('added', (data) => {
        queueEvents.push(data);
      });

      emailJob.add({ to: 'test@example.com' });
      smsJob.add({ to: '+1234567890' });

      expect(queueEvents).toHaveLength(2);
      expect(queueEvents[0].jobName).toBe('email');
      expect(queueEvents[1].jobName).toBe('sms');
    });
  });

  describe('completed event', () => {
    it('should emit on both job and queue when task completes', async () => {
      const job = queue.createJob('compute');

      const jobEvents = [];
      const queueEvents = [];

      job.on('completed', (data) => {
        jobEvents.push(data);
      });

      queue.on('completed', (data) => {
        queueEvents.push(data);
      });

      await job.process(async (data) => {
        return data.value * 2;
      });

      const taskData = { value: 21 };
      job.add(taskData);

      await queue._processNextBatch();

      // Verify job event
      expect(jobEvents).toHaveLength(1);
      expect(jobEvents[0]).toMatchObject({
        taskId: expect.any(Number),
        result: 42,
        taskData,
      });
      expect(jobEvents[0]).not.toHaveProperty('jobName');

      // Verify queue event includes jobName
      expect(queueEvents).toHaveLength(1);
      expect(queueEvents[0]).toMatchObject({
        jobName: 'compute',
        taskId: expect.any(Number),
        result: 42,
        taskData,
      });
    });

    it('should distinguish between different jobs at queue level', async () => {
      const addJob = queue.createJob('add');
      const multiplyJob = queue.createJob('multiply');

      const queueEvents = [];
      queue.on('completed', (data) => {
        queueEvents.push(data);
      });

      await addJob.process(async (data) => {
        return data.a + data.b;
      });

      await multiplyJob.process(async (data) => {
        return data.a * data.b;
      });

      addJob.add({ a: 2, b: 3 });
      multiplyJob.add({ a: 4, b: 5 });

      await queue._processNextBatch();

      expect(queueEvents).toHaveLength(2);

      const addEvent = queueEvents.find((e) => e.jobName === 'add');
      const multiplyEvent = queueEvents.find((e) => e.jobName === 'multiply');

      expect(addEvent.result).toBe(5);
      expect(multiplyEvent.result).toBe(20);
    });
  });

  describe('failed event', () => {
    it('should emit on both job and queue when task fails permanently', async () => {
      const job = queue.createJob('failing-job');

      const jobEvents = [];
      const queueEvents = [];

      job.on('failed', (data) => {
        jobEvents.push(data);
      });

      queue.on('failed', (data) => {
        queueEvents.push(data);
      });

      await job.process(async () => {
        throw new Error('Always fails');
      });

      job.add({ test: 'data' });

      // Process multiple times to exhaust retries (maxRetries = 2, so 3 total attempts)
      for (let i = 0; i <= queue.maxRetries; i++) {
        await queue._processNextBatch();
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      // Verify job event
      expect(jobEvents).toHaveLength(1);
      expect(jobEvents[0]).toMatchObject({
        taskId: expect.any(Number),
        error: 'Always fails',
        taskData: { test: 'data' },
        retryCount: queue.maxRetries + 1,
      });
      expect(jobEvents[0]).not.toHaveProperty('jobName');

      // Verify queue event includes jobName
      expect(queueEvents).toHaveLength(1);
      expect(queueEvents[0]).toMatchObject({
        jobName: 'failing-job',
        taskId: expect.any(Number),
        error: 'Always fails',
        taskData: { test: 'data' },
        retryCount: queue.maxRetries + 1,
      });
    });
  });

  describe('retried event', () => {
    it('should emit on both job and queue when task is retried', async () => {
      const job = queue.createJob('retry-job');

      const jobEvents = [];
      const queueEvents = [];

      job.on('retried', (data) => {
        jobEvents.push(data);
      });

      queue.on('retried', (data) => {
        queueEvents.push(data);
      });

      let attempts = 0;
      await job.process(async (data) => {
        attempts++;
        if (attempts === 1) {
          throw new Error('First attempt fails');
        }
        return 'success';
      });

      job.add({ test: 'data' });

      // First attempt - should fail and emit retried
      await queue._processNextBatch();

      // Verify job event
      expect(jobEvents).toHaveLength(1);
      expect(jobEvents[0]).toMatchObject({
        taskId: expect.any(Number),
        taskData: { test: 'data' },
        retryCount: 1,
        error: 'First attempt fails',
      });
      expect(jobEvents[0]).toHaveProperty('nextRetryAt');
      expect(jobEvents[0]).toHaveProperty('delay');
      expect(jobEvents[0]).not.toHaveProperty('jobName');

      // Verify queue event includes jobName
      expect(queueEvents).toHaveLength(1);
      expect(queueEvents[0]).toMatchObject({
        jobName: 'retry-job',
        taskId: expect.any(Number),
        taskData: { test: 'data' },
        retryCount: 1,
        error: 'First attempt fails',
      });
      expect(queueEvents[0]).toHaveProperty('nextRetryAt');
      expect(queueEvents[0]).toHaveProperty('delay');
    });
  });

  describe('multiple jobs with selective listeners', () => {
    it('should allow listening to specific jobs or all jobs', async () => {
      const emailJob = queue.createJob('email');
      const smsJob = queue.createJob('sms');
      const pushJob = queue.createJob('push');

      // Track events at different levels
      const emailJobEvents = [];
      const allQueueEvents = [];

      // Listen to specific job
      emailJob.on('completed', (data) => {
        emailJobEvents.push(data);
      });

      // Listen to all jobs at queue level
      queue.on('completed', (data) => {
        allQueueEvents.push(data);
      });

      // Set up handlers
      await emailJob.process(async (data) => `Email sent to ${data.to}`);
      await smsJob.process(async (data) => `SMS sent to ${data.to}`);
      await pushJob.process(async (data) => `Push sent to ${data.to}`);

      // Add tasks
      emailJob.add({ to: 'user@example.com' });
      smsJob.add({ to: '+1234567890' });
      pushJob.add({ to: 'device123' });

      // Process all
      await queue._processNextBatch();

      // Email job listener should only see email events
      expect(emailJobEvents).toHaveLength(1);
      expect(emailJobEvents[0].result).toContain('Email sent');

      // Queue listener should see all events
      expect(allQueueEvents).toHaveLength(3);
      expect(allQueueEvents.map((e) => e.jobName).sort()).toEqual([
        'email',
        'push',
        'sms',
      ]);
    });

    it('should handle events from the same job type with multiple listeners', async () => {
      const job = queue.createJob('multi-listener');

      const listener1Events = [];
      const listener2Events = [];
      const queueEvents = [];

      // Multiple listeners on the same job
      job.on('completed', (data) => {
        listener1Events.push(data);
      });

      job.on('completed', (data) => {
        listener2Events.push(data);
      });

      // Queue-level listener
      queue.on('completed', (data) => {
        queueEvents.push(data);
      });

      await job.process(async (data) => data.value * 2);

      job.add({ value: 10 });
      job.add({ value: 20 });

      await queue._processNextBatch();

      // Both job listeners should receive events (without jobName)
      expect(listener1Events).toHaveLength(2);
      expect(listener2Events).toHaveLength(2);
      expect(listener1Events[0]).not.toHaveProperty('jobName');
      expect(listener2Events[0]).not.toHaveProperty('jobName');

      // Queue listener should receive events (with jobName)
      expect(queueEvents).toHaveLength(2);
      expect(queueEvents[0].jobName).toBe('multi-listener');
      expect(queueEvents[1].jobName).toBe('multi-listener');
    });
  });

  describe('event payload structure', () => {
    it('should include all required fields in bubbled events', async () => {
      const job = queue.createJob('test-job');

      const capturedEvents = {
        added: null,
        completed: null,
      };

      queue.on('added', (data) => {
        capturedEvents.added = data;
      });

      queue.on('completed', (data) => {
        capturedEvents.completed = data;
      });

      await job.process(async (data) => {
        return { processed: true, original: data };
      });

      const taskData = { input: 'test' };
      const taskId = job.add(taskData);

      await queue._processNextBatch();

      // Verify added event structure
      expect(capturedEvents.added).toEqual({
        jobName: 'test-job',
        taskId,
        taskData,
      });

      // Verify completed event structure
      expect(capturedEvents.completed).toEqual({
        jobName: 'test-job',
        taskId,
        result: { processed: true, original: taskData },
        taskData,
      });
    });
  });
});
