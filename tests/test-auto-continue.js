import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Queue from '../src/index.js';

describe('Auto-Continue Processing', () => {
  let queue;

  beforeEach(() => {
    queue = new Queue({
      dbPath: ':memory:',
      autoProcess: true,
      maxConcurrent: 2,
      maxRetries: 1,
      baseRetryDelay: 50,
    });
  });

  afterEach(async () => {
    await queue.close();
  });

  it('should immediately continue processing when tasks are added during processing', async () => {
    const processedTasks = [];
    const processingOrder = [];

    // Create a job that adds more tasks during processing
    const emailJob = queue.createJob('email');

    const completedPromise = new Promise((resolve) => {
      let completedCount = 0;
      emailJob.on('completed', ({ taskId, taskData }) => {
        processedTasks.push({ taskId, taskData });
        completedCount++;

        // We expect 4 tasks total: 2 initial + 2 added during processing
        if (completedCount === 4) {
          resolve();
        }
      });
    });

    // Set up handler that adds tasks during processing
    await emailJob.process(async (taskData) => {
      processingOrder.push(taskData.id);

      // First two tasks each add a new task
      if (taskData.id === 1 || taskData.id === 2) {
        // Add a task during processing
        emailJob.add({ id: `${taskData.id}-child`, parent: taskData.id });
      }

      // Simulate some async work
      await new Promise((resolve) => setTimeout(resolve, 10));
      return `Processed ${taskData.id}`;
    });

    // Add initial tasks
    emailJob.add({ id: 1 });
    emailJob.add({ id: 2 });

    // Wait for all tasks to complete
    await Promise.race([
      completedPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Test timeout')), 5000)
      ),
    ]);

    expect(processedTasks).toHaveLength(4);

    // Verify that child tasks were added
    const childTasks = processedTasks.filter(
      (t) =>
        typeof t.taskData.id === 'string' && t.taskData.id.includes('child')
    );
    expect(childTasks).toHaveLength(2);

    // Verify processing order shows continuity (no long gaps)
    expect(processingOrder).toHaveLength(4);
  });

  it('should handle multiple levels of task addition during processing', async () => {
    const processedTasks = [];
    const job = queue.createJob('recursive');

    const completedPromise = new Promise((resolve) => {
      let completedCount = 0;
      job.on('completed', ({ taskId, taskData }) => {
        processedTasks.push({ taskId, taskData });
        completedCount++;

        // We expect: 1 initial + 2 level-1 children + 4 level-2 children = 7 total
        if (completedCount === 7) {
          resolve();
        }
      });
    });

    // Handler that creates a tree of tasks
    await job.process(async (taskData) => {
      const { id, level = 0 } = taskData;

      // Add child tasks for the first two levels
      if (level < 2) {
        job.add({ id: `${id}-a`, level: level + 1 });
        job.add({ id: `${id}-b`, level: level + 1 });
      }

      await new Promise((resolve) => setTimeout(resolve, 5));
      return `Processed ${id}`;
    });

    // Add the root task
    job.add({ id: 'root', level: 0 });

    // Wait for all tasks to complete
    await Promise.race([
      completedPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Test timeout')), 5000)
      ),
    ]);

    expect(processedTasks).toHaveLength(7);

    // Verify we have the correct number of tasks at each level
    const level0 = processedTasks.filter((t) => (t.taskData.level || 0) === 0);
    const level1 = processedTasks.filter((t) => t.taskData.level === 1);
    const level2 = processedTasks.filter((t) => t.taskData.level === 2);

    expect(level0).toHaveLength(1); // root
    expect(level1).toHaveLength(2); // root-a, root-b
    expect(level2).toHaveLength(4); // root-a-a, root-a-b, root-b-a, root-b-b
  });

  it('should continue processing across multiple jobs when tasks are added during processing', async () => {
    const processedTasks = { job1: [], job2: [] };
    const job1 = queue.createJob('job1');
    const job2 = queue.createJob('job2');

    const completedPromise = new Promise((resolve) => {
      let completedCount = 0;
      const checkComplete = () => {
        completedCount++;
        if (completedCount === 4) {
          resolve();
        }
      };

      job1.on('completed', ({ taskData }) => {
        processedTasks.job1.push(taskData);
        checkComplete();
      });

      job2.on('completed', ({ taskData }) => {
        processedTasks.job2.push(taskData);
        checkComplete();
      });
    });

    // job1 handler adds tasks to job2
    await job1.process(async (taskData) => {
      if (taskData.addToJob2) {
        job2.add({ id: `from-job1-${taskData.id}` });
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
      return `Job1 processed ${taskData.id}`;
    });

    // job2 handler adds tasks to job1
    await job2.process(async (taskData) => {
      if (taskData.addToJob1) {
        job1.add({ id: `from-job2-${taskData.id}` });
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
      return `Job2 processed ${taskData.id}`;
    });

    // Add initial tasks that cross-reference each other
    job1.add({ id: 1, addToJob2: true });
    job2.add({ id: 2, addToJob1: true });

    // Wait for all tasks to complete
    await Promise.race([
      completedPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Test timeout')), 5000)
      ),
    ]);

    expect(processedTasks.job1).toHaveLength(2);
    expect(processedTasks.job2).toHaveLength(2);
  });

  it('should not get stuck in an infinite loop with circular task addition', async () => {
    const processedTasks = [];
    const maxTasks = 10;
    const job = queue.createJob('circular');

    const completedPromise = new Promise((resolve) => {
      job.on('completed', ({ taskData }) => {
        processedTasks.push(taskData);
        if (processedTasks.length === maxTasks) {
          resolve();
        }
      });
    });

    // Handler that always adds a new task (but we'll limit it)
    await job.process(async (taskData) => {
      const { id } = taskData;

      // Only add more tasks if we haven't hit the limit
      if (id < maxTasks) {
        job.add({ id: id + 1 });
      }

      await new Promise((resolve) => setTimeout(resolve, 5));
      return `Processed ${id}`;
    });

    // Add the first task
    job.add({ id: 1 });

    // Wait for all tasks to complete
    await Promise.race([
      completedPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Test timeout')), 5000)
      ),
    ]);

    expect(processedTasks).toHaveLength(maxTasks);

    // Verify all tasks were processed in order
    const ids = processedTasks.map((t) => t.id);
    expect(ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

});
