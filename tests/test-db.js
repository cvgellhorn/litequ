import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Database from '../src/db.js';

describe('Database', () => {
  let db;
  const testDbPath = ':memory:'; // Use in-memory database for tests

  beforeEach(async () => {
    db = new Database(testDbPath);
  });

  afterEach(async () => {
    await db.close();
  });

  describe('initialization', () => {
    it('should create database tables on first use', () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      expect(taskId).toBeTypeOf('number');
    });

    it('should not recreate tables on subsequent initializations', () => {
      db.initialize();
      db.initialize(); // Should not throw
    });
  });

  describe('insertTask', () => {
    it('should insert a task and return an ID', async () => {
      const jobName = 'test-job';
      const taskData = '{"type": "test", "data": 123}';
      const taskId = db.insertTask(jobName, taskData);

      expect(taskId).toBeTypeOf('number');
      expect(taskId).toBeGreaterThan(0);

      // Verify the task was inserted with the correct job name
      const task = db.getTaskById(taskId);
      expect(task.job_name).toBe(jobName);
    });

    it('should insert multiple tasks with incrementing IDs', async () => {
      const taskId1 = db.insertTask('job1', '{"task": 1}');
      const taskId2 = db.insertTask('job2', '{"task": 2}');

      expect(taskId2).to.equal(taskId1 + 1);
    });

    it('should insert tasks for different jobs', async () => {
      const taskId1 = db.insertTask('email', '{"task": 1}');
      const taskId2 = db.insertTask('sms', '{"task": 2}');
      const taskId3 = db.insertTask('email', '{"task": 3}');

      const task1 = db.getTaskById(taskId1);
      const task2 = db.getTaskById(taskId2);
      const task3 = db.getTaskById(taskId3);

      expect(task1.job_name).toBe('email');
      expect(task2.job_name).toBe('sms');
      expect(task3.job_name).toBe('email');
    });
  });

  describe('getPendingTasks', () => {
    it('should return empty array when no tasks exist', async () => {
      const tasks = db.getPendingTasks();
      expect(tasks).toBeInstanceOf(Array);
      expect(tasks).toHaveLength(0);
    });

    it('should return pending tasks', async () => {
      db.insertTask('test-job', '{"test": "data1"}');
      db.insertTask('test-job', '{"test": "data2"}');

      const tasks = db.getPendingTasks();
      expect(tasks).toHaveLength(2);
      expect(tasks[0].status).toBe('pending');
      expect(tasks[1].status).toBe('pending');
    });

    it('should respect limit parameter', async () => {
      db.insertTask('test-job', '{"test": "data1"}');
      db.insertTask('test-job', '{"test": "data2"}');
      db.insertTask('test-job', '{"test": "data3"}');

      const tasks = db.getPendingTasks(2);
      expect(tasks).toHaveLength(2);
    });

    it('should filter tasks by job names', async () => {
      db.insertTask('email', '{"test": "email1"}');
      db.insertTask('sms', '{"test": "sms1"}');
      db.insertTask('email', '{"test": "email2"}');
      db.insertTask('push', '{"test": "push1"}');

      // Get only email and push tasks
      const tasks = db.getPendingTasks(10, new Date().toISOString(), [
        'email',
        'push',
      ]);
      expect(tasks).toHaveLength(3);
      expect(tasks.every((t) => ['email', 'push'].includes(t.job_name))).toBe(
        true
      );
    });

    it('should return all tasks when no job filter is provided', async () => {
      db.insertTask('email', '{"test": "email1"}');
      db.insertTask('sms', '{"test": "sms1"}');
      db.insertTask('push', '{"test": "push1"}');

      const tasks = db.getPendingTasks(10, new Date().toISOString(), null);
      expect(tasks).toHaveLength(3);
    });

    it('should return failed tasks ready for retry', async () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      const pastTime = new Date(Date.now() - 1000).toISOString();

      db.updateTaskStatus(taskId, 'failed', 1, pastTime);

      const tasks = db.getPendingTasks();
      expect(tasks).toHaveLength(1);
      expect(tasks[0].status).toBe('failed');
    });

    it('should not return failed tasks not ready for retry', async () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      const futureTime = new Date(Date.now() + 10000).toISOString();

      db.updateTaskStatus(taskId, 'failed', 1, futureTime);

      const tasks = db.getPendingTasks();
      expect(tasks).toHaveLength(0);
    });
  });

  describe('updateTaskStatus', () => {
    it('should update task status', async () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      db.updateTaskStatus(taskId, 'processing', 0, null);

      const task = db.getTaskById(taskId);
      expect(task.status).toBe('processing');
    });

    it('should update retry count and next retry time', async () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      const nextRetryAt = new Date().toISOString();

      db.updateTaskStatus(taskId, 'failed', 2, nextRetryAt);

      const task = db.getTaskById(taskId);
      expect(task.status).toBe('failed');
      expect(task.retry_count).toBe(2);
      expect(task.next_retry_at).toBe(nextRetryAt);
    });
  });

  describe('getTaskById', () => {
    it('should return task by ID', async () => {
      const jobName = 'test-job';
      const taskData = '{"test": "specific_data"}';
      const taskId = db.insertTask(jobName, taskData);

      const task = db.getTaskById(taskId);
      expect(task.id).toBe(taskId);
      expect(task.job_name).toBe(jobName);
      expect(task.task_data).toBe(taskData);
      expect(task.status).toBe('pending');
    });

    it('should return undefined for non-existent task', async () => {
      const task = db.getTaskById(999);
      expect(task).toBeUndefined();
    });
  });

  describe('deleteTask', () => {
    it('should delete a task', async () => {
      const taskId = db.insertTask('test-job', '{"test": "data"}');
      db.deleteTask(taskId);

      const task = db.getTaskById(taskId);
      expect(task).toBeUndefined();
    });
  });

  describe('getTaskStats', () => {
    it('should return task statistics grouped by job and status', async () => {
      db.insertTask('email', '{"test": "data1"}');
      db.insertTask('email', '{"test": "data2"}');
      const taskId = db.insertTask('sms', '{"test": "data3"}');
      db.updateTaskStatus(taskId, 'completed', 0, null);

      const stats = db.getTaskStats();
      expect(stats).toBeInstanceOf(Array);

      // Check that stats include job_name
      const emailPending = stats.find(
        (s) => s.job_name === 'email' && s.status === 'pending'
      );
      const smsCompleted = stats.find(
        (s) => s.job_name === 'sms' && s.status === 'completed'
      );

      expect(emailPending.count).toBe(2);
      expect(smsCompleted.count).toBe(1);
    });

    it('should group stats by both job name and status', async () => {
      db.insertTask('job1', '{"test": "1"}');
      db.insertTask('job1', '{"test": "2"}');
      db.insertTask('job2', '{"test": "3"}');

      const taskId1 = db.insertTask('job1', '{"test": "4"}');
      db.updateTaskStatus(taskId1, 'completed', 0, null);

      const stats = db.getTaskStats();

      // Should have separate entries for each combination
      const job1Pending = stats.find(
        (s) => s.job_name === 'job1' && s.status === 'pending'
      );
      const job1Completed = stats.find(
        (s) => s.job_name === 'job1' && s.status === 'completed'
      );
      const job2Pending = stats.find(
        (s) => s.job_name === 'job2' && s.status === 'pending'
      );

      expect(job1Pending.count).toBe(2);
      expect(job1Completed.count).toBe(1);
      expect(job2Pending.count).toBe(1);
    });
  });

  describe('cleanupCompletedTasks', () => {
    it('should remove old completed tasks', async () => {
      const taskId1 = db.insertTask('test-job', '{"test": "data1"}');
      const taskId2 = db.insertTask('test-job', '{"test": "data2"}');

      db.updateTaskStatus(taskId1, 'completed', 0, null);
      db.updateTaskStatus(taskId2, 'completed', 0, null);

      // Manually update one task to be old
      db.run(
        `
        UPDATE queue 
        SET updated_at = datetime('now', '-25 hours') 
        WHERE id = ?
      `,
        [taskId1]
      );

      const deletedCount = db.cleanupCompletedTasks(24);
      expect(deletedCount.changes).toBe(1);

      const task1 = db.getTaskById(taskId1);
      const task2 = db.getTaskById(taskId2);

      expect(task1).toBeUndefined();
      expect(task2).toBeDefined();
    });

    it('should cleanup old completed tasks across different jobs', async () => {
      const taskId1 = db.insertTask('email', '{"test": "data1"}');
      const taskId2 = db.insertTask('sms', '{"test": "data2"}');
      const taskId3 = db.insertTask('email', '{"test": "data3"}');

      db.updateTaskStatus(taskId1, 'completed', 0, null);
      db.updateTaskStatus(taskId2, 'completed', 0, null);
      db.updateTaskStatus(taskId3, 'completed', 0, null);

      // Make first two tasks old
      db.run(
        `
        UPDATE queue 
        SET updated_at = datetime('now', '-25 hours') 
        WHERE id IN (?, ?)
      `,
        [taskId1, taskId2]
      );

      const deletedCount = db.cleanupCompletedTasks(24);
      expect(deletedCount.changes).toBe(2);

      const task1 = db.getTaskById(taskId1);
      const task2 = db.getTaskById(taskId2);
      const task3 = db.getTaskById(taskId3);

      expect(task1).toBeUndefined();
      expect(task2).toBeUndefined();
      expect(task3).toBeDefined();
    });
  });
});
