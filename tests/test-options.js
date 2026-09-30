import { describe, afterEach, it, expect, vi } from 'vitest';
import Queue from '../src/queue.js';
import Database from '../src/db.js';
import { createTempDirs } from './helpers/tmp.js';

describe('Queue options', () => {
  const tmp = createTempDirs();
  const queues = [];

  /** @param {Object} [options] */
  function makeQueue(options) {
    const queue = new Queue(options);
    queues.push(queue);
    return queue;
  }

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    tmp.cleanup();
    vi.restoreAllMocks();
  });

  describe('dbPath', () => {
    it('should default to an in-memory database', () => {
      const queue = makeQueue();
      expect(queue.dbPath).toBe(':memory:');
      expect(queue.db.dbPath).toBe(':memory:');
    });

    it('should default the Database class to an in-memory database', async () => {
      const db = new Database();
      expect(db.dbPath).toBe(':memory:');
      await db.close();
    });
  });

  describe('busyTimeout', () => {
    it('should default to 5000 ms', () => {
      const queue = makeQueue({ dbPath: tmp.dbFile() });
      queue.db.initialize();
      expect(queue.db.db.pragma('busy_timeout', { simple: true })).toBe(5000);
    });

    it('should apply a custom busy timeout', () => {
      const queue = makeQueue({ dbPath: tmp.dbFile(), busyTimeout: 1234 });
      queue.db.initialize();
      expect(queue.db.db.pragma('busy_timeout', { simple: true })).toBe(1234);
    });

    it('should use WAL for files but not for in-memory databases', () => {
      const fileQueue = makeQueue({ dbPath: tmp.dbFile() });
      const memoryQueue = makeQueue({ dbPath: ':memory:' });
      fileQueue.db.initialize();
      memoryQueue.db.initialize();

      expect(fileQueue.db.db.pragma('journal_mode', { simple: true })).toBe(
        'wal'
      );
      expect(memoryQueue.db.db.pragma('journal_mode', { simple: true })).toBe(
        'memory'
      );
    });
  });

  describe('logger', () => {
    function createLogger() {
      return { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
    }

    it('should default to console', () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
      const queue = makeQueue();

      queue.logger.warn('hello');

      expect(warn).toHaveBeenCalledWith('hello');
    });

    it('should log unparseable task data through the logger, not console', async () => {
      const consoleError = vi.spyOn(console, 'error');
      const logger = createLogger();
      const queue = makeQueue({ autoProcess: false, maxRetries: 1, logger });
      const job = queue.createJob('broken');
      await job.process(async () => 'ok');
      queue.db.run('INSERT INTO queue (job_name, task_data) VALUES (?, ?)', [
        'broken',
        'not json',
      ]);

      await queue.processOnce();

      expect(logger.error).toHaveBeenCalled();
      expect(consoleError).not.toHaveBeenCalled();
    });

    it('should log processing errors when nobody listens for error events', async () => {
      const logger = createLogger();
      const queue = makeQueue({ autoProcess: false, logger });
      const job = queue.createJob('work');
      await job.process(async () => 'ok');
      queue.db.run('DROP TABLE queue');

      await expect(queue.processOnce()).resolves.toBe(0);

      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining('process'),
        expect.any(Error)
      );
    });

    it('should still emit error events when there is a listener', async () => {
      const logger = createLogger();
      const queue = makeQueue({ autoProcess: false, logger });
      const job = queue.createJob('work');
      await job.process(async () => 'ok');
      const errors = [];
      queue.on('error', (info) => errors.push(info));
      queue.db.run('DROP TABLE queue');

      await queue.processOnce();

      expect(errors).toHaveLength(1);
      expect(errors[0].operation).toBe('process');
      expect(logger.error).not.toHaveBeenCalled();
    });
  });
});
