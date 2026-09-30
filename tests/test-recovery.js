import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';
import { createLegacyDatabase } from './helpers/legacy.js';

describe('Interrupted task recovery', () => {
  const tmp = createTempDirs();
  const queues = [];
  let logger;

  /** @param {Object} options */
  function makeQueue(options) {
    const queue = new Queue({ logger, ...options });
    queues.push(queue);
    return queue;
  }

  beforeEach(() => {
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
  });

  /** Creates a file with one task left in `processing`, as after a crash. */
  function fileWithInterruptedTask() {
    const file = tmp.dbFile();
    createLegacyDatabase(file, [
      { job_name: 'email', task_data: '{"to":"a@example.com"}' },
      {
        job_name: 'email',
        task_data: '{"to":"b@example.com"}',
        status: 'processing',
      },
    ]);
    return file;
  }

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    tmp.cleanup();
  });

  it('should set tasks left in processing back to pending on open', () => {
    const file = fileWithInterruptedTask();

    const queue = makeQueue({ dbPath: file, autoProcess: false });

    expect(queue.getTask(2).status).toBe('pending');
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('1 interrupted task')
    );
  });

  it('should process a recovered task once a handler is registered', async () => {
    const file = fileWithInterruptedTask();
    const queue = makeQueue({ dbPath: file, autoProcess: false });
    const handled = [];

    await queue
      .createJob('email')
      .process(async (data) => handled.push(data.to));
    await queue.processOnce();

    expect(handled.sort()).toEqual(['a@example.com', 'b@example.com']);
    expect(queue.getTask(2).status).toBe('completed');
  });

  it('should leave interrupted tasks alone with recoverInterrupted: false', () => {
    const file = fileWithInterruptedTask();

    const queue = makeQueue({
      dbPath: file,
      autoProcess: false,
      recoverInterrupted: false,
    });

    expect(queue.getTask(2).status).toBe('processing');
    expect(logger.info).not.toHaveBeenCalled();
  });

  it('should not log when there is nothing to recover', () => {
    const file = tmp.dbFile();
    createLegacyDatabase(file, [{ job_name: 'email', task_data: '{}' }]);

    makeQueue({ dbPath: file, autoProcess: false });

    expect(logger.info).not.toHaveBeenCalled();
  });
});
