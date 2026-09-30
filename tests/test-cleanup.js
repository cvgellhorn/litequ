import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';

describe('cleanup()', () => {
  const tmp = createTempDirs();
  let queue;

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-30T12:00:00.000Z'));
    queue = new Queue({
      dbPath: tmp.dbFile(),
      autoProcess: false,
      logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
    });
  });

  afterEach(async () => {
    vi.useRealTimers();
    await queue.close();
    tmp.cleanup();
  });

  /** Inserts a completed task last updated at the given CURRENT_TIMESTAMP-style time. */
  function completedAt(updatedAt) {
    return queue.db.run(
      `INSERT INTO queue (job_name, task_data, status, updated_at)
       VALUES ('work', '{}', 'completed', ?)`,
      [updatedAt]
    ).lastID;
  }

  it('should keep tasks completed after the cutoff on the same day', () => {
    const id = completedAt('2026-09-30 11:50:00');

    const result = queue.cleanup(1);

    expect(result.changes).toBe(0);
    expect(queue.getTask(id)).toBeDefined();
  });

  it('should delete tasks completed before the cutoff', () => {
    const id = completedAt('2026-09-30 10:59:59');

    const result = queue.cleanup(1);

    expect(result.changes).toBe(1);
    expect(queue.getTask(id)).toBeUndefined();
  });

  it('should delete tasks completed on an earlier day', () => {
    const id = completedAt('2026-09-28 23:00:00');

    expect(queue.cleanup(24).changes).toBe(1);
    expect(queue.getTask(id)).toBeUndefined();
  });
});
