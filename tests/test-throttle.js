import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import Queue from '../src/queue.js';
import { createTempDirs } from './helpers/tmp.js';
import { openRaw } from './helpers/legacy.js';

describe('Throttling with dedupeKey', () => {
  const tmp = createTempDirs();
  const queues = [];
  let file;

  function makeQueue() {
    const queue = new Queue({
      dbPath: file,
      autoProcess: false,
      logger: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
    });
    queues.push(queue);
    return queue;
  }

  function countRows(where = '1 = 1', params = []) {
    const raw = openRaw(file);
    try {
      return raw
        .prepare(`SELECT COUNT(*) AS n FROM queue WHERE ${where}`)
        .get(...params).n;
    } finally {
      raw.close();
    }
  }

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-30T12:00:00.000Z'));
    file = tmp.dbFile();
  });

  afterEach(async () => {
    vi.useRealTimers();
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    tmp.cleanup();
  });

  it('should drop a duplicate within the window and return null', () => {
    const job = makeQueue().createJob('digest');
    const options = { dedupeKey: 'user-1', throttleMs: 60_000 };

    const first = job.add({ n: 1 }, options);
    vi.advanceTimersByTime(59_000);
    const second = job.add({ n: 2 }, options);

    expect(first).toBeTypeOf('number');
    expect(second).toBeNull();
    expect(countRows()).toBe(1);
  });

  it('should allow the task again after the window', () => {
    const job = makeQueue().createJob('digest');
    const options = { dedupeKey: 'user-1', throttleMs: 60_000 };

    job.add({ n: 1 }, options);
    vi.advanceTimersByTime(60_001);

    expect(job.add({ n: 2 }, options)).toBeTypeOf('number');
    expect(countRows()).toBe(2);
  });

  it('should work with windows shorter than a second', () => {
    const job = makeQueue().createJob('ping');
    const options = { dedupeKey: 'host-1', throttleMs: 500 };

    job.add({}, options);
    vi.advanceTimersByTime(400);
    expect(job.add({}, options)).toBeNull();
    vi.advanceTimersByTime(200);
    expect(job.add({}, options)).toBeTypeOf('number');
  });

  it('should not let different keys or jobs interact', () => {
    const queue = makeQueue();
    const digest = queue.createJob('digest');
    const alert = queue.createJob('alert');
    const throttle = (dedupeKey) => ({ dedupeKey, throttleMs: 60_000 });

    expect(digest.add({}, throttle('user-1'))).toBeTypeOf('number');
    expect(digest.add({}, throttle('user-2'))).toBeTypeOf('number');
    expect(alert.add({}, throttle('user-1'))).toBeTypeOf('number');
    expect(digest.add({})).toBeTypeOf('number');
    expect(countRows()).toBe(4);
  });

  it('should survive closing and reopening the file', async () => {
    const options = { dedupeKey: 'user-1', throttleMs: 60_000 };
    const first = makeQueue();
    first.createJob('digest').add({ n: 1 }, options);
    await first.close();

    const second = makeQueue();

    expect(second.createJob('digest').add({ n: 2 }, options)).toBeNull();
  });

  it('should throttle against a task stored with a dedupeKey but no window', () => {
    const job = makeQueue().createJob('digest');
    job.add({ n: 1 }, { dedupeKey: 'user-1' });

    expect(
      job.add({ n: 2 }, { dedupeKey: 'user-1', throttleMs: 60_000 })
    ).toBeNull();
    expect(countRows('dedupe_key = ?', ['user-1'])).toBe(1);
  });

  it('should not emit added for a dropped task', () => {
    const queue = makeQueue();
    const job = queue.createJob('digest');
    const added = vi.fn();
    queue.on('added', added);
    const options = { dedupeKey: 'user-1', throttleMs: 60_000 };

    job.add({ n: 1 }, options);
    job.add({ n: 2 }, options);

    expect(added).toHaveBeenCalledTimes(1);
  });

  it('should reject invalid options', () => {
    const job = makeQueue().createJob('digest');

    expect(() => job.add({}, { dedupeKey: 'k', throttleMs: -1 })).toThrow(
      TypeError
    );
    expect(() => job.add({}, { dedupeKey: 42, throttleMs: 1000 })).toThrow(
      TypeError
    );
  });
});
