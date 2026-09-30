import { describe, beforeEach, afterEach, it, expect, vi } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import BetterSqlite3 from 'better-sqlite3';
import Queue from '../src/queue.js';
import { QueueReadOnlyError, sqliteWritable } from '../src/index.js';
import { canWrite } from '../src/roles.js';
import { createTempDirs } from './helpers/tmp.js';
import { openRaw } from './helpers/legacy.js';

const isRoot = typeof process.getuid === 'function' && process.getuid() === 0;

describe('Following a writable role', () => {
  const tmp = createTempDirs();
  const queues = [];
  let logger;
  let file;
  let writable;

  /** @param {Object} [options] */
  function makeQueue(options) {
    const queue = new Queue({
      dbPath: file,
      writable: () => writable,
      roleCheckInterval: 1000,
      logger,
      ...options,
    });
    queues.push(queue);
    return queue;
  }

  function countRows() {
    const raw = openRaw(file);
    try {
      raw.exec(
        'CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, task_data TEXT)'
      );
      return raw.prepare('SELECT COUNT(*) AS n FROM queue').get().n;
    } finally {
      raw.close();
    }
  }

  /** Advances the role timer and waits for the resulting role-change event. */
  async function nextRoleChange(queue) {
    const changed = new Promise((resolve) =>
      queue.once('role-change', resolve)
    );
    await vi.advanceTimersByTimeAsync(1000);
    return changed;
  }

  beforeEach(() => {
    vi.useFakeTimers({
      toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'],
    });
    logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
    file = tmp.dbFile();
    writable = true;
  });

  afterEach(async () => {
    await Promise.all(queues.splice(0).map((queue) => queue.close()));
    vi.useRealTimers();
    tmp.cleanup();
  });

  it('should open dbPath when writable() is true', () => {
    const queue = makeQueue();

    expect(queue.dbPath).toBe(file);
    expect(queue.status).toMatchObject({ writable: true, dbPath: file });
  });

  it('should open readOnlyDbPath when writable() is false', () => {
    writable = false;
    const queue = makeQueue();

    expect(queue.dbPath).toBe(':memory:');
    expect(queue.status.writable).toBe(false);
    expect(fs.existsSync(file)).toBe(false);
  });

  it('should start read-only when writable() throws at construction', () => {
    const queue = makeQueue({
      writable: () => {
        throw new Error('no idea');
      },
    });

    expect(queue.dbPath).toBe(':memory:');
    expect(logger.error).toHaveBeenCalled();
  });

  it('should move open tasks into the file when it becomes writable', async () => {
    writable = false;
    const queue = makeQueue();
    queue.createJob('email').add({ to: 'a@example.com' });

    writable = true;
    const event = await nextRoleChange(queue);

    expect(event).toEqual({ writable: true });
    expect(queue.dbPath).toBe(file);
    expect(countRows()).toBe(1);
  });

  it('should leave file tasks for the new writer when it becomes read-only', async () => {
    const queue = makeQueue();
    queue.createJob('email').add({ to: 'a@example.com' });

    writable = false;
    const event = await nextRoleChange(queue);

    expect(event).toEqual({ writable: false });
    expect(queue.dbPath).toBe(':memory:');
    expect(queue.getStats()).toEqual([]);
    expect(countRows()).toBe(1);
  });

  it('should keep the current role when writable() throws', async () => {
    const queue = makeQueue({
      writable: () => {
        if (writable) return true;
        throw new Error('lookup failed');
      },
    });
    const onRoleChange = vi.fn();
    queue.on('role-change', onRoleChange);

    writable = false;
    await vi.advanceTimersByTimeAsync(1000);

    expect(onRoleChange).not.toHaveBeenCalled();
    expect(queue.dbPath).toBe(file);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('writable'),
      expect.any(Error)
    );
  });

  it("should throw QueueReadOnlyError from add() with whenReadOnly: 'throw'", () => {
    writable = false;
    const queue = makeQueue({ whenReadOnly: 'throw' });
    const job = queue.createJob('email');

    expect(() => job.add({ to: 'a@example.com' })).toThrow(QueueReadOnlyError);
    expect(queue.getStats()).toEqual([]);
  });

  it('should reject an unknown whenReadOnly value', () => {
    expect(() => makeQueue({ whenReadOnly: 'drop' })).toThrow(/whenReadOnly/);
  });

  it('should stop the role timer on close', async () => {
    const queue = makeQueue();
    const onRoleChange = vi.fn();
    queue.on('role-change', onRoleChange);
    expect(vi.getTimerCount()).toBe(1);

    await queue.close();
    writable = false;
    await vi.advanceTimersByTimeAsync(5000);

    expect(vi.getTimerCount()).toBe(0);
    expect(onRoleChange).not.toHaveBeenCalled();
  });

  it('should not keep the process alive with the role timer', () => {
    vi.useRealTimers();
    const queue = makeQueue();

    expect(queue._roleTimer.hasRef()).toBe(false);
  });
});

describe('sqliteWritable()', () => {
  const tmp = createTempDirs();

  afterEach(() => {
    tmp.cleanup();
  });

  function createDatabase(file) {
    const raw = new BetterSqlite3(file);
    raw.pragma('journal_mode = WAL');
    raw.exec('CREATE TABLE queue (id INTEGER PRIMARY KEY)');
    raw.close();
  }

  it('should report a writable file as writable', () => {
    const file = tmp.dbFile();
    createDatabase(file);

    expect(sqliteWritable(file)()).toBe(true);
  });

  it('should not leave anything behind in the database', () => {
    const file = tmp.dbFile();
    createDatabase(file);

    sqliteWritable(file)();

    const raw = openRaw(file);
    const tables = raw
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
      .all()
      .map((row) => row.name);
    raw.close();
    expect(tables).toEqual(['queue']);
  });

  it.skipIf(isRoot)(
    'should report a read-only file in a read-only directory as read-only',
    () => {
      const file = tmp.dbFile();
      createDatabase(file);
      fs.chmodSync(file, 0o444);
      fs.chmodSync(path.dirname(file), 0o555);

      expect(sqliteWritable(file)()).toBe(false);
    }
  );

  it.skipIf(isRoot)(
    'should report a missing file in a read-only directory as read-only',
    () => {
      const dir = tmp.make();
      fs.chmodSync(dir, 0o555);

      expect(sqliteWritable(path.join(dir, 'queue.db'))()).toBe(false);
    }
  );

  it('should detect a connection opened with { readonly: true }', () => {
    const file = tmp.dbFile();
    createDatabase(file);
    const connection = new BetterSqlite3(file, { readonly: true });

    try {
      expect(canWrite(connection)).toBe(false);
    } finally {
      connection.close();
    }
  });

  it('should detect that BEGIN IMMEDIATE alone is not enough', () => {
    const file = tmp.dbFile();
    createDatabase(file);
    const connection = new BetterSqlite3(file, { readonly: true });

    try {
      // SQLite grants the write lock without writing on a read-only
      // connection, which is why canWrite() performs a real write.
      connection.exec('BEGIN IMMEDIATE');
      connection.exec('ROLLBACK');
      expect(canWrite(connection)).toBe(false);
    } finally {
      connection.close();
    }
  });
});
