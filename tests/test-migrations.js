import { describe, afterEach, it, expect } from 'vitest';
import Queue from '../src/queue.js';
import Database, { SCHEMA_VERSION } from '../src/db.js';
import { createTempDirs } from './helpers/tmp.js';
import { createLegacyDatabase, openRaw } from './helpers/legacy.js';

describe('Schema migrations', () => {
  const tmp = createTempDirs();

  afterEach(() => {
    tmp.cleanup();
  });

  function readSchema(file) {
    const raw = openRaw(file);
    try {
      return {
        version: raw.pragma('user_version', { simple: true }),
        columns: raw.pragma('table_info(queue)').map((column) => column.name),
        indexes: raw.pragma('index_list(queue)').map((index) => index.name),
        rows: raw.prepare('SELECT * FROM queue ORDER BY id').all(),
      };
    } finally {
      raw.close();
    }
  }

  it('should stamp a new database with the current schema version', async () => {
    const file = tmp.dbFile();
    const db = new Database(file);
    db.initialize();
    await db.close();

    expect(readSchema(file).version).toBe(SCHEMA_VERSION);
  });

  it('should migrate an unversioned (litequu 2.x / litequ 1.x) file and keep its rows', async () => {
    const file = tmp.dbFile();
    const retryAt = new Date(Date.now() + 60_000).toISOString();
    createLegacyDatabase(file, [
      { job_name: 'email', task_data: '{"to":"a@example.com"}' },
      {
        job_name: 'email',
        task_data: '{"to":"b@example.com"}',
        status: 'completed',
      },
      {
        job_name: 'sms',
        task_data: '{"to":"+1"}',
        status: 'failed',
        retry_count: 2,
        next_retry_at: retryAt,
      },
    ]);
    const before = readSchema(file);
    expect(before.version).toBe(0);

    const queue = new Queue({ dbPath: file, autoProcess: false });
    queue.db.initialize();
    await queue.close();

    const after = readSchema(file);
    expect(after.version).toBe(SCHEMA_VERSION);
    expect(after.columns).toEqual(
      expect.arrayContaining([
        ...before.columns,
        'locked_by',
        'locked_until',
        'dedupe_key',
      ])
    );
    expect(after.indexes).toContain('idx_dedupe');
    expect(after.rows).toHaveLength(3);
    for (const [index, row] of before.rows.entries()) {
      expect(after.rows[index]).toMatchObject(row);
    }
  });

  it('should be idempotent when a database is opened repeatedly', async () => {
    const file = tmp.dbFile();
    createLegacyDatabase(file, [{ job_name: 'email', task_data: '{}' }]);

    for (let i = 0; i < 3; i++) {
      const db = new Database(file);
      db.initialize();
      await db.close();
    }

    const schema = readSchema(file);
    expect(schema.version).toBe(SCHEMA_VERSION);
    expect(schema.rows).toHaveLength(1);
  });
});
