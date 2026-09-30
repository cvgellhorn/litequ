import BetterSqlite3 from 'better-sqlite3';
import { createLogger } from './logger.js';

/**
 * Current schema version, stored in `PRAGMA user_version`.
 * Version 0 is the unversioned litequu 2.x schema.
 * @type {number}
 */
export const SCHEMA_VERSION = 2;

/**
 * Schema migrations, applied in order to databases whose `user_version` is
 * below `version`. Each step must be safe to run on a database that already
 * has its changes, because `CREATE TABLE IF NOT EXISTS` always creates the
 * baseline table first.
 * @type {Array<{ version: number, up: (db: any) => void }>}
 */
const MIGRATIONS = [
  // 1: the litequu 2.x schema, unchanged; only starts version tracking.
  { version: 1, up: () => {} },
  // 2: leases. locked_until is an ISO timestamp, like next_retry_at.
  {
    version: 2,
    up: (db) => {
      addColumn(db, 'locked_by', 'TEXT');
      addColumn(db, 'locked_until', 'DATETIME');
    },
  },
];

/**
 * Adds a column to the queue table unless it already exists.
 * @param {any} db - The better-sqlite3 connection
 * @param {string} name - Column name
 * @param {string} type - Column type
 * @returns {void}
 */
function addColumn(db, name, type) {
  const columns = db.pragma('table_info(queue)').map((column) => column.name);
  if (!columns.includes(name)) {
    db.exec(`ALTER TABLE queue ADD COLUMN ${name} ${type}`);
  }
}

/**
 * SQL condition for tasks that can be claimed: pending tasks, failed tasks
 * whose retry is due, and processing tasks whose lease is missing or expired
 * (their worker stopped). Takes the current time twice as parameters.
 */
const READY_CONDITION = `(
  status = 'pending'
  OR (status = 'failed' AND next_retry_at <= ?)
  OR (status = 'processing' AND (locked_until IS NULL OR locked_until <= ?))
)`;

/**
 * Builds a `job_name IN (...)` filter.
 * @param {Array<string> | null | undefined} jobNames - Job names, or null for all jobs
 * @returns {{ sql: string, params: Array<string> }} SQL fragment starting with AND, and its parameters
 */
function jobFilter(jobNames) {
  if (!jobNames || jobNames.length === 0) {
    return { sql: '', params: [] };
  }
  return {
    sql: ` AND job_name IN (${jobNames.map(() => '?').join(', ')})`,
    params: [...jobNames],
  };
}

/**
 * Returns true for paths that open an in-memory (or temporary) database.
 * @param {string} dbPath - Database path
 * @returns {boolean} Whether the database lives only in memory
 */
export function isMemoryPath(dbPath) {
  return dbPath === ':memory:' || dbPath === '';
}

/**
 * Database class for managing SQLite operations for the queue system.
 * Provides a wrapper around better-sqlite3 with connection management and queue-specific operations.
 */
class Database {
  /**
   * Creates a new Database instance.
   * @param {string} [dbPath=':memory:'] - Path to the SQLite database file
   * @param {Object} [options={}] - Connection options
   * @param {number} [options.busyTimeout=5000] - Milliseconds to wait for a lock held by another connection
   * @param {import('./logger.js').Logger} [options.logger] - Logger for connection errors, defaults to console
   */
  constructor(dbPath = ':memory:', options = {}) {
    this.dbPath = dbPath;
    this.busyTimeout = options.busyTimeout ?? 5000;
    this.logger = createLogger(options.logger);
    this.db = null;
    this.initialized = false;
    this.closed = false;

    if (!Number.isInteger(this.busyTimeout) || this.busyTimeout < 0) {
      throw new TypeError('busyTimeout must be a non-negative integer');
    }
  }

  /**
   * Creates and returns a database connection.
   * Implements lazy connection initialization, applies the busy timeout and,
   * for files, sets WAL mode for better concurrency.
   * @private
   * @returns {BetterSqlite3.Database} The database connection instance
   * @throws {Error} When database connection fails
   */
  _createConnection() {
    if (this.closed) {
      throw new Error(`litequ: database ${this.dbPath} is closed`);
    }
    if (!this.db) {
      try {
        this.db = new BetterSqlite3(this.dbPath);
        this.db.pragma(`busy_timeout = ${this.busyTimeout}`);
        if (!isMemoryPath(this.dbPath)) {
          this.db.pragma('journal_mode = WAL');
        }
      } catch (err) {
        this.logger.error('litequ: database connection error', err);
        this.db = null;
        throw err;
      }
    }
    return this.db;
  }

  /**
   * Executes a SQL statement that modifies the database (INSERT, UPDATE, DELETE).
   * @param {string} sql - The SQL statement to execute
   * @param {Array} [params=[]] - Parameters to bind to the SQL statement
   * @returns {Object} Result object with lastID (last inserted row ID) and changes (number of rows affected)
   * @throws {Error} When database connection is not available
   */
  run(sql, params = []) {
    const db = this._createConnection();
    if (!db) {
      throw new Error('Database connection not available');
    }
    const stmt = db.prepare(sql);
    const result = stmt.run(params);
    return { lastID: result.lastInsertRowid, changes: result.changes };
  }

  /**
   * Executes a SQL SELECT statement and returns the first matching row.
   * @param {string} sql - The SQL SELECT statement to execute
   * @param {Array} [params=[]] - Parameters to bind to the SQL statement
   * @returns {Object|undefined} The first row that matches the query, or undefined if no matches
   * @throws {Error} When database connection is not available
   */
  get(sql, params = []) {
    const db = this._createConnection();
    if (!db) {
      throw new Error('Database connection not available');
    }
    const stmt = db.prepare(sql);
    const result = stmt.get(params);
    return result;
  }

  /**
   * Executes a SQL SELECT statement and returns all matching rows.
   * @param {string} sql - The SQL SELECT statement to execute
   * @param {Array} [params=[]] - Parameters to bind to the SQL statement
   * @returns {Array<Object>} Array of all rows that match the query
   * @throws {Error} When database connection is not available
   */
  all(sql, params = []) {
    const db = this._createConnection();
    if (!db) {
      throw new Error('Database connection not available');
    }
    const stmt = db.prepare(sql);
    const result = stmt.all(params);
    return result;
  }

  /**
   * Initializes the database: creates the queue table and indexes if they
   * don't exist, then runs any schema migrations the file still needs.
   * This method is idempotent - it can be called multiple times safely.
   * @returns {void}
   */
  initialize() {
    if (this.initialized) return;

    const db = this._createConnection();

    db.exec(`
      CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT NOT NULL,
        task_data TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        retry_count INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        next_retry_at DATETIME DEFAULT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_status ON queue (status);
      CREATE INDEX IF NOT EXISTS idx_next_retry ON queue (next_retry_at);
      CREATE INDEX IF NOT EXISTS idx_job_name ON queue (job_name);
    `);

    this._migrate(db);

    this.initialized = true;
  }

  /**
   * Applies pending migrations inside a write transaction. The version is
   * re-read under the lock, so concurrent openers migrate only once.
   * @private
   * @param {any} db - The better-sqlite3 connection
   * @returns {void}
   */
  _migrate(db) {
    if (db.pragma('user_version', { simple: true }) >= SCHEMA_VERSION) {
      return;
    }

    db.transaction(() => {
      const version = db.pragma('user_version', { simple: true });
      if (version >= SCHEMA_VERSION) return;

      for (const migration of MIGRATIONS) {
        if (migration.version > version) {
          migration.up(db);
        }
      }
      db.pragma(`user_version = ${SCHEMA_VERSION}`);
    }).immediate();
  }

  /**
   * Inserts a new task into the queue.
   * @param {string} jobName - Name of the job/worker that will process this task
   * @param {string} taskData - JSON string representation of the task data
   * @returns {number} The ID of the newly inserted task
   */
  insertTask(jobName, taskData) {
    this.initialize();
    const result = this.run(
      'INSERT INTO queue (job_name, task_data) VALUES (?, ?)',
      [jobName, taskData]
    );
    return result.lastID;
  }

  /**
   * Retrieves tasks that are ready to run without claiming them: pending
   * tasks, failed tasks ready for retry, and processing tasks whose lease
   * expired. The queue itself uses `claimTasks()`.
   * @param {number} [limit=5] - Maximum number of tasks to retrieve
   * @param {string} [currentTime=new Date().toISOString()] - Current time in ISO format for retry and lease comparison
   * @param {Array<string>} [jobNames=null] - Optional array of job names to filter by. If null, retrieves tasks for all jobs.
   * @returns {Array<Object>} Array of task objects ready for processing
   */
  getPendingTasks(
    limit = 5,
    currentTime = new Date().toISOString(),
    jobNames = null
  ) {
    this.initialize();
    const filter = jobFilter(jobNames);
    return this.all(
      `
      SELECT * FROM queue
      WHERE ${READY_CONDITION}${filter.sql}
      ORDER BY created_at ASC, id ASC
      LIMIT ?
    `,
      [currentTime, currentTime, ...filter.params, limit]
    );
  }

  /**
   * Atomically claims ready tasks for a worker: marks them `processing` with
   * the worker's lease in a single `UPDATE ... RETURNING` statement, so two
   * processes on one file never claim the same task.
   * @param {number} limit - Maximum number of tasks to claim
   * @param {string} currentTime - Current time in ISO format
   * @param {Array<string> | null} jobNames - Job names to claim tasks for, or null for all jobs
   * @param {{ lockedBy: string, lockedUntil: string }} lease - Worker id and ISO lease expiry
   * @returns {Array<Object>} The claimed task rows in `created_at, id` order
   */
  claimTasks(limit, currentTime, jobNames, { lockedBy, lockedUntil }) {
    this.initialize();
    const filter = jobFilter(jobNames);
    const tasks = this.all(
      `
      UPDATE queue
      SET status = 'processing', locked_by = ?, locked_until = ?,
          updated_at = CURRENT_TIMESTAMP
      WHERE id IN (
        SELECT id FROM queue
        WHERE ${READY_CONDITION}${filter.sql}
        ORDER BY created_at ASC, id ASC
        LIMIT ?
      )
      RETURNING *
    `,
      [lockedBy, lockedUntil, currentTime, currentTime, ...filter.params, limit]
    );
    // RETURNING doesn't guarantee order.
    return tasks.sort((a, b) =>
      a.created_at === b.created_at
        ? a.id - b.id
        : a.created_at < b.created_at
          ? -1
          : 1
    );
  }

  /**
   * Extends a running task's lease, if the worker still holds it.
   * @param {number} id - Task ID
   * @param {string} lockedBy - Worker id that holds the lease
   * @param {string} lockedUntil - New ISO lease expiry
   * @returns {boolean} False if the worker no longer holds the lease
   */
  extendLease(id, lockedBy, lockedUntil) {
    this.initialize();
    return (
      this.run(
        `
      UPDATE queue SET locked_until = ?
      WHERE id = ? AND locked_by = ? AND status = 'processing'
    `,
        [lockedUntil, id, lockedBy]
      ).changes > 0
    );
  }

  /**
   * Sets tasks left in `processing` by a process that stopped (crash, deploy,
   * restart) back to `pending`, so they are picked up again. Only tasks whose
   * lease is missing or expired are restarted; a live lease means another
   * worker is still running the task.
   * @param {string} [currentTime=new Date().toISOString()] - Current time in ISO format
   * @returns {number} Number of tasks that were restarted
   */
  recoverInterruptedTasks(currentTime = new Date().toISOString()) {
    this.initialize();
    return this.run(
      `
      UPDATE queue
      SET status = 'pending', locked_by = NULL, locked_until = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE status = 'processing'
        AND (locked_until IS NULL OR locked_until <= ?)
    `,
      [currentTime]
    ).changes;
  }

  /**
   * Retrieves tasks that still have work to do, for copying to another
   * database: `pending`, `processing` without a live lease, and `failed`
   * with a scheduled retry.
   * @param {string} [currentTime=new Date().toISOString()] - Current time in ISO format
   * @returns {Array<Object>} Task rows in `created_at, id` order
   */
  getOpenTasks(currentTime = new Date().toISOString()) {
    this.initialize();
    return this.all(
      `
      SELECT * FROM queue
      WHERE status = 'pending'
        OR (status = 'processing' AND (locked_until IS NULL OR locked_until <= ?))
        OR (status = 'failed' AND next_retry_at IS NOT NULL)
      ORDER BY created_at ASC, id ASC
    `,
      [currentTime]
    );
  }

  /**
   * Inserts tasks copied from another database in one transaction. They get
   * new ids; `processing` tasks are inserted as `pending`, failed tasks keep
   * their scheduled retry.
   * @param {Array<Object>} tasks - Rows from `getOpenTasks()`
   * @returns {number} Number of tasks inserted
   */
  importTasks(tasks) {
    this.initialize();
    const insert = this.db.prepare(`
      INSERT INTO queue (job_name, task_data, status, retry_count, next_retry_at, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    this.db
      .transaction(() => {
        for (const task of tasks) {
          insert.run(
            task.job_name,
            task.task_data,
            task.status === 'failed' ? 'failed' : 'pending',
            task.retry_count,
            task.next_retry_at,
            task.created_at
          );
        }
      })
      .immediate();
    return tasks.length;
  }

  /**
   * Retrieves the earliest time a task may become claimable: a scheduled
   * retry, or the lease expiry of a task another worker is running.
   * @param {Array<string> | null} [jobNames=null] - Only consider these jobs, or all jobs when null
   * @returns {string|null} ISO timestamp, or null if nothing is scheduled
   */
  getNextWakeTime(jobNames = null) {
    this.initialize();
    const filter = jobFilter(jobNames);
    const row = this.get(
      `
      SELECT MIN(at) AS at FROM (
        SELECT next_retry_at AS at FROM queue
        WHERE status = 'failed' AND next_retry_at IS NOT NULL${filter.sql}
        UNION ALL
        SELECT locked_until AS at FROM queue
        WHERE status = 'processing' AND locked_until IS NOT NULL${filter.sql}
      )
    `,
      [...filter.params, ...filter.params]
    );
    return row?.at || null;
  }

  /**
   * Retrieves the earliest next_retry_at timestamp among failed tasks.
   * Used to schedule the next wake-up when there are no ready tasks.
   * @returns {string|null} ISO timestamp of the earliest next_retry_at or null if none
   */
  getEarliestNextRetryTime() {
    this.initialize();
    const row = this.get(
      `
      SELECT next_retry_at FROM queue
      WHERE status = 'failed' AND next_retry_at IS NOT NULL
      ORDER BY next_retry_at ASC
      LIMIT 1
    `
    );
    return row?.next_retry_at || null;
  }

  /**
   * Updates the status and retry information for a specific task.
   * @param {number} id - The task ID to update
   * @param {string} status - New status ('pending', 'processing', 'completed', 'failed')
   * @param {number} [retryCount=0] - Current retry count for the task
   * @param {string|null} [nextRetryAt=null] - ISO timestamp for next retry attempt, or null if no retry scheduled
   * @param {Object} [options={}] - Update options
   * @param {string} [options.lockedBy] - Only update if this worker still holds the task's lease
   * @returns {Object} Result object with changes count (0 if the lease was lost)
   */
  updateTaskStatus(
    id,
    status,
    retryCount = 0,
    nextRetryAt = null,
    options = {}
  ) {
    this.initialize();
    const params = [status, retryCount, nextRetryAt, id];
    let sql = `
      UPDATE queue
      SET status = ?, retry_count = ?, next_retry_at = ?,
          locked_by = NULL, locked_until = NULL, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `;
    if (options.lockedBy !== undefined) {
      sql += ' AND locked_by = ?';
      params.push(options.lockedBy);
    }
    return this.run(sql, params);
  }

  /**
   * Deletes a task from the queue by its ID.
   * @param {number} id - The ID of the task to delete
   * @returns {Object} Result object with changes count
   */
  deleteTask(id) {
    this.initialize();
    return this.run('DELETE FROM queue WHERE id = ?', [id]);
  }

  /**
   * Retrieves a specific task by its ID.
   * @param {number} id - The ID of the task to retrieve
   * @returns {Object|undefined} The task object if found, undefined otherwise
   */
  getTaskById(id) {
    this.initialize();
    return this.get('SELECT * FROM queue WHERE id = ?', [id]);
  }

  /**
   * Retrieves statistics about tasks grouped by job name and status.
   * @returns {Array<Object>} Array of objects with job_name, status and count properties
   * @example
   * // Returns: [{ job_name: 'email', status: 'pending', count: 5 }, { job_name: 'email', status: 'completed', count: 10 }]
   */
  getTaskStats() {
    this.initialize();
    return this.all(`
      SELECT job_name, status, COUNT(*) as count 
      FROM queue 
      GROUP BY job_name, status
    `);
  }

  /**
   * Deletes completed tasks older than the specified time period.
   * @param {number} [olderThanHours=24] - Tasks older than this many hours will be deleted
   * @returns {Object} Result object with changes count indicating how many tasks were deleted
   */
  cleanupCompletedTasks(olderThanHours = 24) {
    this.initialize();
    const cutoffTime = new Date(
      Date.now() - olderThanHours * 60 * 60 * 1000
    ).toISOString();
    return this.run(
      `
      DELETE FROM queue 
      WHERE status = 'completed' AND updated_at < ?
    `,
      [cutoffTime]
    );
  }

  /**
   * Closes the database connection gracefully. A closed Database can't be
   * reopened; later calls throw instead of silently opening a new connection.
   * @returns {Promise<void>} Promise that resolves when the database is closed
   */
  close() {
    this.closed = true;
    this.initialized = false;

    if (this.db) {
      const dbToClose = this.db;
      this.db = null;

      try {
        dbToClose.close();
      } catch (err) {
        this.logger.error('litequ: error closing database', err);
      }
    }

    return Promise.resolve();
  }
}

export default Database;
