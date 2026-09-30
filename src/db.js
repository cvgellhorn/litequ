import BetterSqlite3 from 'better-sqlite3';
import { createLogger } from './logger.js';

/**
 * Current schema version, stored in `PRAGMA user_version`.
 * Version 0 is the unversioned litequu 2.x schema.
 * @type {number}
 */
export const SCHEMA_VERSION = 1;

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
];

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
   * Retrieves pending tasks from the queue, including failed tasks ready for retry.
   * @param {number} [limit=5] - Maximum number of tasks to retrieve
   * @param {string} [currentTime=new Date().toISOString()] - Current time in ISO format for retry comparison
   * @param {Array<string>} [jobNames=null] - Optional array of job names to filter by. If null, retrieves tasks for all jobs.
   * @returns {Array<Object>} Array of task objects ready for processing
   */
  getPendingTasks(
    limit = 5,
    currentTime = new Date().toISOString(),
    jobNames = null
  ) {
    this.initialize();

    let sql = `
      SELECT * FROM queue 
      WHERE (status = 'pending' OR (status = 'failed' AND next_retry_at <= ?))
    `;
    /** @type {Array<string|number>} */
    const params = [currentTime];

    // Add job name filter if provided
    if (jobNames && jobNames.length > 0) {
      const placeholders = jobNames.map(() => '?').join(', ');
      sql += ` AND job_name IN (${placeholders})`;
      params.push(...jobNames);
    }

    sql += `
      ORDER BY created_at ASC 
      LIMIT ?
    `;
    params.push(limit);

    return this.all(sql, params);
  }

  /**
   * Sets tasks left in `processing` by a process that stopped (crash, deploy,
   * restart) back to `pending`, so they are picked up again.
   * @returns {number} Number of tasks that were restarted
   */
  recoverInterruptedTasks() {
    this.initialize();
    return this.run(
      `
      UPDATE queue
      SET status = 'pending', updated_at = CURRENT_TIMESTAMP
      WHERE status = 'processing'
    `
    ).changes;
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
   * @returns {Object} Result object with changes count
   */
  updateTaskStatus(id, status, retryCount = 0, nextRetryAt = null) {
    this.initialize();
    return this.run(
      `
      UPDATE queue 
      SET status = ?, retry_count = ?, next_retry_at = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `,
      [status, retryCount, nextRetryAt, id]
    );
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
