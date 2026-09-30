import BetterSqlite3 from 'better-sqlite3';

/**
 * Creates a database file with the litequu 2.x schema (user_version 0) and
 * inserts the given rows, so tests can check how newer versions open it.
 * @param {string} file - Path of the database file to create
 * @param {Array<Object>} [rows=[]] - Rows with job_name, task_data and optional status, retry_count, next_retry_at
 */
export function createLegacyDatabase(file, rows = []) {
  const db = new BetterSqlite3(file);
  db.pragma('journal_mode = WAL');
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

  const insert = db.prepare(`
    INSERT INTO queue (job_name, task_data, status, retry_count, next_retry_at)
    VALUES (@job_name, @task_data, @status, @retry_count, @next_retry_at)
  `);
  for (const row of rows) {
    insert.run({
      status: 'pending',
      retry_count: 0,
      next_retry_at: null,
      ...row,
    });
  }

  db.close();
}

/**
 * Opens a database file directly, bypassing litequ, for assertions.
 * @param {string} file - Path of the database file
 * @returns {BetterSqlite3.Database} A raw better-sqlite3 connection; close it after use
 */
export function openRaw(file) {
  return new BetterSqlite3(file);
}
