import BetterSqlite3 from 'better-sqlite3';

/**
 * A function the queue calls to find out whether this process may write to
 * the database file. How to tell depends on the deployment (a primary/replica
 * flag, an environment variable, a file the platform writes, ...), so the
 * application supplies it.
 * @typedef {() => boolean} WritableCheck
 */

/**
 * Returns true for SQLite errors that mean the database can't be written.
 * @param {any} error - Error thrown by better-sqlite3
 * @returns {boolean} Whether the error means "read-only"
 */
function isReadOnlyError(error) {
  const code = typeof error?.code === 'string' ? error.code : '';
  return (
    code.startsWith('SQLITE_READONLY') || code.startsWith('SQLITE_CANTOPEN')
  );
}

/**
 * Checks whether a connection can write by creating and dropping a scratch
 * table inside a savepoint that is rolled back, so nothing is kept.
 *
 * `BEGIN IMMEDIATE` alone is not enough: SQLite grants it on read-only files
 * and on connections opened with `{ readonly: true }` without writing
 * anything, so the probe performs a real write.
 * @param {any} connection - An open better-sqlite3 connection
 * @returns {boolean} False if SQLite refuses the write as read-only
 * @throws {Error} For other errors, such as SQLITE_BUSY after the busy timeout
 */
export function canWrite(connection) {
  try {
    connection.exec('SAVEPOINT litequ_write_probe');
    try {
      connection.exec(
        'CREATE TABLE litequ_write_probe (x INTEGER); DROP TABLE litequ_write_probe;'
      );
    } finally {
      connection.exec(
        'ROLLBACK TO litequ_write_probe; RELEASE litequ_write_probe;'
      );
    }
    return true;
  } catch (error) {
    if (isReadOnlyError(error)) {
      return false;
    }
    throw error;
  }
}

/**
 * Builds a generic writable check that probes the database file itself. Each
 * call opens a short-lived connection, takes the write lock briefly for a
 * write that is rolled back, and closes the connection. A missing file is
 * created if its directory is writable.
 *
 * Not verified against replicated filesystems. If your platform tells you
 * which node is the primary, a `writable` callback based on that is more
 * reliable. Doesn't use file permission bits, which FUSE-based filesystems
 * may not report reliably.
 * @param {string} dbPath - Path of the database file to probe
 * @param {Object} [options={}] - Probe options
 * @param {number} [options.busyTimeout=1000] - Milliseconds to wait for another connection's lock
 * @returns {WritableCheck} A function that returns false when SQLite reports the file read-only or can't open it
 */
export function sqliteWritable(dbPath, options = {}) {
  const busyTimeout = options.busyTimeout ?? 1000;

  return () => {
    let connection;
    try {
      connection = new BetterSqlite3(dbPath);
      connection.pragma(`busy_timeout = ${busyTimeout}`);
      return canWrite(connection);
    } catch (error) {
      if (isReadOnlyError(error)) {
        return false;
      }
      throw error;
    } finally {
      connection?.close();
    }
  };
}
