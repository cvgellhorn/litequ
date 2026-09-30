import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import BetterSqlite3 from 'better-sqlite3';

/**
 * A function the queue calls to find out whether this process may write to
 * the database file.
 * @typedef {() => boolean} WritableCheck
 */

/**
 * Builds a writable check for LiteFS. LiteFS writes `<dir>/.primary` only on
 * replicas, containing the primary's hostname; on the primary the file is
 * absent. This node is writable when the file can't be read or when its
 * trimmed contents equal `os.hostname()`, the same rule as `litefs-js`'s
 * `getInstanceInfoSync`. The file is read on every call.
 * @param {string} [dir=process.env.LITEFS_DIR] - The LiteFS mount directory. When unset, the check always returns true, so local development and tests work unchanged.
 * @returns {WritableCheck} A function that returns true on the primary
 */
export function litefsWritable(dir = process.env.LITEFS_DIR) {
  if (!dir) {
    return () => true;
  }

  const primaryFile = path.join(dir, '.primary');
  return () => {
    let primary;
    try {
      primary = fs.readFileSync(primaryFile, 'utf8').trim();
    } catch {
      // Like litefs-js: no readable .primary file means this is the primary.
      return true;
    }
    return primary === os.hostname();
  };
}

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
 * Not verified against a LiteFS replica; LiteFS users should prefer
 * `litefsWritable`. Doesn't use file permission bits, which FUSE mounts such
 * as LiteFS don't report reliably.
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
