import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/**
 * Creates temporary directories for a test and removes them afterwards.
 * Call `cleanup()` from `afterEach`.
 */
export function createTempDirs() {
  const dirs = [];

  return {
    /** @returns {string} Path of a new, empty temporary directory */
    make() {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'litequ-'));
      dirs.push(dir);
      return dir;
    },

    /** @returns {string} Path of a database file inside a new temporary directory */
    dbFile(name = 'queue.db') {
      return path.join(this.make(), name);
    },

    cleanup() {
      for (const dir of dirs.splice(0)) {
        fs.chmodSync(dir, 0o700);
        fs.rmSync(dir, { recursive: true, force: true });
      }
    },
  };
}
