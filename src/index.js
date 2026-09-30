/**
 * LiteQu - A lightweight, persistent queue library for Node.js
 *
 * @module litequ
 * @author Christoph von Gellhorn
 */

import Queue, { Job } from './queue.js';
import Database from './db.js';
import { QueueReadOnlyError } from './errors.js';
import { sqliteWritable } from './roles.js';

/**
 * Default export - The main Queue class for task queue management.
 * @type {typeof Queue}
 */
export default Queue;

/**
 * Named exports.
 * - `Queue`: the main queue class for task management
 * - `Job`: the job class for named worker types
 * - `Database`: the database class for direct database operations
 * - `QueueReadOnlyError`: thrown by `add()` on a read-only queue with `whenReadOnly: 'throw'`
 * - `sqliteWritable`: a generic `writable` check that probes the database file
 */
export { Queue, Job, Database, QueueReadOnlyError, sqliteWritable };
