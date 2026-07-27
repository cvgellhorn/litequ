/**
 * LiteQu - A lightweight, persistent queue library for Node.js
 *
 * @module litequ
 * @author Christoph von Gellhorn
 * @version 1.0.0
 */

import Queue, { Job } from './queue.js';
import Database from './db.js';

/**
 * Default export - The main Queue class for task queue management.
 * @type {typeof Queue}
 */
export default Queue;

/**
 * Named exports for Queue, Job, and Database classes.
 * @type {Object}
 * @property {typeof Queue} Queue - The main queue class for task management
 * @property {typeof Job} Job - The job class for named worker types
 * @property {typeof Database} Database - The database class for direct database operations
 */
export { Queue, Job, Database };
