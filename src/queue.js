import { EventEmitter } from 'events';
import path from 'node:path';
import Database, { isMemoryPath } from './db.js';
import { QueueReadOnlyError } from './errors.js';
import { createLogger } from './logger.js';

const WHEN_READ_ONLY = ['memory', 'throw'];

/**
 * Job class representing a named worker type with its own processor function.
 * Extends EventEmitter to provide event-based notifications for job-specific task lifecycle events.
 *
 * @extends EventEmitter
 * @fires Job#added - When a task is added to this job
 * @fires Job#completed - When a task completes successfully
 * @fires Job#failed - When a task fails after all retries
 * @fires Job#retried - When a task is scheduled for retry
 */
class Job extends EventEmitter {
  /**
   * Creates a new Job instance.
   * @param {Queue} queue - Reference to the parent Queue instance
   * @param {string} name - Name of this job type
   */
  constructor(queue, name) {
    super();
    this.queue = queue;
    this.name = name;
    this.handler = null;
  }

  /**
   * Registers a handler function for this job and starts processing if autoProcess is enabled.
   * @param {Function} handler - Function to process tasks for this job, receives task data as parameter
   * @returns {Promise<void>} Promise that resolves after initial processing setup
   * @throws {Error} When handler is not a function
   */
  async process(handler) {
    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    this.handler = handler;

    // Register this job with the queue
    this.queue._registerJob(this);

    // If autoProcess is enabled, trigger processing
    if (this.queue.autoProcess) {
      return this.queue._processNextBatch();
    }
  }

  /**
   * Adds a new task to this job's queue.
   * @param {*} taskData - The data for the task (will be JSON serialized)
   * @returns {number} The ID of the newly added task
   * @throws {Error} When task insertion fails or the queue is closed
   * @throws {QueueReadOnlyError} When the queue is read-only and `whenReadOnly` is `'throw'`
   * @fires Job#added
   */
  add(taskData) {
    if (this.queue.closed) {
      throw new Error(
        `litequ: cannot add a task to job "${this.name}" because the queue is closed`
      );
    }
    if (!this.queue.writable && this.queue.whenReadOnly === 'throw') {
      throw new QueueReadOnlyError(
        `litequ: cannot add a task to job "${this.name}" because this process is read-only`
      );
    }

    try {
      const taskId = this.queue.db.insertTask(
        this.name,
        JSON.stringify(taskData)
      );

      // Emit on this job instance
      this.emit('added', { taskId, taskData });

      // Bubble event up to queue with jobName
      this.queue.emit('added', { jobName: this.name, taskId, taskData });

      // If auto-processing is enabled and we have a handler
      if (this.queue.autoProcess && this.handler) {
        if (this.queue.isProcessing) {
          // Mark that tasks were added during processing to trigger continuation
          this.queue._tasksAddedDuringProcessing = true;
        } else {
          // Cancel any scheduled wake since we have immediate work now
          this.queue.stopPolling();
          this.queue._scheduleBatch();
        }
      }

      return taskId;
    } catch (error) {
      this.queue._emitError({ error, operation: 'add', jobName: this.name });
      throw error;
    }
  }

  /**
   * Emits an event on this job instance and bubbles it up to the parent queue.
   * @private
   * @param {string} event - Event name
   * @param {Object} data - Event data
   */
  _emit(event, data) {
    // Emit on this job instance
    this.emit(event, data);

    // Bubble event up to queue with jobName
    this.queue.emit(event, { ...data, jobName: this.name });
  }
}

/**
 * Queue class for managing and processing background tasks.
 * Extends EventEmitter to provide event-based notifications for task lifecycle events.
 *
 * @extends EventEmitter
 * @fires Queue#added - When a task is added to the queue
 * @fires Queue#completed - When a task completes successfully
 * @fires Queue#failed - When a task fails after all retries
 * @fires Queue#retried - When a task is scheduled for retry
 * @fires Queue#error - When an error occurs during queue operations
 * @fires Queue#idle - When the queue has finished its work and nothing is running
 * @fires Queue#database-switched - When `switchDatabase()` has swapped the connection
 * @fires Queue#role-change - When the writable role changed and the database was switched
 */
class Queue extends EventEmitter {
  /**
   * Creates a new Queue instance.
   * @param {Object} [options={}] - Configuration options for the queue
   * @param {string} [options.dbPath=':memory:'] - Path to the SQLite database file. Use a file path for persistence.
   * @param {number} [options.maxConcurrent=5] - Maximum number of tasks to process concurrently
   * @param {number} [options.maxRetries=15] - Maximum number of retry attempts for failed tasks
   * @param {number} [options.baseRetryDelay=15_000] - Base delay in milliseconds between retries (exponential backoff)
   * @param {boolean} [options.autoProcess=true] - Whether to automatically process tasks when added
   * @param {boolean} [options.jitter=true] - Whether to add randomness to retry delays
   * @param {number} [options.busyTimeout=5000] - Milliseconds to wait for a lock held by another connection
   * @param {Partial<import('./logger.js').Logger>} [options.logger=console] - Logger with error, warn and info methods
   * @param {boolean} [options.recoverInterrupted=true] - Restart tasks a stopped process left in `processing` when a file database is opened
   * @param {import('./roles.js').WritableCheck} [options.writable] - Returns whether this process may write to `dbPath`; when omitted the queue is always writable
   * @param {string} [options.readOnlyDbPath=':memory:'] - Database used while `writable()` returns false
   * @param {'memory' | 'throw'} [options.whenReadOnly='memory'] - While read-only, store new tasks in `readOnlyDbPath` (`'memory'`) or make `add()` throw `QueueReadOnlyError` (`'throw'`)
   * @param {number} [options.roleCheckInterval=10_000] - Milliseconds between `writable()` checks
   */
  constructor(options = {}) {
    super();
    this.dbPath = options.dbPath || ':memory:';
    this.maxConcurrent = options.maxConcurrent || 5;
    this.maxRetries = options.maxRetries || 15;
    this.baseRetryDelay = options.baseRetryDelay || 15_000; // 15 seconds
    this.autoProcess = options.autoProcess !== false; // defaults to true
    this.jitter = options.jitter !== false; // adds randomness to retry delays

    this.busyTimeout = options.busyTimeout ?? 5000;
    this.logger = createLogger(options.logger);

    this.recoverInterrupted = options.recoverInterrupted !== false;

    this.writablePath = this.dbPath;
    this.readOnlyDbPath = options.readOnlyDbPath || ':memory:';
    this.whenReadOnly = options.whenReadOnly ?? 'memory';
    this.roleCheckInterval = options.roleCheckInterval ?? 10_000;
    this._writableCheck = options.writable ?? null;
    if (!WHEN_READ_ONLY.includes(this.whenReadOnly)) {
      throw new TypeError(
        `whenReadOnly must be one of ${WHEN_READ_ONLY.join(', ')}; got ${this.whenReadOnly}`
      );
    }
    if (this._writableCheck && typeof this._writableCheck !== 'function') {
      throw new TypeError('writable must be a function that returns a boolean');
    }

    // Unsure at startup means read-only: writing to a replica would fail.
    this.writable = this._evaluateWritable() ?? false;
    this.dbPath = this.writable ? this.writablePath : this.readOnlyDbPath;

    this.db = this._openDatabase(this.dbPath, this.recoverInterrupted).db;
    this.currentRunning = 0;
    this.isProcessing = false;
    this.jobs = new Map(); // Map of job name -> Job instance
    this.pollingTimer = null; // used as a one-shot wake-up timer
    this._activeBatch = null; // Promise of the batch currently being processed
    this._tasksAddedDuringProcessing = false; // Flag to track if tasks were added while processing
    this.paused = false;
    this.closed = false;
    this._closePromise = null;
    this._scheduledBatches = 0; // setImmediate batches that haven't started yet
    this._manualDrains = 0; // processOnce() calls in progress
    this._workSinceIdle = false; // whether any task ran since the last idle event
    this._internal = new EventEmitter(); // internal events, safe from removeAllListeners()
    this._switchChain = Promise.resolve(); // serializes switchDatabase() calls
    this._roleSwitching = false;
    this._roleTimer = null;
    this._startRoleCheck();
  }

  /**
   * Opens and initializes a database, optionally restarting interrupted tasks.
   * @private
   * @param {string} dbPath - Path of the database to open
   * @param {boolean} recoverInterrupted - Whether to restart tasks left in `processing`
   * @returns {{ db: Database, recovered: number }} The open database and the number of restarted tasks
   */
  _openDatabase(dbPath, recoverInterrupted) {
    const db = new Database(dbPath, {
      busyTimeout: this.busyTimeout,
      logger: this.logger,
    });
    db.initialize();

    let recovered = 0;
    if (recoverInterrupted && !isMemoryPath(dbPath)) {
      recovered = db.recoverInterruptedTasks();
      if (recovered > 0) {
        this.logger.info(
          `litequ: restarted ${recovered} interrupted task(s) in ${dbPath}`
        );
      }
    }

    return { db, recovered };
  }

  /**
   * Calls the `writable` option.
   * @private
   * @returns {boolean | null} The result, true without a check, or null if the check threw
   */
  _evaluateWritable() {
    if (!this._writableCheck) {
      return true;
    }
    try {
      return Boolean(this._writableCheck());
    } catch (error) {
      this.logger.error(
        'litequ: the writable() check threw; keeping the current role',
        error
      );
      return null;
    }
  }

  /**
   * Starts the timer that re-evaluates `writable()`. The timer doesn't keep
   * the process alive.
   * @private
   * @returns {void}
   */
  _startRoleCheck() {
    if (!this._writableCheck) {
      return;
    }
    this._roleTimer = setInterval(() => {
      this._checkRole();
    }, this.roleCheckInterval);
    this._roleTimer.unref?.();
  }

  /**
   * Re-evaluates `writable()` and switches databases when the role changed.
   * Becoming writable moves open tasks into the file; becoming read-only
   * moves nothing, leaving the file's tasks for the new writer.
   * @private
   * @returns {Promise<void>} Promise that resolves when the check is done
   * @fires Queue#role-change
   */
  async _checkRole() {
    if (this._roleSwitching || this.closed) {
      return;
    }

    const writable = this._evaluateWritable();
    if (writable === null || writable === this.writable) {
      return;
    }

    this._roleSwitching = true;
    try {
      await this.switchDatabase(
        writable ? this.writablePath : this.readOnlyDbPath,
        { moveOpenTasks: writable }
      );
      this.writable = writable;
      this.emit('role-change', { writable });
    } catch (error) {
      if (!this.closed) {
        this.logger.error(
          `litequ: switching to the ${writable ? 'writable' : 'read-only'} database failed`,
          error
        );
      }
    } finally {
      this._roleSwitching = false;
    }
  }

  /**
   * Moves the queue to another database. Pauses, waits until no task is
   * running (a running task writes its status back to the database it came
   * from), opens the new database, copies open tasks over, closes the old
   * connection and resumes, unless the queue was paused before the call.
   * Calls are serialized.
   *
   * Tasks are copied, not deleted: the old database keeps its rows. Copied
   * tasks get new ids and keep `job_name`, `task_data`, `retry_count`,
   * `next_retry_at` and `created_at`; `processing` tasks become `pending`.
   * @param {string} dbPath - Path of the database to switch to
   * @param {Object} [options={}] - Switch options
   * @param {boolean} [options.moveOpenTasks=true] - Copy `pending`, `processing` and scheduled-retry tasks from the old database
   * @param {boolean} [options.recoverInterrupted=true] - Restart tasks left in `processing` in the new database
   * @returns {Promise<void>} Promise that resolves after the switch
   * @throws {Error} When the queue is closed
   * @fires Queue#database-switched
   */
  switchDatabase(dbPath, options = {}) {
    const run = this._switchChain.then(() =>
      this._switchDatabase(dbPath, options)
    );
    this._switchChain = run.then(
      () => {},
      () => {}
    );
    return run;
  }

  /**
   * Implements `switchDatabase()`.
   * @private
   * @param {string} dbPath - Path of the database to switch to
   * @param {{ moveOpenTasks?: boolean, recoverInterrupted?: boolean }} options - Switch options
   * @returns {Promise<void>} Promise that resolves after the switch
   */
  async _switchDatabase(
    dbPath,
    { moveOpenTasks = true, recoverInterrupted = true } = {}
  ) {
    const assertOpen = () => {
      if (this.closed) {
        throw new Error(
          'litequ: cannot switch databases because the queue is closed'
        );
      }
    };
    assertOpen();

    const wasPaused = this.paused;
    this.pause();
    try {
      await this.whenIdle();
      assertOpen();

      const from = this.dbPath;
      const { db: newDb, recovered } = this._openDatabase(
        dbPath,
        recoverInterrupted
      );

      // From here to the swap everything is synchronous, so no task can be
      // added to the old database after it was copied.
      let moved = 0;
      if (moveOpenTasks && !this._isSameFile(from, dbPath)) {
        try {
          moved = newDb.importTasks(this.db.getOpenTasks());
        } catch (error) {
          await newDb.close();
          throw error;
        }
      }

      const oldDb = this.db;
      this.db = newDb;
      this.dbPath = dbPath;
      await oldDb.close();

      this.emit('database-switched', { from, to: dbPath, moved, recovered });
    } finally {
      if (!wasPaused) {
        this.resume();
      }
    }
  }

  /**
   * Whether two paths name the same database file.
   * @private
   * @param {string} a - First path
   * @param {string} b - Second path
   * @returns {boolean} True for the same file; in-memory databases are never the same
   */
  _isSameFile(a, b) {
    return (
      !isMemoryPath(a) &&
      !isMemoryPath(b) &&
      path.resolve(a) === path.resolve(b)
    );
  }

  /**
   * Creates a new named job with its own processor function.
   * @param {string} name - Name of the job type
   * @returns {Job} A new Job instance
   */
  createJob(name) {
    if (this.jobs.has(name)) {
      return this.jobs.get(name);
    }

    const job = new Job(this, name);
    this.jobs.set(name, job);
    return job;
  }

  /**
   * Registers a job with the queue (internal method called by Job.process).
   * @internal - This method is part of the internal API between Job and Queue
   * @param {Job} job - The job instance to register
   */
  _registerJob(job) {
    if (!this.jobs.has(job.name)) {
      this.jobs.set(job.name, job);
    }
  }

  /**
   * Processes every task that is ready now, for all jobs with a registered handler.
   * Runs in batches of up to maxConcurrent and resolves once no ready tasks remain.
   * Works whether or not autoProcess is enabled; if an automatic batch is in
   * flight, it waits for that batch first. Retries that are not yet due are left
   * for a later call. Does nothing while the queue is paused or closed.
   * @returns {Promise<number>} Number of tasks attempted (completed or failed)
   * @throws {TypeError} When called with a handler (removed legacy signature)
   * @fires Queue#error
   */
  async processOnce(...args) {
    if (args.length > 0) {
      throw new TypeError(
        'processOnce() no longer accepts a handler. Register handlers with ' +
          'queue.createJob(name).process(handler), then call queue.processOnce().'
      );
    }

    let processed = 0;
    this._manualDrains++;
    try {
      while (!this.paused && !this.closed) {
        if (this.isProcessing) {
          await this._activeBatch;
          continue;
        }

        const count = await this._processNextBatch(false);
        if (count === 0) {
          break;
        }
        processed += count;
      }
    } finally {
      this._manualDrains--;
      this._checkIdle();
    }
    return processed;
  }

  /**
   * Processes the next batch of available tasks.
   * If a batch is already running, returns that batch's promise instead.
   * @internal - This method is part of the internal API between Job and Queue
   * @param {boolean} [continueInBackground=true] - Schedule the next batch when this one was full
   * @returns {Promise<number>} Promise resolving to the number of tasks in the batch
   * @fires Queue#error
   */
  _processNextBatch(continueInBackground = true) {
    if (this.paused || this.closed) {
      return Promise.resolve(0);
    }
    if (this.isProcessing) {
      return this._activeBatch; // Already processing
    }

    this._activeBatch = this._runBatch(continueInBackground);
    return this._activeBatch;
  }

  /**
   * Fetches and processes one batch of ready tasks.
   * @private
   * @param {boolean} continueInBackground - Schedule the next batch when this one was full
   * @returns {Promise<number>} Promise resolving to the number of tasks in the batch
   * @fires Queue#error
   */
  async _runBatch(continueInBackground) {
    const hasJobHandlers = Array.from(this.jobs.values()).some(
      (job) => job.handler
    );
    if (!hasJobHandlers) {
      return 0;
    }

    this.isProcessing = true;
    this._tasksAddedDuringProcessing = false;

    try {
      const availableSlots = this.maxConcurrent - this.currentRunning;
      if (availableSlots <= 0) {
        return 0;
      }

      const now = new Date().toISOString();

      const jobNamesWithHandlers = Array.from(this.jobs.entries())
        .filter(([, job]) => job.handler)
        .map(([name]) => name);

      const tasks = this.db.getPendingTasks(
        availableSlots,
        now,
        jobNamesWithHandlers
      );

      if (tasks.length > 0) {
        this._workSinceIdle = true;
      }

      const processingPromises = tasks.map((task) => this._processTask(task));
      await Promise.all(processingPromises);

      if (
        continueInBackground &&
        tasks.length === availableSlots &&
        this.currentRunning < this.maxConcurrent
      ) {
        this._scheduleBatch();
      }

      return tasks.length;
    } catch (error) {
      this._emitError({ error, operation: 'process' });
      return 0;
    } finally {
      this.isProcessing = false;

      if (this._tasksAddedDuringProcessing) {
        this._tasksAddedDuringProcessing = false;
        this._scheduleBatch();
      } else {
        this._scheduleNextWake();
      }

      this._checkIdle();
    }
  }

  /**
   * Starts a batch on the next turn of the event loop. While it's pending the
   * queue doesn't count as idle.
   * @private
   * @returns {void}
   */
  _scheduleBatch() {
    this._scheduledBatches++;
    setImmediate(() => {
      this._scheduledBatches--;
      this._processNextBatch();
      this._checkIdle();
    });
  }

  /**
   * Whether no batch, task, scheduled batch or processOnce() call is running.
   * @private
   * @returns {boolean} True when the queue is idle
   */
  _isIdle() {
    return (
      !this.isProcessing &&
      this.currentRunning === 0 &&
      this._scheduledBatches === 0 &&
      this._manualDrains === 0
    );
  }

  /**
   * Emits the internal and public `idle` events if the queue has become idle.
   * The public event fires only if a task ran since the last one.
   * @private
   * @returns {void}
   * @fires Queue#idle
   */
  _checkIdle() {
    if (!this._isIdle()) {
      return;
    }

    this._internal.emit('idle');
    if (this._workSinceIdle) {
      this._workSinceIdle = false;
      this.emit('idle');
    }
  }

  /**
   * Processes a single task.
   * @private
   * @param {Object} task - The task object from the database
   * @returns {Promise<void>} Promise that resolves after task processing
   * @fires Queue#completed
   */
  async _processTask(task) {
    // Status updates go to the database the task came from, even if the
    // queue's connection changes while the handler runs.
    const db = this.db;
    this.currentRunning++;

    try {
      db.updateTaskStatus(task.id, 'processing', task.retry_count, null);

      let taskData;
      try {
        taskData = JSON.parse(task.task_data);
      } catch (parseError) {
        throw new Error(`Invalid task data JSON: ${parseError.message}`);
      }

      const job = this.jobs.get(task.job_name);
      if (!job || !job.handler) {
        throw new Error(`No handler registered for job: ${task.job_name}`);
      }

      const result = await job.handler(taskData);
      if (this._isResultLost(db, task)) {
        return;
      }
      db.updateTaskStatus(task.id, 'completed', task.retry_count, null);

      job._emit('completed', { taskId: task.id, result, taskData });
    } catch (error) {
      await this._handleTaskFailure(task, error, db);
    } finally {
      this.currentRunning--;
    }
  }

  /**
   * Checks whether a task finished after its database was closed, for
   * example after `close({ timeout })` gave up. Its result can't be saved,
   * so the task stays `processing` and is recovered later.
   * @private
   * @param {Database} db - The database the task came from
   * @param {Object} task - The task row
   * @returns {boolean} True if the result can't be saved
   */
  _isResultLost(db, task) {
    if (!db.closed) {
      return false;
    }
    this.logger.warn(
      `litequ: task ${task.id} (${task.job_name}) finished after the queue closed; its result was not saved`
    );
    return true;
  }

  /**
   * Handles task failure by implementing retry logic with exponential backoff.
   * @private
   * @param {Object} task - The failed task object
   * @param {Error} error - The error that caused the task to fail
   * @param {Database} [db=this.db] - The database the task came from
   * @returns {Promise<void>} Promise that resolves after handling the failure
   * @fires Queue#retried
   * @fires Queue#failed
   */
  async _handleTaskFailure(task, error, db = this.db) {
    if (this._isResultLost(db, task)) {
      return;
    }

    const retryCount = task.retry_count + 1;
    const job = this.jobs.get(task.job_name);

    if (retryCount <= this.maxRetries) {
      const baseDelay = this.baseRetryDelay * Math.pow(2, retryCount - 1);
      const jitterDelay = this.jitter
        ? baseDelay * (0.5 + Math.random() * 0.5)
        : baseDelay;
      const delay = Math.floor(jitterDelay);
      const nextRetryAt = new Date(Date.now() + delay).toISOString();

      db.updateTaskStatus(task.id, 'failed', retryCount, nextRetryAt);

      const taskData = this._parseTaskDataForEvent(task);

      const eventData = {
        taskId: task.id,
        taskData,
        retryCount,
        nextRetryAt,
        delay,
        error: error.message,
      };

      if (job) {
        job._emit('retried', eventData);
      } else {
        this.emit('retried', { ...eventData, jobName: task.job_name });
      }

      this._scheduleNextWake();
    } else {
      db.updateTaskStatus(task.id, 'failed', retryCount, null);

      const taskData = this._parseTaskDataForEvent(task);

      const eventData = {
        taskId: task.id,
        error: error.message,
        taskData,
        retryCount,
      };

      if (job) {
        job._emit('failed', eventData);
      } else {
        this.emit('failed', { ...eventData, jobName: task.job_name });
      }
    }
  }

  /**
   * Parses a task's JSON data for an event payload. Invalid JSON is logged
   * and returned as `{ raw }` so the event can still be emitted.
   * @private
   * @param {Object} task - The task row
   * @returns {*} The parsed task data, or `{ raw: string }`
   */
  _parseTaskDataForEvent(task) {
    try {
      return JSON.parse(task.task_data);
    } catch (parseError) {
      this.logger.error(
        `litequ: task ${task.id} has invalid JSON task data`,
        parseError
      );
      return { raw: task.task_data };
    }
  }

  /**
   * Emits an `error` event, or logs the error when nobody listens. An `error`
   * event without listeners would otherwise throw out of the queue.
   * @internal - This method is part of the internal API between Job and Queue
   * @param {{ error: Error, operation: string, jobName?: string }} info - Error details
   * @returns {void}
   * @fires Queue#error
   */
  _emitError(info) {
    if (this.listenerCount('error') > 0) {
      this.emit('error', info);
    } else {
      this.logger.error(`litequ: ${info.operation} failed`, info.error);
    }
  }

  /**
   * Stops the polling timer for new tasks.
   * @returns {void}
   */
  stopPolling() {
    if (this.pollingTimer) {
      clearTimeout(this.pollingTimer);
      this.pollingTimer = null;
    }
  }

  /**
   * Schedule a one-shot wake-up based on the earliest next_retry_at.
   * Uses unref() so it won't keep the process alive when idle.
   * If there are no scheduled retries, no timer is set.
   * @private
   */
  _scheduleNextWake() {
    // Clear any existing timer first
    this.stopPolling();

    // Nothing to schedule if we are not auto-processing or have no handlers
    const hasAnyHandler = Array.from(this.jobs.values()).some(
      (job) => job.handler
    );
    if (!this.autoProcess || !hasAnyHandler || this.paused || this.closed) {
      return;
    }

    // If there are currently tasks running or we're processing, no need to schedule
    if (this.isProcessing || this.currentRunning > 0) {
      return;
    }

    const earliest = this.db.getEarliestNextRetryTime();

    // No scheduled retries, remain idle. New tasks will wake via add().
    if (!earliest) {
      return;
    }

    const delay = Math.max(0, new Date(earliest).getTime() - Date.now());
    const timer = setTimeout(() => {
      // Guard: handler might have been removed/stopped
      const hasHandler = Array.from(this.jobs.values()).some(
        (job) => job.handler
      );
      if (!this.isProcessing && hasHandler) {
        this._processNextBatch();
      }
    }, delay);

    // Do not keep the process alive while waiting
    if (typeof timer.unref === 'function') {
      timer.unref();
    }

    this.pollingTimer = timer;
  }

  /**
   * Retrieves statistics about tasks grouped by their status.
   * @returns {Array<Object>} Array of objects with status and count properties
   */
  getStats() {
    return this.db.getTaskStats();
  }

  /**
   * Retrieves a specific task by its ID.
   * @param {number} id - The ID of the task to retrieve
   * @returns {Object|undefined} The task object if found, undefined otherwise
   */
  getTask(id) {
    return this.db.getTaskById(id);
  }

  /**
   * Deletes completed tasks older than the specified time period.
   * @param {number} [olderThanHours=24] - Tasks older than this many hours will be deleted
   * @returns {Object} Result object with changes count indicating how many tasks were deleted
   */
  cleanup(olderThanHours = 24) {
    return this.db.cleanupCompletedTasks(olderThanHours);
  }

  /**
   * Stops starting new batches and clears the retry wake-up timer. Tasks that
   * are already running finish normally, and `add()` still stores new tasks.
   * @returns {void}
   */
  pause() {
    if (this.closed) {
      return;
    }
    this.paused = true;
    this.stopPolling();
  }

  /**
   * Clears a pause and, when autoProcess is enabled, starts processing ready
   * tasks right away, including tasks added while paused.
   * @returns {void}
   */
  resume() {
    if (this.closed || !this.paused) {
      return;
    }
    this.paused = false;

    if (this.autoProcess) {
      this._processNextBatch();
    }
  }

  /**
   * Resolves once no batch or task is running and no follow-up batch is
   * scheduled. Resolves immediately if the queue is already idle.
   * @returns {Promise<void>} Promise that resolves when the queue is idle
   */
  whenIdle() {
    if (this._isIdle()) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this._internal.once('idle', () => resolve());
    });
  }

  /**
   * Waits for the queue to become idle, up to an optional timeout.
   * @private
   * @param {number} [timeout] - Maximum milliseconds to wait; no limit when omitted
   * @returns {Promise<boolean>} True if the queue became idle, false on timeout
   */
  async _waitForIdle(timeout) {
    if (timeout === undefined || timeout === null) {
      await this.whenIdle();
      return true;
    }

    let timer;
    const timedOut = new Promise((resolve) => {
      timer = setTimeout(() => resolve(false), timeout);
    });
    try {
      return await Promise.race([this.whenIdle().then(() => true), timedOut]);
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Stops every timer the queue owns.
   * @private
   * @returns {void}
   */
  _stopTimers() {
    this.stopPolling();
    if (this._roleTimer) {
      clearInterval(this._roleTimer);
      this._roleTimer = null;
    }
  }

  /**
   * Closes the queue: pauses it, stops its timers, waits for running tasks
   * and closes the database connection. Calling it again returns the same
   * promise. After `close()`, `add()` throws.
   * @param {Object} [options={}] - Close options
   * @param {number} [options.timeout] - Maximum milliseconds to wait for running tasks; no limit when omitted
   * @returns {Promise<void>} Promise that resolves when the queue is fully closed
   */
  close(options = {}) {
    if (!this._closePromise) {
      this._closePromise = this._close(options);
    }
    return this._closePromise;
  }

  /**
   * Implements `close()`.
   * @private
   * @param {{ timeout?: number }} options - Close options
   * @returns {Promise<void>} Promise that resolves when the queue is closed
   */
  async _close({ timeout } = {}) {
    this.pause();
    this.closed = true;
    this._stopTimers();

    const idle = await this._waitForIdle(timeout);
    if (!idle) {
      this.logger.warn(
        `litequ: close() timed out after ${timeout} ms with ${this.currentRunning} task(s) still running`
      );
    }

    await this.db.close();
  }

  /**
   * Gets the current status of the queue.
   * @returns {Object} Status object with currentRunning, maxConcurrent, isProcessing, autoProcess, paused, closed, writable, dbPath, and jobs properties
   */
  get status() {
    const jobsStatus = {};
    for (const [name, job] of this.jobs.entries()) {
      jobsStatus[name] = {
        hasHandler: !!job.handler,
      };
    }

    return {
      currentRunning: this.currentRunning,
      maxConcurrent: this.maxConcurrent,
      isProcessing: this.isProcessing,
      autoProcess: this.autoProcess,
      paused: this.paused,
      closed: this.closed,
      writable: this.writable,
      dbPath: this.dbPath,
      jobs: jobsStatus,
    };
  }
}

export default Queue;
export { Job };
