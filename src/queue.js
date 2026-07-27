import { EventEmitter } from 'events';
import Database from './db.js';

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
   * @throws {Error} When task insertion fails
   * @fires Job#added
   */
  add(taskData) {
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
          setImmediate(() => this.queue._processNextBatch());
        }
      }

      return taskId;
    } catch (error) {
      this.queue.emit('error', {
        error,
        operation: 'add',
        jobName: this.name,
      });
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
 */
class Queue extends EventEmitter {
  /**
   * Creates a new Queue instance.
   * @param {Object} [options={}] - Configuration options for the queue
   * @param {string} [options.dbPath='./queue.db'] - Path to the SQLite database file
   * @param {number} [options.maxConcurrent=5] - Maximum number of tasks to process concurrently
   * @param {number} [options.maxRetries=15] - Maximum number of retry attempts for failed tasks
   * @param {number} [options.baseRetryDelay=15_000] - Base delay in milliseconds between retries (exponential backoff)
   * @param {boolean} [options.autoProcess=true] - Whether to automatically process tasks when added
   * @param {boolean} [options.jitter=true] - Whether to add randomness to retry delays
   */
  constructor(options = {}) {
    super();
    this.dbPath = options.dbPath || './queue.db';
    this.maxConcurrent = options.maxConcurrent || 5;
    this.maxRetries = options.maxRetries || 15;
    this.baseRetryDelay = options.baseRetryDelay || 15_000; // 15 seconds
    this.autoProcess = options.autoProcess !== false; // defaults to true
    this.jitter = options.jitter !== false; // adds randomness to retry delays

    this.db = new Database(this.dbPath);
    this.currentRunning = 0;
    this.isProcessing = false;
    this.jobs = new Map(); // Map of job name -> Job instance
    this.pollingTimer = null; // used as a one-shot wake-up timer
    this._tasksAddedDuringProcessing = false; // Flag to track if tasks were added while processing
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
   * Processes the next batch of available tasks.
   * @internal - This method is part of the internal API between Job and Queue
   * @returns {Promise<void>} Promise that resolves after batch processing
   * @fires Queue#error
   */
  async _processNextBatch() {
    if (this.isProcessing) {
      return; // Already processing
    }

    const hasJobHandlers = Array.from(this.jobs.values()).some(
      (job) => job.handler
    );
    if (!hasJobHandlers) {
      return;
    }

    this.isProcessing = true;
    this._tasksAddedDuringProcessing = false;

    try {
      const availableSlots = this.maxConcurrent - this.currentRunning;
      if (availableSlots <= 0) {
        return;
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

      const processingPromises = tasks.map((task) => this._processTask(task));
      await Promise.all(processingPromises);

      if (
        tasks.length === availableSlots &&
        this.currentRunning < this.maxConcurrent
      ) {
        setImmediate(() => this._processNextBatch());
      }
    } catch (error) {
      this.emit('error', { error, operation: 'process' });
    } finally {
      this.isProcessing = false;

      if (this._tasksAddedDuringProcessing) {
        this._tasksAddedDuringProcessing = false;
        setImmediate(() => this._processNextBatch());
      } else {
        this._scheduleNextWake();
      }
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
    this.currentRunning++;

    try {
      this.db.updateTaskStatus(task.id, 'processing', task.retry_count, null);

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
      this.db.updateTaskStatus(task.id, 'completed', task.retry_count, null);

      job._emit('completed', { taskId: task.id, result, taskData });
    } catch (error) {
      await this._handleTaskFailure(task, error);
    } finally {
      this.currentRunning--;
    }
  }

  /**
   * Handles task failure by implementing retry logic with exponential backoff.
   * @private
   * @param {Object} task - The failed task object
   * @param {Error} error - The error that caused the task to fail
   * @returns {Promise<void>} Promise that resolves after handling the failure
   * @fires Queue#retried
   * @fires Queue#failed
   */
  async _handleTaskFailure(task, error) {
    const retryCount = task.retry_count + 1;
    const job = this.jobs.get(task.job_name);

    if (retryCount <= this.maxRetries) {
      const baseDelay = this.baseRetryDelay * Math.pow(2, retryCount - 1);
      const jitterDelay = this.jitter
        ? baseDelay * (0.5 + Math.random() * 0.5)
        : baseDelay;
      const delay = Math.floor(jitterDelay);
      const nextRetryAt = new Date(Date.now() + delay).toISOString();

      this.db.updateTaskStatus(task.id, 'failed', retryCount, nextRetryAt);

      let taskData;
      try {
        taskData = JSON.parse(task.task_data);
      } catch (parseError) {
        console.error('Error parsing task data:', parseError);
        taskData = { raw: task.task_data };
      }

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
      this.db.updateTaskStatus(task.id, 'failed', retryCount, null);

      let taskData;
      try {
        taskData = JSON.parse(task.task_data);
      } catch (parseError) {
        console.error('Error parsing task data:', parseError);
        taskData = { raw: task.task_data };
      }

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
    if (!this.autoProcess || !hasAnyHandler) {
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
   * Gracefully closes the queue by stopping polling and waiting for running tasks to complete.
   * @returns {Promise<void>} Promise that resolves when the queue is fully closed
   */
  async close() {
    this.stopPolling();

    // Wait for current tasks to finish
    while (this.currentRunning > 0) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return this.db.close();
  }

  /**
   * Gets the current status of the queue.
   * @returns {Object} Status object with currentRunning, maxConcurrent, isProcessing, autoProcess, and jobs properties
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
      jobs: jobsStatus,
    };
  }
}

export default Queue;
export { Job };
