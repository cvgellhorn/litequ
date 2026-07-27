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
   * @param {number} [options.pollingInterval=5000] - Interval in milliseconds for polling new tasks
   * @param {boolean} [options.autoProcess=true] - Whether to automatically process tasks when added
   * @param {boolean} [options.jitter=true] - Whether to add randomness to retry delays
   */
  constructor(options = {}) {
    super();
    this.dbPath = options.dbPath || './queue.db';
    this.maxConcurrent = options.maxConcurrent || 5;
    this.maxRetries = options.maxRetries || 15;
    this.baseRetryDelay = options.baseRetryDelay || 15_000; // 15 seconds
    this.pollingInterval = options.pollingInterval || 5000; // 5 seconds
    this.autoProcess = options.autoProcess !== false; // defaults to true
    this.jitter = options.jitter !== false; // adds randomness to retry delays

    this.db = new Database(this.dbPath);
    this.currentRunning = 0;
    this.isProcessing = false;
    this.jobs = new Map(); // Map of job name -> Job instance
    this.handler = null; // Deprecated: kept for backward compatibility
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
   * Adds a new task to the queue.
   * @deprecated Use createJob(name).add(taskData) instead
   * @param {*} taskData - The data for the task (will be JSON serialized)
   * @returns {number} The ID of the newly added task
   * @throws {Error} When task insertion fails
   * @fires Queue#added
   * @fires Queue#error
   */
  add(taskData) {
    try {
      // Use a default job name for backward compatibility
      const taskId = this.db.insertTask('default', JSON.stringify(taskData));
      this.emit('added', { taskId, taskData });

      // If auto-processing is enabled and we have a handler
      if (this.autoProcess && this.handler) {
        if (this.isProcessing) {
          // Mark that tasks were added during processing to trigger continuation
          this._tasksAddedDuringProcessing = true;
        } else {
          // Cancel any scheduled wake since we have immediate work now
          this.stopPolling();
          setImmediate(() => this._processNextBatch());
        }
      }

      return taskId;
    } catch (error) {
      this.emit('error', { error, operation: 'add' });
      throw error;
    }
  }

  /**
   * Sets up continuous task processing with the provided handler function.
   * Starts polling for new tasks if autoProcess is enabled.
   * @param {Function} handler - Function to process each task, receives task data as parameter
   * @returns {Promise<void>} Promise that resolves after initial batch processing
   * @throws {Error} When handler is not a function
   */
  async process(handler) {
    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    this.handler = handler;

    if (this.autoProcess) {
      this._startPolling();
    }

    return this._processNextBatch();
  }

  /**
   * Processes a single batch of tasks without setting up continuous processing.
   * @param {Function} handler - Function to process each task, receives task data as parameter
   * @returns {Promise<void>} Promise that resolves after batch processing completes
   * @throws {Error} When handler is not a function
   */
  async processOnce(handler) {
    if (typeof handler !== 'function') {
      throw new Error('Handler must be a function');
    }

    return this._processNextBatch(handler);
  }

  /**
   * Processes the next batch of available tasks.
   * @internal - This method is part of the internal API between Job and Queue
   * @param {Function|null} [oneTimeHandler=null] - Optional one-time handler, otherwise uses instance handler
   * @returns {Promise<void>} Promise that resolves after batch processing
   * @throws {Error} When no handler is provided
   * @fires Queue#error
   */
  async _processNextBatch(oneTimeHandler = null) {
    if (this.isProcessing && !oneTimeHandler) {
      return; // Already processing
    }

    const handlerToUse = oneTimeHandler || this.handler;

    // Check if we have any jobs with handlers or a legacy handler
    const hasJobHandlers = Array.from(this.jobs.values()).some(
      (job) => job.handler
    );
    if (!handlerToUse && !hasJobHandlers) {
      // No handlers available, nothing to process
      return;
    }

    this.isProcessing = true;
    // Reset the flag at the start of each batch
    this._tasksAddedDuringProcessing = false;

    try {
      const availableSlots = this.maxConcurrent - this.currentRunning;
      if (availableSlots <= 0) {
        return;
      }

      const now = new Date().toISOString();

      // Get job names that have handlers registered
      const jobNamesWithHandlers = Array.from(this.jobs.entries())
        .filter(([, job]) => job.handler)
        .map(([name]) => name);

      // Fetch tasks for jobs with handlers, or all tasks if using legacy handler
      const tasks = handlerToUse
        ? this.db.getPendingTasks(availableSlots, now)
        : this.db.getPendingTasks(availableSlots, now, jobNamesWithHandlers);

      const processingPromises = tasks.map((task) =>
        this._processTask(task, handlerToUse)
      );
      await Promise.all(processingPromises);

      // If there might be more tasks, process them
      if (
        tasks.length === availableSlots &&
        this.currentRunning < this.maxConcurrent
      ) {
        setImmediate(() => this._processNextBatch(oneTimeHandler));
      }
    } catch (error) {
      this.emit('error', { error, operation: 'process' });
    } finally {
      if (!oneTimeHandler) {
        this.isProcessing = false;

        // Check if tasks were added during processing and continue immediately
        if (this._tasksAddedDuringProcessing) {
          this._tasksAddedDuringProcessing = false;
          setImmediate(() => this._processNextBatch());
        } else {
          // No new tasks added, schedule next wake based on retry times
          this._scheduleNextWake();
        }
      }
    }
  }

  /**
   * Processes a single task with the provided handler.
   * @private
   * @param {Object} task - The task object from the database
   * @param {Function} legacyHandler - Optional legacy handler function (for backward compatibility)
   * @returns {Promise<void>} Promise that resolves after task processing
   * @fires Queue#completed
   */
  async _processTask(task, legacyHandler = null) {
    this.currentRunning++;

    try {
      this.db.updateTaskStatus(task.id, 'processing', task.retry_count, null);

      let taskData;
      try {
        taskData = JSON.parse(task.task_data);
      } catch (parseError) {
        throw new Error(`Invalid task data JSON: ${parseError.message}`);
      }

      // Determine which handler to use
      let handler;
      let job = null;

      if (legacyHandler) {
        // Use legacy handler for backward compatibility
        handler = legacyHandler;
      } else {
        // Find the job for this task
        job = this.jobs.get(task.job_name);
        if (!job || !job.handler) {
          throw new Error(`No handler registered for job: ${task.job_name}`);
        }
        handler = job.handler;
      }

      const result = await handler(taskData);
      this.db.updateTaskStatus(task.id, 'completed', task.retry_count, null);

      // Emit through job instance if available, which will bubble up to queue
      if (job) {
        job._emit('completed', { taskId: task.id, result, taskData });
      } else {
        // Legacy: emit directly on queue
        this.emit('completed', { taskId: task.id, result, taskData });
      }
    } catch (error) {
      await this._handleTaskFailure(task, error, legacyHandler);
    } finally {
      this.currentRunning--;
    }
  }

  /**
   * Handles task failure by implementing retry logic with exponential backoff.
   * @private
   * @param {Object} task - The failed task object
   * @param {Error} error - The error that caused the task to fail
   * @param {Function} legacyHandler - Optional legacy handler (for backward compatibility)
   * @returns {Promise<void>} Promise that resolves after handling the failure
   * @fires Queue#retried
   * @fires Queue#failed
   */
  async _handleTaskFailure(task, error, legacyHandler = null) {
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

      // Emit through job instance if available, which will bubble up to queue
      if (job && !legacyHandler) {
        job._emit('retried', eventData);
      } else {
        // Legacy: emit directly on queue
        this.emit('retried', eventData);
      }

      // Ensure a wake-up is scheduled for future processing
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

      // Emit through job instance if available, which will bubble up to queue
      if (job && !legacyHandler) {
        job._emit('failed', eventData);
      } else {
        // Legacy: emit directly on queue
        this.emit('failed', eventData);
      }
    }
  }

  /**
   * Starts polling for new tasks at regular intervals.
   * @private
   * @returns {void}
   */
  _startPolling() {
    // Start by scheduling an immediate wake to process any ready tasks now
    const hasAnyHandler =
      this.handler || Array.from(this.jobs.values()).some((job) => job.handler);
    if (!this.isProcessing && hasAnyHandler) {
      setImmediate(() => this._processNextBatch());
    }

    // After that, rely on one-shot wake scheduling instead of a persistent interval
    this._scheduleNextWake();
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
    const hasAnyHandler =
      this.handler || Array.from(this.jobs.values()).some((job) => job.handler);
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
      const hasHandler =
        this.handler ||
        Array.from(this.jobs.values()).some((job) => job.handler);
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
   * @returns {Object} Status object with currentRunning, maxConcurrent, isProcessing, autoProcess, hasHandler, and jobs properties
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
      hasHandler: !!this.handler,
      jobs: jobsStatus,
    };
  }
}

export default Queue;
export { Job };
