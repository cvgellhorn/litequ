/**
 * Thrown by `job.add()` when the queue is read-only and was configured with
 * `whenReadOnly: 'throw'`. Nothing is stored when it's thrown.
 */
export class QueueReadOnlyError extends Error {
  /**
   * @param {string} message - Error message
   */
  constructor(message) {
    super(message);
    this.name = 'QueueReadOnlyError';
    /** @type {string} */
    this.code = 'LITEQU_READ_ONLY';
  }
}
