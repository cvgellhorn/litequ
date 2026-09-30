/**
 * Logger used by litequ. Any object with `error`, `warn` and `info` methods
 * works, for example `console` or a pino/winston logger.
 * @typedef {Object} Logger
 * @property {(...args: any[]) => void} error - Logs errors
 * @property {(...args: any[]) => void} warn - Logs warnings
 * @property {(...args: any[]) => void} info - Logs informational messages
 */

const LEVELS = /** @type {const} */ (['error', 'warn', 'info']);

/**
 * Normalizes a user-supplied logger. Methods are called on the original
 * object, so loggers that rely on `this` keep working. A missing method falls
 * back to the matching `console` method.
 * @param {Partial<Logger>} [logger] - Logger to wrap, defaults to `console`
 * @returns {Logger} A logger with all three methods
 */
export function createLogger(logger) {
  const target = logger ?? console;
  /** @type {Logger} */
  const normalized = /** @type {any} */ ({});

  for (const level of LEVELS) {
    normalized[level] =
      typeof target[level] === 'function'
        ? (...args) => target[level](...args)
        : (...args) => console[level](...args);
  }

  return normalized;
}
