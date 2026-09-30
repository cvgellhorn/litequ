/**
 * Creates a promise that a test resolves by hand.
 * @returns {{ promise: Promise<any>, resolve: (value?: any) => void, reject: (error: Error) => void }}
 */
export function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/**
 * Waits for pending setImmediate callbacks, where the queue schedules work.
 * @param {number} [times=3] - How many macrotask turns to wait
 */
export async function flush(times = 3) {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}
