import { describe, it, expect, afterEach } from 'vitest';
import Queue from '../src/queue.js';

describe('Idle behavior and wake scheduling', () => {
  let queue;

  afterEach(async () => {
    if (queue) {
      await queue.close();
      queue = null;
    }
  });

  it('should not keep the process alive when idle (no timers when empty)', async () => {
    queue = new Queue({ dbPath: ':memory:', autoProcess: true });

    // Provide a no-op handler and start processing
    await queue.process(async () => {
      return 'noop';
    });

    // Give the immediate tick a moment to run
    await new Promise((resolve) => setTimeout(resolve, 20));

    // With no tasks and no scheduled retries, there should be no timer
    expect(queue.pollingTimer).toBeNull();
  });

  it('should schedule an unref-ed wake and process retries in the future', async () => {
    queue = new Queue({
      dbPath: ':memory:',
      autoProcess: true,
      baseRetryDelay: 50,
      jitter: false,
      maxRetries: 1,
    });

    let attempts = 0;

    await queue.process(async () => {
      attempts++;
      throw new Error('fail');
    });

    // Add a task that will fail and schedule a retry
    queue.add({ foo: 'bar' });

    // Wait for first attempt to fail and schedule the wake
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(attempts).toBeGreaterThanOrEqual(1);

    // A one-shot timer should be scheduled and unref'ed
    expect(queue.pollingTimer).toBeTruthy();
    if (queue.pollingTimer && typeof queue.pollingTimer.hasRef === 'function') {
      expect(queue.pollingTimer.hasRef()).toBe(false);
    }

    // After the delay passes, the retry should have been attempted automatically
    await new Promise((resolve) => setTimeout(resolve, 90));
    expect(attempts).toBeGreaterThanOrEqual(2);
  });
});
