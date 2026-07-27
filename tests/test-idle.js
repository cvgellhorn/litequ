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
    const job = queue.createJob('idle');

    await job.process(async () => 'noop');

    await new Promise((resolve) => setTimeout(resolve, 20));

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

    const job = queue.createJob('retry');
    let attempts = 0;

    await job.process(async () => {
      attempts++;
      throw new Error('fail');
    });

    job.add({ foo: 'bar' });

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(attempts).toBeGreaterThanOrEqual(1);

    expect(queue.pollingTimer).toBeTruthy();
    if (queue.pollingTimer && typeof queue.pollingTimer.hasRef === 'function') {
      expect(queue.pollingTimer.hasRef()).toBe(false);
    }

    await new Promise((resolve) => setTimeout(resolve, 90));
    expect(attempts).toBeGreaterThanOrEqual(2);
  });
});
