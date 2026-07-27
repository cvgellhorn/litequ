# LiteQu Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a fresh `litequ` package/repo from `litequu`, rename everything to `litequ`, remove the legacy single-handler Queue API, verify tests/lint, and push `cvgellhorn/litequ`.

**Architecture:** Copy source/tests/examples/config from `/Users/cvgellhorn/dev/litequu` into this repo (no git history). Keep the multi-job `Queue`/`Job` model and queue-level event bubbling. Delete `Queue.add` / `Queue.process` / `Queue.processOnce` and all legacy-handler branches. Manual drain with `autoProcess: false` continues via existing `queue._processNextBatch()` (already used by job-based tests/examples).

**Tech Stack:** Node.js >=22, ESM, better-sqlite3, Vitest, ESLint, Prettier, GitHub (`gh`).

## Global Constraints

- Package name: `litequ` (unscoped), version `1.0.0`
- Product title in docs: `LiteQu`
- Remote: public `https://github.com/cvgellhorn/litequ`
- Leave `/Users/cvgellhorn/dev/litequu` completely untouched
- No npm publish in this work
- Zero remaining `litequu`, `@sturmfrei/litequu`, or `sturmfreico/litequu` strings in the new repo (except historical mentions inside `docs/superpowers/**` design/plan files that intentionally describe the migration source)
- Queue-level event bubbling with `jobName` must remain

## File Structure

| Path | Responsibility |
| --- | --- |
| `src/queue.js` | `Job` + `Queue` (multi-job only after cleanup) |
| `src/db.js` | SQLite persistence (copy as-is) |
| `src/index.js` | Public exports; module named `litequ` |
| `tests/*.js` | Vitest suites; no legacy Queue API usage |
| `examples/*.js` | Runnable demos; multi-job only; relative imports OK |
| `package.json` | `litequ` metadata + scripts |
| `README.md` | LiteQu docs, install `npm i litequ`, no legacy API |
| Config/LICENSE | Copied from source repo |

---

### Task 1: Scaffold copy and package rename

**Files:**
- Create: `src/db.js`, `src/queue.js`, `src/index.js`, `tests/**`, `examples/**`, `.gitignore`, `.prettierrc`, `eslint.config.js`, `jsconfig.json`, `vitest.config.js`, `LICENSE`, `README.md`, `package.json`
- Keep: `docs/superpowers/**` (already present)
- Do not copy: `litequu/.git`, `litequu/node_modules`, `*.db`

**Interfaces:**
- Consumes: `/Users/cvgellhorn/dev/litequu` as read-only source
- Produces: Working tree with copied sources; `package.json` name `litequ` @ `1.0.0`

- [ ] **Step 1: Copy project files from litequu**

```bash
cd /Users/cvgellhorn/dev/litequ
cp -R /Users/cvgellhorn/dev/litequu/src .
cp -R /Users/cvgellhorn/dev/litequu/tests .
cp -R /Users/cvgellhorn/dev/litequu/examples .
cp /Users/cvgellhorn/dev/litequu/.gitignore \
   /Users/cvgellhorn/dev/litequu/.prettierrc \
   /Users/cvgellhorn/dev/litequu/eslint.config.js \
   /Users/cvgellhorn/dev/litequu/jsconfig.json \
   /Users/cvgellhorn/dev/litequu/vitest.config.js \
   /Users/cvgellhorn/dev/litequu/LICENSE \
   /Users/cvgellhorn/dev/litequu/README.md \
   .
```

- [ ] **Step 2: Write `package.json` for litequ**

Overwrite `package.json` with:

```json
{
  "name": "litequ",
  "version": "1.0.0",
  "description": "A simple same-thread queuing system for Node.js using SQLite with retry mechanism and exponential backoff",
  "main": "src/index.js",
  "type": "module",
  "files": [
    "src/**/*",
    "README.md",
    "LICENSE"
  ],
  "scripts": {
    "test": "vitest --no-watch",
    "lint": "eslint src/ tests/",
    "example": "node examples/basic-usage.js"
  },
  "keywords": [
    "queue",
    "sqlite",
    "task",
    "retry",
    "backoff",
    "job"
  ],
  "author": "Christoph von Gellhorn",
  "license": "MIT",
  "dependencies": {
    "better-sqlite3": "^12.2.0"
  },
  "devDependencies": {
    "@eslint/js": "^9.35.0",
    "vitest": "^3.2.4",
    "eslint": "^9.35.0",
    "eslint-config-prettier": "^10.1.8",
    "eslint-plugin-prettier": "^5.5.4",
    "prettier": "^3.6.2"
  },
  "engines": {
    "node": ">=22.0.0"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/cvgellhorn/litequ.git"
  },
  "bugs": {
    "url": "https://github.com/cvgellhorn/litequ/issues"
  },
  "homepage": "https://github.com/cvgellhorn/litequ"
}
```

- [ ] **Step 3: Rename module header in `src/index.js`**

Replace the file header comment with:

```javascript
/**
 * LiteQu - A lightweight, persistent queue library for Node.js
 *
 * @module litequ
 * @author Christoph von Gellhorn
 * @version 1.0.0
 */
```

Leave the exports unchanged for now.

- [ ] **Step 4: Install dependencies**

```bash
cd /Users/cvgellhorn/dev/litequ
npm install
```

Expected: `node_modules/` created; `package-lock.json` with `"name": "litequ"`.

- [ ] **Step 5: Sanity-check baseline tests still pass on copied code**

```bash
npm test
```

Expected: PASS (legacy API still present in this task).

- [ ] **Step 6: Commit scaffold**

```bash
git add src tests examples package.json package-lock.json .gitignore .prettierrc eslint.config.js jsconfig.json vitest.config.js LICENSE README.md src/index.js
git commit -m "$(cat <<'EOF'
Scaffold litequ from litequu with renamed package metadata.

EOF
)"
```

---

### Task 2: Rewrite tests off the legacy Queue API

**Files:**
- Modify: `tests/test-queue.js` (full rewrite to jobs)
- Modify: `tests/test-idle.js` (jobs instead of `queue.add`/`queue.process`)
- Modify: `tests/test-auto-continue.js` (delete legacy-only test)
- Modify: `tests/test-integration.js` (replace remaining `queue.add` / `processOnce` with jobs + `_processNextBatch`)
- Test: `npm test`

**Interfaces:**
- Consumes: Existing `createJob`, `job.add`, `job.process`, `queue._processNextBatch`, queue events
- Produces: Test suite that does not call `queue.add`, `queue.process`, or `queue.processOnce`

- [ ] **Step 1: Replace `tests/test-idle.js` with job-based idle/wake tests**

Overwrite `tests/test-idle.js`:

```javascript
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
```

- [ ] **Step 2: Run idle tests**

```bash
npx vitest --no-watch tests/test-idle.js
```

Expected: PASS.

- [ ] **Step 3: Delete the legacy auto-continue test**

In `tests/test-auto-continue.js`, remove the entire `it('should work with legacy queue.add() and queue.process() methods', ...)` block (from that `it(` through its closing `});` before the suite’s final `});`).

- [ ] **Step 4: Rewrite `tests/test-queue.js` to job API**

Overwrite `tests/test-queue.js` with:

```javascript
import { describe, beforeEach, afterEach, it, expect } from 'vitest';
import Queue from '../src/queue.js';

describe('Queue', () => {
  let queue;
  const testDbPath = ':memory:';

  beforeEach(() => {
    queue = new Queue({
      dbPath: testDbPath,
      autoProcess: false,
      maxRetries: 2,
      baseRetryDelay: 100,
    });
  });

  afterEach(async () => {
    await queue.close();
  });

  describe('constructor', () => {
    it('should create queue with default options', () => {
      const defaultQueue = new Queue();
      expect(defaultQueue.maxConcurrent).toBe(5);
      expect(defaultQueue.maxRetries).toBe(15);
      expect(defaultQueue.baseRetryDelay).toBe(15_000);
      defaultQueue.close();
    });

    it('should create queue with custom options', () => {
      const customQueue = new Queue({
        maxConcurrent: 10,
        maxRetries: 5,
        baseRetryDelay: 2000,
      });
      expect(customQueue.maxConcurrent).toBe(10);
      expect(customQueue.maxRetries).toBe(5);
      expect(customQueue.baseRetryDelay).toBe(2000);
      customQueue.close();
    });
  });

  describe('createJob / job.add', () => {
    it('should add a task and return task ID', () => {
      const job = queue.createJob('test');
      const taskId = job.add({ type: 'test', data: 123 });

      expect(taskId).toBeTypeOf('number');
      expect(taskId).toBeGreaterThan(0);
    });

    it('should emit added event on job and queue', async () => {
      const job = queue.createJob('test');
      const taskData = { type: 'test', data: 123 };

      const addedPromise = new Promise((resolve) => {
        queue.on('added', (info) => {
          expect(info.jobName).toBe('test');
          expect(info.taskId).toBeTypeOf('number');
          expect(info.taskData).toEqual(taskData);
          resolve();
        });
      });

      job.add(taskData);
      await addedPromise;
    });

    it('should handle complex task data', () => {
      const job = queue.createJob('test');
      const complexData = {
        user: { id: 1, name: 'John' },
        actions: ['create', 'update'],
        metadata: { timestamp: Date.now() },
      };

      const taskId = job.add(complexData);
      const task = queue.getTask(taskId);

      expect(JSON.parse(task.task_data)).toEqual(complexData);
    });
  });

  describe('job processing', () => {
    it('should process a single task successfully', async () => {
      const job = queue.createJob('calc');
      job.add({ value: 42 });

      const results = [];
      await job.process(async (data) => {
        results.push(data.value * 2);
        return data.value * 2;
      });
      await queue._processNextBatch();

      expect(results).toHaveLength(1);
      expect(results[0]).toBe(84);
    });

    it('should emit completed event on success with jobName', async () => {
      const job = queue.createJob('calc');
      const completedPromise = new Promise((resolve) => {
        queue.on('completed', (info) => {
          expect(info.jobName).toBe('calc');
          expect(info.taskId).toBeTypeOf('number');
          expect(info.result).toBe(20);
          expect(info.taskData).toEqual({ value: 10 });
          resolve();
        });
      });

      job.add({ value: 10 });
      await job.process(async (data) => data.value * 2);
      await queue._processNextBatch();
      await completedPromise;
    });

    it('should retry failed tasks', async () => {
      const job = queue.createJob('flaky');
      let attempts = 0;

      job.add({ shouldFail: true });
      await job.process(async () => {
        attempts++;
        if (attempts < 2) {
          throw new Error('temporary failure');
        }
        return 'ok';
      });

      await queue._processNextBatch();
      expect(attempts).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 150));
      await queue._processNextBatch();
      expect(attempts).toBe(2);
    });

    it('should emit failed after max retries', async () => {
      const job = queue.createJob('doomed');
      const failedPromise = new Promise((resolve) => {
        queue.on('failed', (info) => {
          expect(info.jobName).toBe('doomed');
          expect(info.error).toBeTruthy();
          resolve();
        });
      });

      job.add({ alwaysFail: true });
      await job.process(async () => {
        throw new Error('always fails');
      });

      for (let i = 0; i < 5; i++) {
        await queue._processNextBatch();
        await new Promise((resolve) => setTimeout(resolve, 120));
      }

      await failedPromise;
    });
  });

  describe('status / stats / cleanup', () => {
    it('should report job handler status', async () => {
      const job = queue.createJob('status');
      expect(queue.status.jobs.status?.hasHandler).toBeFalsy();

      await job.process(async () => 'ok');
      expect(queue.status.jobs.status.hasHandler).toBe(true);
      expect(queue.status.autoProcess).toBe(false);
    });

    it('should return stats and cleanup completed tasks', async () => {
      const job = queue.createJob('stats');
      job.add({ n: 1 });
      await job.process(async () => 'done');
      await queue._processNextBatch();

      const stats = queue.getStats();
      expect(Array.isArray(stats)).toBe(true);

      const result = queue.cleanup(0);
      expect(result).toBeTruthy();
    });
  });
});
```

- [ ] **Step 5: Fix legacy usages in `tests/test-integration.js`**

Find any `queue.add(...)` / `newQueue.add(...)` / `processOnce(...)` and replace with job-based equivalents.

For the concurrency test that currently does:

```javascript
await highConcurrencyQueue.processOnce(handler);
```

Use:

```javascript
const job = highConcurrencyQueue.createJob('concurrent');
// add tasks via job.add(...) instead of any queue.add
await job.process(handler);
await highConcurrencyQueue._processNextBatch();
```

For the persistence-style test that uses `queue.add` / `newQueue.add` / `processOnce`, switch to:

```javascript
const job = queue.createJob('persist');
job.add({ persistent: true, data: 'test1' });
job.add({ persistent: true, data: 'test2' });
// ...
const newJob = newQueue.createJob('persist');
newJob.add({ persistent: true, data: 'test1' });
newJob.add({ persistent: true, data: 'test2' });
await newJob.process(async (taskData) => {
  /* same body as before */
});
await newQueue._processNextBatch();
```

Ensure no remaining matches:

```bash
rg "queue\.(add|process|processOnce)\(|\.processOnce\(" tests/
```

Expected: no matches (aside from possibly `_processNextBatch`).

- [ ] **Step 6: Run full test suite (legacy API still in source — tests must not call it)**

```bash
npm test
```

Expected: PASS.

- [ ] **Step 7: Commit test rewrites**

```bash
git add tests/
git commit -m "$(cat <<'EOF'
Rewrite tests to use multi-job API only.

EOF
)"
```

---

### Task 3: Remove legacy Queue API from `src/queue.js`

**Files:**
- Modify: `src/queue.js`
- Test: `tests/test-queue.js` (add absence checks), `npm test`

**Interfaces:**
- Consumes: Job-only processing path
- Produces: `Queue` without `add` / `process` / `processOnce` / `handler` / `_startPolling`; `_processNextBatch()` with no handler argument; `_processTask(task)` / `_handleTaskFailure(task, error)` job-only

- [ ] **Step 1: Write failing tests that legacy methods are gone**

Append to `tests/test-queue.js` inside the top-level `describe('Queue', ...)`:

```javascript
  describe('legacy API removed', () => {
    it('should not expose queue.add, queue.process, or queue.processOnce', () => {
      expect(queue.add).toBeUndefined();
      expect(queue.process).toBeUndefined();
      expect(queue.processOnce).toBeUndefined();
    });

    it('should not expose queue-level handler state', () => {
      expect(queue.handler).toBeUndefined();
      expect(Object.prototype.hasOwnProperty.call(queue.status, 'hasHandler')).toBe(
        false
      );
    });
  });
```

- [ ] **Step 2: Run the new tests — expect FAIL**

```bash
npx vitest --no-watch tests/test-queue.js -t "legacy API removed"
```

Expected: FAIL (`queue.add` / `process` / `processOnce` still functions; `handler` still present).

- [ ] **Step 3: Delete legacy public methods and `this.handler` from Queue constructor**

In `src/queue.js` `Queue` constructor, remove:

```javascript
this.handler = null; // Deprecated: kept for backward compatibility
```

Delete the entire methods:

- `add(taskData) { ... }`
- `async process(handler) { ... }`
- `async processOnce(handler) { ... }`
- `_startPolling() { ... }` (only called by legacy `process`)

- [ ] **Step 4: Simplify `_processNextBatch` to jobs-only**

Replace `_processNextBatch` with:

```javascript
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
```

- [ ] **Step 5: Simplify `_processTask` and `_handleTaskFailure`**

Replace `_processTask` with:

```javascript
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
```

Replace `_handleTaskFailure` with:

```javascript
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
```

- [ ] **Step 6: Remove legacy handler checks from wake scheduling and `status`**

In `_scheduleNextWake`, replace every:

```javascript
this.handler || Array.from(this.jobs.values()).some((job) => job.handler)
```

with:

```javascript
Array.from(this.jobs.values()).some((job) => job.handler)
```

Replace the `status` getter with:

```javascript
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
```

- [ ] **Step 7: Grep source for leftover legacy markers**

```bash
rg "legacy|this\.handler|processOnce|Deprecated" src/
```

Expected: no matches related to the removed API.

- [ ] **Step 8: Run tests — expect PASS**

```bash
npm test
```

Expected: PASS, including `legacy API removed`.

- [ ] **Step 9: Commit**

```bash
git add src/queue.js tests/test-queue.js
git commit -m "$(cat <<'EOF'
Remove legacy single-handler Queue API.

EOF
)"
```

---

### Task 4: Rename docs and examples to LiteQu / litequ

**Files:**
- Modify: `README.md` (full rewrite of branding + remove legacy sections)
- Modify: `examples/*.js` only if they contain `litequu` strings (they import relative `../src/index.js` today — keep that; update any console titles mentioning LiteQuu if present)
- Verify: no `litequu` outside `docs/superpowers/**`

**Interfaces:**
- Consumes: Multi-job API only
- Produces: Docs/examples that install/import `litequ`

- [ ] **Step 1: Rewrite README branding and install/import**

In `README.md`:

1. Change `# LiteQuu` → `# LiteQu`
2. Replace every `@sturmfrei/litequu` / `litequu` package import with `litequ`
3. Replace install block with:

```bash
npm i litequ
```

4. Quick start import:

```javascript
import Queue from 'litequ';
```

5. Delete the entire section `### Single-Handler API (Legacy, still supported)` and its code fence.
6. Delete API subsections `#### add(taskData) (Legacy)`, `#### process(handler) (Legacy)`, `#### processOnce(handler) (Legacy)`.
7. In Best Practices / examples later in the README, rewrite any remaining `queue.process` / `queue.add` snippets to `createJob` + `job.process` / `job.add`. Keep queue-level `queue.on('failed' | 'completed' | ...)` examples.
8. Ensure a clear Events section still shows queue-level bubbling with `info.jobName`.

- [ ] **Step 2: Scan repo for old names (excluding design/plan docs)**

```bash
rg -n "litequu|@sturmfrei|sturmfreico|LiteQuu" --glob '!docs/superpowers/**' --glob '!node_modules/**'
```

Expected: no matches. Fix any leftovers in `README.md`, `src/`, `examples/`, `package.json`, etc.

- [ ] **Step 3: Commit docs**

```bash
git add README.md examples/
git commit -m "$(cat <<'EOF'
Rebrand docs and examples to litequ; drop legacy API docs.

EOF
)"
```

---

### Task 5: Verify and push public GitHub repo

**Files:**
- No source changes expected; may touch `package-lock.json` only if install rewrote it

**Interfaces:**
- Produces: `https://github.com/cvgellhorn/litequ` with `main` pushed

- [ ] **Step 1: Final verification**

```bash
cd /Users/cvgellhorn/dev/litequ
npm test
npm run lint
rg -n "litequu|@sturmfrei|sturmfreico|LiteQuu" --glob '!docs/superpowers/**' --glob '!node_modules/**'
```

Expected: tests PASS, lint PASS (or only pre-existing style issues fixed if lint fails on edited files), rg empty.

- [ ] **Step 2: Confirm litequu source repo unchanged**

```bash
git -C /Users/cvgellhorn/dev/litequu status
```

Expected: clean working tree / no modifications from this work.

- [ ] **Step 3: Create GitHub repo and push**

```bash
cd /Users/cvgellhorn/dev/litequ
git branch -M main
gh repo create cvgellhorn/litequ --public --source=. --remote=origin --push
```

If `origin` already exists from `create_project`, use:

```bash
gh repo create cvgellhorn/litequ --public --source=. --remote=origin --push
```

or if remote exists empty:

```bash
gh repo create cvgellhorn/litequ --public
git push -u origin main
```

Expected: remote created; push succeeds.

- [ ] **Step 4: Return the URL**

```bash
gh repo view cvgellhorn/litequ --json url -q .url
```

Expected: `https://github.com/cvgellhorn/litequ`

Do **not** run `npm publish`.

---

## Spec coverage checklist

| Spec requirement | Task |
| --- | --- |
| New directory `/Users/cvgellhorn/dev/litequ` | Task 1 (already rooted) |
| Package `litequ` @ `1.0.0` | Task 1 |
| Fresh git history | Task 1+ (no litequu history copied) |
| Leave litequu untouched | Task 5 Step 2 |
| Remove `add`/`process`/`processOnce` + legacy handler branches | Task 3 |
| Keep queue event bubbling | Tasks 2–3 (tests assert `jobName`) |
| Rename all product references | Tasks 1, 4 |
| Rewrite tests/examples/docs | Tasks 2, 4 |
| `npm test` + `npm run lint` | Task 5 |
| Public GitHub push, no npm publish | Task 5 |
