# LiteQu Migration Design

Date: 2026-07-27  
Status: Approved for planning

## Goal

Create a new package and GitHub repository named **litequ**, owned by **cvgellhorn**, based on the existing `litequu` codebase. Leave `@sturmfrei/litequu` / `sturmfreico/litequu` unchanged. Ship a clean multi-job-only API under the new name, then push a public GitHub repo. npm publish happens later, separately.

## Decisions

| Topic | Decision |
| --- | --- |
| Package name | `litequ` (unscoped) |
| Repo / directory | `/Users/cvgellhorn/dev/litequ` → `github.com/cvgellhorn/litequ` |
| Git history | Fresh start (no history from `litequu`) |
| GitHub | Create public repo and push initial commit |
| npm publish | Out of scope for this work |
| Starting version | `1.0.0` (new package identity) |
| Existing `litequu` | Untouched |

## Approach

Copy source, tests, examples, and config from `litequu` into the new directory (exclude `.git`, `node_modules`, and local DB artifacts). Rename all product references to `litequ`. Surgically remove the legacy single-handler Queue API. Rewrite affected tests/examples/docs. Verify with install/test/lint. Init git, commit, create remote, push.

## Public API (kept)

Multi-job surface only:

- `new Queue(options)`
- `queue.createJob(name)` → `Job`
- `job.add(taskData)`
- `job.process(handler)`
- Queue management: `getStats()`, `getTask(id)`, `cleanup()`, `close()`, `stopPolling()`, `status`
- Named exports: `Queue`, `Job`, `Database` (same shape as today, renamed module)

### Queue-level events (required)

Job lifecycle events continue to bubble to the parent queue with `jobName`:

- `added`, `completed`, `failed`, `retried`, `error`

Listeners may attach at job scope or queue scope. Example:

```javascript
queue.on('failed', (info) => {
  // info.jobName identifies which job failed
});
```

Removing the legacy API must not remove or weaken this bubbling behavior.

## Legacy API (removed)

Delete from `Queue` and all call sites/docs/tests/examples:

- `queue.add(taskData)`
- `queue.process(handler)`
- `queue.processOnce(handler)`
- Internal `this.handler` and all legacy-handler branches in processing, failure handling, polling, wake scheduling, and `status.hasHandler` (queue-level)

No compatibility shims. Callers must use jobs.

## Naming / branding updates

Replace every `litequu` / `@sturmfrei/litequu` / Sturmfrei repo URL reference in the new repo with:

- Package: `litequ`
- Product title: `LiteQu` (README heading / prose); package/import/install strings always `litequ`
- Import: `import Queue from 'litequ'`
- Install: `npm i litequ`
- Repository / bugs / homepage: `https://github.com/cvgellhorn/litequ`

Author remains Christoph von Gellhorn. LICENSE stays MIT.

## Files to copy (baseline)

From `litequu` into `litequ`:

- `src/**`
- `tests/**`
- `examples/**`
- `package.json` (then rewrite)
- Config: `.gitignore`, `.prettierrc`, `eslint.config.js`, `jsconfig.json`, `vitest.config.js`, `LICENSE`, `README.md`

Do **not** copy:

- `.git`
- `node_modules`
- Local DB files (`queue.db`, `example-queue.db`, etc.)

## Docs / tests / examples

- README: multi-job + queue events as the primary story; remove legacy sections and any `litequu` install/import strings
- Examples: multi-job only
- Tests: delete or rewrite suites that only exercise legacy `queue.add` / `queue.process` / `queue.processOnce`; keep coverage for job API, event bubbling, retries, auto-continue, idle/wake behavior

## Verification

Before push:

1. `npm install`
2. `npm test`
3. `npm run lint`

## Ship

1. `git init` (if needed) and initial commit on `main`
2. `gh repo create cvgellhorn/litequ --public --source=. --remote=origin --push`
3. Return the repository URL

Do not publish to npm in this pass.

## Non-goals

- Changing `litequu` in any way
- Deprecation notices or dual-package compatibility layer
- npm release / version tags beyond the initial `1.0.0` in package.json
- Feature work beyond rename + legacy removal + docs/test cleanup
