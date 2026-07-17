# QueueCTL — Architecture & Design

## Overview

QueueCTL is a CLI-based background job queue system built with **Node.js** and **SQLite**. It uses two npm dependencies: `better-sqlite3` (synchronous SQLite bindings) and `commander` (CLI framework).

## Why Node.js + SQLite?

| Concern | Decision | Rationale |
|---------|----------|-----------|
| **Language** | Node.js | Async I/O for non-blocking worker loops, `child_process` for command execution, native signal handling |
| **Storage** | SQLite via `better-sqlite3` | ACID transactions, file-level persistence, synchronous API ideal for CLI tools, built-in write locking |
| **CLI** | `commander` | Industry-standard CLI framework with automatic `--help` generation and subcommand support |
| **Concurrency** | Detached OS processes | Workers spawn as independent detached processes, true parallelism without shared memory concerns |

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Terminal                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                   src/cli.js                             │
│              (CLI Parser & Router)                       │
│                                                         │
│  • Commander-based command routing                      │
│  • Spawns worker processes (detached: true)             │
│  • Sends SIGTERM for graceful shutdown                  │
│  • Formats status/list/dlq output tables                │
└────────────────────────┬────────────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
┌──────────────────────┐  ┌──────────────────────┐
│    src/worker.js     │  │    src/worker.js     │
│   (Worker Process)   │  │   (Worker Process)   │
│                      │  │                      │
│ • Polls for jobs     │  │ • Polls for jobs     │
│ • Executes commands  │  │ • Executes commands  │
│ • Signal handling    │  │ • Signal handling    │
│ • Heartbeat updates  │  │ • Heartbeat updates  │
└──────────┬───────────┘  └──────────┬───────────┘
           └────────────┬────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  src/database.js                         │
│               (Data Access Layer)                        │
│                                                         │
│  • Schema init (jobs, workers, config tables)           │
│  • Atomic job acquisition (immediate transactions)     │
│  • Retry scheduling with exponential backoff            │
│  • Dead worker detection & orphan job recovery          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              .queuectl/queuectl.db                       │
│                 (SQLite - WAL mode)                      │
│                                                         │
│  Tables: jobs | workers | config                        │
│  Persists across restarts                               │
└─────────────────────────────────────────────────────────┘
```

## Concurrency Control

The critical challenge is preventing two workers from grabbing the same job:

```javascript
// In database.js — acquireJob()
const txn = this.db.transaction(() => {
  // SELECT next pending/failed job
  // UPDATE state to 'processing'
  // Return acquired job
});
return txn.immediate();  // Acquires exclusive write lock
```

`better-sqlite3`'s `.immediate()` transaction mode acquires an exclusive write lock before any reads. Any other worker attempting to acquire a job at the same instant is blocked until the first transaction commits.

## Exponential Backoff

```
delay = backoff_base ^ attempts  (in seconds)

Example with base=2:
  Attempt 1 fails → wait 2^1 = 2 seconds
  Attempt 2 fails → wait 2^2 = 4 seconds
  Attempt 3 fails → wait 2^3 = 8 seconds
```

The `run_at` timestamp is set to `now + delay`. Workers skip jobs whose `run_at` is in the future.

## Graceful Shutdown

```
1. CLI sends SIGTERM to worker PIDs
2. Worker catches signal → sets this.running = false
3. If currently executing a command → lets it finish
4. Updates job result in database
5. Unregisters from workers table
6. Exits cleanly
```

If a worker crashes, the self-healing mechanism detects dead PIDs during the next `acquireJob()` or `status` call, removes the worker record, and resets its orphaned job to `failed` for retry.

## File Structure

```
cli/
├── src/
│   ├── cli.js              # CLI entry point (Commander-based)
│   ├── database.js          # SQLite data access layer
│   └── worker.js            # Worker process execution loop
├── test/
│   └── test_queuectl.js     # Database unit tests (node:test)
├── verify_flows.js           # End-to-end CLI integration tests
├── run_tests.sh              # Test runner script
├── package.json              # npm config with bin entry
├── README.md                 # Project documentation
├── design.md                 # This file
└── .gitignore                # Git exclusions
```

## Trade-offs

1. **Polling vs. Event-driven**: Workers poll the database every 1s. A more sophisticated system would use OS notifications or a message broker. Polling is simpler and sufficient here.

2. **better-sqlite3 (sync) vs. sql.js (async)**: We chose synchronous SQLite because CLI tools benefit from straightforward sequential logic. Async would add complexity without benefit in this context.

3. **SQLite vs. Redis/PostgreSQL**: SQLite keeps the system portable and zero-infrastructure. The trade-off is no distributed multi-machine support, which is outside scope.
