# QueueCTL — CLI-Based Background Job Queue System

QueueCTL is a production-grade, lightweight, CLI-driven background job queue system written in **Node.js** with persistent storage powered by **SQLite**. It manages background jobs executing shell commands, supports parallel worker processes with strict concurrency control, schedules retries using exponential backoff, isolates failed jobs inside a Dead Letter Queue (DLQ), and performs graceful shutdowns.

---

## 🚀 Setup Instructions

### Prerequisites
- **Node.js** v18+ (tested on v22)
- **npm** (bundled with Node.js)

### Installation

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/queuectl.git
cd queuectl

# Install dependencies
npm install

# Link the CLI globally (optional)
npm link
```

After `npm link`, you can use `queuectl` from anywhere. Without linking, use `node src/cli.js` or `./src/cli.js` from the project root.

---

## 🛠️ Usage Examples

### 1. Configuration
Set retry counts or backoff bases. This updates values globally in the SQLite state.
```bash
# Set maximum retry attempts (default: 3)
queuectl config set max-retries 3

# Set exponential backoff base multiplier (default: 2.0)
queuectl config set backoff-base 2
```

### 2. Enqueuing Jobs
Enqueue jobs by passing a JSON payload containing the command to execute. You can optionally define a custom `id` and `max_retries`.
```bash
queuectl enqueue '{"id":"job1", "command":"sleep 2"}'
```
*Output:*
```json
Job enqueued successfully:
{
  "id": "job1",
  "command": "sleep 2",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2026-07-17T11:00:00Z",
  "updated_at": "2026-07-17T11:00:00Z"
}
```

### 3. Running Workers
Start multiple concurrent workers to execute pending commands.
```bash
queuectl worker start --count 3
```
*Output:*
```
Starting 3 worker(s)...
  -> Spawned worker_1784266158_1 (PID: 74678) | Logs: .queuectl/logs/worker_1784266158_1.log
  -> Spawned worker_1784266158_2 (PID: 74679) | Logs: .queuectl/logs/worker_1784266158_2.log
  -> Spawned worker_1784266158_3 (PID: 74680) | Logs: .queuectl/logs/worker_1784266158_3.log
```

### 4. Queue & Worker Status
View active workers, last heartbeats, and a summary of job lifecycle states.
```bash
queuectl status
```
*Output:*
```
=============================================
QueueCTL Status Summary
=============================================
Job Lifecycles:
  PENDING         : 0
  PROCESSING      : 0
  COMPLETED       : 3
  FAILED          : 1
  DEAD (DLQ)      : 1
  TOTAL           : 5
---------------------------------------------
Active Workers:
  - worker_1784266158_1 (PID: 74678) | idle | Heartbeat: 2s ago
  - worker_1784266158_2 (PID: 74679) | idle | Heartbeat: 1s ago
  - worker_1784266158_3 (PID: 74680) | idle | Heartbeat: 3s ago
=============================================
```

### 5. Listing Jobs
List jobs filtered by their current lifecycle state.
```bash
queuectl list --state completed
```

### 6. Managing Dead Letter Queue (DLQ)
View dead jobs or queue them back up.
```bash
# View dead jobs
queuectl dlq list

# Requeue a dead job back to pending
queuectl dlq retry job_fail
```

### 7. Graceful Shutdown
Stop all running workers gracefully (finish current job before exit).
```bash
queuectl worker stop
```

---

## 🧩 Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     User Terminal                        │
│                                                         │
│  queuectl enqueue '{"id":"j1","command":"echo hi"}'     │
│  queuectl worker start --count 3                        │
│  queuectl status                                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                   src/cli.js                             │
│            (Commander-based CLI Router)                  │
│                                                         │
│  • Command parsing and validation                       │
│  • Spawns detached worker processes                     │
│  • Sends SIGTERM for graceful shutdown                  │
│  • Formats status/list/dlq output tables                │
└────────────────────────┬────────────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
┌──────────────────────┐  ┌──────────────────────┐
│   src/worker.js      │  │   src/worker.js      │
│  (Worker Process)    │  │  (Worker Process)    │
│                      │  │                      │
│ • Polls for jobs     │  │ • Polls for jobs     │
│ • child_process.spawn│  │ • child_process.spawn│
│ • Signal handling    │  │ • Signal handling    │
│ • Heartbeat updates  │  │ • Heartbeat updates  │
└──────────┬───────────┘  └──────────┬───────────┘
           └────────────┬────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  src/database.js                         │
│               (Data Access Layer)                        │
│                                                         │
│  • better-sqlite3 with WAL mode                        │
│  • Atomic job acquisition (immediate transactions)     │
│  • Retry scheduling with exponential backoff            │
│  • Dead worker detection & orphan job recovery          │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│              .queuectl/queuectl.db                       │
│                 (SQLite - WAL mode)                      │
│                                                         │
│  Tables: jobs | workers | config                        │
└─────────────────────────────────────────────────────────┘
```

### Job Lifecycle
```
pending → processing → completed
                    ↘ failed (retry with backoff) → dead (DLQ)
```

### Concurrency Control
Workers use SQLite's **immediate transactions** to atomically acquire jobs. Only one worker can read-and-update a job at the same time — preventing duplicate processing.

### Exponential Backoff
`delay = backoff_base ^ attempts` seconds. The `run_at` field prevents workers from picking up a failed job before its backoff delay expires.

### Graceful Shutdown
Workers catch `SIGTERM`/`SIGINT`, finish the current job, update the database, and exit cleanly.

---

## 🛠️ Assumptions & Trade-offs

1. **better-sqlite3**: Chosen for synchronous API (cleaner in CLI tools) and battle-tested SQLite bindings. The only native dependency.
2. **commander**: Standard Node.js CLI framework for clean help text generation and subcommand routing.
3. **Process-per-worker**: Workers run as detached child processes (`child_process.spawn` with `detached: true`), providing true OS-level parallelism and isolation.
4. **Local state**: All data lives in `.queuectl/` in the current working directory — self-contained and portable.
5. **Polling interval**: Workers poll every 1 second. Sufficient for this use case; a production system might use OS notifications.

---

## 🧪 Testing Instructions

### Run All Tests
```bash
chmod +x run_tests.sh
./run_tests.sh
```

### Unit Tests Only
Tests core database logic (transactions, configs, DLQ, worker pruning) using Node's built-in test runner:
```bash
npm test
```

### Integration Tests Only
E2E tests exercising the actual CLI commands:
```bash
npm run test:e2e
```

### What the tests verify:
1. ✅ Configuration management
2. ✅ Basic job completes successfully
3. ✅ Failed job retries with backoff and moves to DLQ
4. ✅ Multiple workers process jobs without overlap
5. ✅ Invalid commands fail gracefully
6. ✅ Job data survives restart
