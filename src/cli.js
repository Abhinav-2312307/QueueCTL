#!/usr/bin/env node

const { Command } = require('commander');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const QueueDB = require('./database');

// --- Helper: formatted table printer ---
function printTable(headers, rows) {
  if (!rows.length) {
    console.log('No records found.');
    return;
  }

  const strRows = rows.map((r) => r.map(String));
  const colWidths = headers.map((h, i) => {
    let maxLen = h.length;
    for (const row of strRows) {
      if (row[i] && row[i].length > maxLen) maxLen = row[i].length;
    }
    return maxLen;
  });

  const headerLine = headers.map((h, i) => h.padEnd(colWidths[i])).join('  ');
  console.log(headerLine);
  console.log('-'.repeat(headerLine.length));
  for (const row of strRows) {
    console.log(row.map((v, i) => v.padEnd(colWidths[i])).join('  '));
  }
}

// --- Command Handlers ---

function handleEnqueue(jobJson) {
  let data;
  try {
    data = JSON.parse(jobJson);
  } catch (e) {
    console.error(`Error: Invalid JSON format. Details: ${e.message}`);
    process.exit(1);
  }

  if (!data.command) {
    console.error("Error: JSON payload must contain a 'command' field.");
    process.exit(1);
  }

  let maxRetries = null;
  if (data.max_retries !== undefined) {
    maxRetries = parseInt(data.max_retries, 10);
    if (isNaN(maxRetries)) {
      console.error("Error: 'max_retries' must be an integer.");
      process.exit(1);
    }
  }

  const db = new QueueDB();
  const result = db.enqueueJob(data.command, data.id || null, maxRetries);
  db.close();

  if (result.success) {
    console.log('Job enqueued successfully:');
    console.log(JSON.stringify(result.job, null, 2));
  } else {
    console.error(`Error: ${result.error}`);
    process.exit(1);
  }
}

function handleWorkerStart(options) {
  const count = parseInt(options.count, 10);
  if (isNaN(count) || count < 1) {
    console.error('Error: Worker count must be at least 1.');
    process.exit(1);
  }

  const workerScript = path.join(__dirname, 'worker.js');
  if (!fs.existsSync(workerScript)) {
    console.error(`Error: Worker script not found at ${workerScript}`);
    process.exit(1);
  }

  const db = new QueueDB();
  const logsDir = path.join(db.queuectlDir, 'logs');
  fs.mkdirSync(logsDir, { recursive: true });

  console.log(`Starting ${count} worker(s)...`);

  for (let i = 0; i < count; i++) {
    const workerId = `worker_${Math.floor(Date.now() / 1000)}_${i + 1}`;
    const logFilePath = path.join(logsDir, `${workerId}.log`);
    const logFd = fs.openSync(logFilePath, 'a');

    const child = spawn(process.execPath, [workerScript, workerId, db.dbPath], {
      detached: true,
      stdio: ['ignore', logFd, logFd],
    });

    child.unref();
    fs.closeSync(logFd);

    console.log(`  -> Spawned ${workerId} (PID: ${child.pid}) | Logs: ${logFilePath}`);
  }

  db.close();

  // Small delay to let workers register
  setTimeout(() => {}, 500);
}

function handleWorkerStop() {
  const db = new QueueDB();
  const workers = db.getActiveWorkers();

  if (!workers.length) {
    console.log('No active workers are registered in the queue database.');
    db.close();
    return;
  }

  const pids = new Map();
  for (const w of workers) {
    pids.set(w.pid, w.id);
  }

  console.log(`Found ${pids.size} active worker(s). Sending graceful termination signal (SIGTERM)...`);

  for (const [pid, workerId] of pids) {
    console.log(`  -> Stopping worker ${workerId} (PID: ${pid})...`);
    try {
      process.kill(pid, 'SIGTERM');
    } catch {
      // Already exited
    }
  }

  // Poll for graceful termination
  const graceTimeout = 15000;
  const startTime = Date.now();

  const poll = () => {
    const remaining = new Map();
    for (const [pid, workerId] of pids) {
      try {
        process.kill(pid, 0);
        remaining.set(pid, workerId);
      } catch {
        // Process exited
      }
    }

    if (remaining.size === 0) {
      console.log('All workers stopped successfully.');
      db.cleanDeadWorkers();
      db.close();
      return;
    }

    if (Date.now() - startTime > graceTimeout) {
      console.log(`Workers ${[...remaining.values()]} did not stop within ${graceTimeout / 1000}s. Forcing shutdown (SIGKILL)...`);
      for (const [pid] of remaining) {
        try {
          process.kill(pid, 'SIGKILL');
        } catch {
          // Already gone
        }
      }
      console.log('Forced termination signal sent to remaining workers.');
      db.cleanDeadWorkers();
      db.close();
      return;
    }

    setTimeout(poll, 500);
  };

  setTimeout(poll, 500);
}

function handleStatus() {
  const db = new QueueDB();
  const jobs = db.listJobs();
  const workers = db.getActiveWorkers();

  const states = { pending: 0, processing: 0, completed: 0, failed: 0, dead: 0 };
  for (const job of jobs) {
    states[job.state] = (states[job.state] || 0) + 1;
  }

  console.log('='.repeat(45));
  console.log('QueueCTL Status Summary');
  console.log('='.repeat(45));
  console.log('Job Lifecycles:');
  for (const [state, count] of Object.entries(states)) {
    const label = state === 'dead' ? 'DEAD (DLQ)' : state.toUpperCase();
    console.log(`  ${label.padEnd(15)} : ${count}`);
  }
  console.log(`  ${'TOTAL'.padEnd(15)} : ${jobs.length}`);
  console.log('-'.repeat(45));

  console.log('Active Workers:');
  if (!workers.length) {
    console.log('  No workers active.');
  }
  for (const w of workers) {
    const currentJob = w.current_job_id ? `processing job: ${w.current_job_id}` : 'idle';
    const heartbeatDiff = Math.floor(Date.now() / 1000 - w.last_heartbeat);
    console.log(`  - ${w.id} (PID: ${w.pid}) | ${currentJob} | Heartbeat: ${heartbeatDiff}s ago`);
  }
  console.log('='.repeat(45));
  db.close();
}

function handleList(options) {
  const db = new QueueDB();
  const jobs = db.listJobs(options.state);
  db.close();

  const headers = ['ID', 'Attempts', 'Max Retries', 'Created At', 'Updated At', 'Command'];
  const rows = jobs.map((j) => [j.id, j.attempts, j.max_retries, j.created_at, j.updated_at, j.command]);

  console.log(`Jobs with state '${options.state}':`);
  printTable(headers, rows);
}

function handleDlqList() {
  const db = new QueueDB();
  const jobs = db.listJobs('dead');
  db.close();

  const headers = ['ID', 'Attempts', 'Created At', 'Command', 'Last Error'];
  const rows = jobs.map((j) => {
    let errMsg = (j.error_message || '').replace(/\n/g, ' ');
    if (errMsg.length > 40) errMsg = errMsg.substring(0, 37) + '...';
    return [j.id, j.attempts, j.created_at, j.command, errMsg];
  });

  console.log('Jobs in Dead Letter Queue (DLQ):');
  printTable(headers, rows);
}

function handleDlqRetry(jobId) {
  const db = new QueueDB();
  const result = db.retryDlqJob(jobId);
  db.close();

  console.log(result.message);
  if (!result.success) process.exit(1);
}

function handleConfigSet(key, value) {
  if (key === 'max-retries') {
    const v = parseInt(value, 10);
    if (isNaN(v) || v < 0) {
      console.error("Error: 'max-retries' must be a non-negative integer.");
      process.exit(1);
    }
  } else if (key === 'backoff-base') {
    const v = parseFloat(value);
    if (isNaN(v) || v <= 0) {
      console.error("Error: 'backoff-base' must be a positive number.");
      process.exit(1);
    }
  }

  const db = new QueueDB();
  db.setConfig(key, value);
  db.close();

  console.log(`Configuration set successfully: ${key} = ${value}`);
}

// --- CLI Setup ---

const program = new Command();
program
  .name('queuectl')
  .description('QueueCTL - CLI-based background job queue system')
  .version('1.0.0');

// Enqueue
program
  .command('enqueue')
  .description('Add a new job to the queue')
  .argument('<job_json>', "JSON string e.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}'")
  .action(handleEnqueue);

// Worker
const workerCmd = program.command('worker').description('Manage worker processes');

workerCmd
  .command('start')
  .description('Start one or more workers in the background')
  .option('--count <number>', 'Number of worker processes to spawn', '1')
  .action(handleWorkerStart);

workerCmd
  .command('stop')
  .description('Stop running workers gracefully')
  .action(handleWorkerStop);

// Status
program
  .command('status')
  .description('Show summary of all job states and active workers')
  .action(handleStatus);

// List
program
  .command('list')
  .description('List jobs by state')
  .requiredOption('--state <state>', 'Job state to filter by (pending|processing|completed|failed|dead)')
  .action(handleList);

// DLQ
const dlqCmd = program.command('dlq').description('Manage Dead Letter Queue (DLQ)');

dlqCmd
  .command('list')
  .description('View all jobs in the Dead Letter Queue')
  .action(handleDlqList);

dlqCmd
  .command('retry')
  .description('Retry a job in the Dead Letter Queue')
  .argument('<job_id>', 'ID of the dead job to return to the pending queue')
  .action(handleDlqRetry);

// Config
const configCmd = program.command('config').description('Manage queue configuration');

configCmd
  .command('set')
  .description('Set configuration value')
  .argument('<key>', 'Configuration key (max-retries|backoff-base)')
  .argument('<value>', 'Value to set')
  .action(handleConfigSet);

program.parse(process.argv);
