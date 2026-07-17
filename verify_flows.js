const { execSync, execFileSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const QueueDB = require('./src/database');

const CLI = path.resolve(__dirname, 'src/cli.js');

function runCli(args) {
  try {
    const stdout = execFileSync(process.execPath, [CLI, ...args], {
      cwd: __dirname,
      encoding: 'utf-8',
      timeout: 30000,
    });
    return { code: 0, stdout, stderr: '' };
  } catch (err) {
    return {
      code: err.status || 1,
      stdout: err.stdout || '',
      stderr: err.stderr || '',
    };
  }
}

function printHeader(title) {
  console.log('\n' + '='.repeat(55));
  console.log(` TESTING: ${title}`);
  console.log('='.repeat(55));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  // Clean database directory
  const queuectlDir = path.resolve(__dirname, '.queuectl');
  if (fs.existsSync(queuectlDir)) {
    try {
      const db = new QueueDB();
      const workers = db.getActiveWorkers();
      for (const w of workers) {
        try { process.kill(w.pid, 9); } catch {}
      }
      db.close();
    } catch {}
    fs.rmSync(queuectlDir, { recursive: true, force: true });
  }

  console.log('Initializing QueueCTL verification suite...');

  // 1. Config
  printHeader('1. CLI Configuration Management');
  let res = runCli(['config', 'set', 'max-retries', '2']);
  assert(res.code === 0, `Config set max-retries failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  res = runCli(['config', 'set', 'backoff-base', '2']);
  assert(res.code === 0, `Config set backoff-base failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  let db = new QueueDB();
  assert(db.getConfig('max-retries') === '2', 'Config max-retries not updated');
  assert(db.getConfig('backoff-base') === '2', 'Config backoff-base not updated');
  console.log('Config correctly updated in SQLite database.');
  db.close();

  // 2. Basic Job Success
  printHeader('2. Basic Job Success Execution');
  const jobPayload = JSON.stringify({ id: 'job_success', command: "echo 'Hello World'" });
  res = runCli(['enqueue', jobPayload]);
  assert(res.code === 0, `Enqueue failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  db = new QueueDB();
  let job = db.getJob('job_success');
  assert(job !== null, 'Job not enqueued');
  assert(job.state === 'pending', `Expected pending, got ${job.state}`);
  console.log("Job successfully enqueued as 'pending'.");
  db.close();

  res = runCli(['worker', 'start', '--count', '1']);
  assert(res.code === 0, `Worker start failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  console.log('Waiting for job_success to complete...');
  for (let i = 0; i < 10; i++) {
    await sleep(500);
    db = new QueueDB();
    job = db.getJob('job_success');
    db.close();
    if (job.state === 'completed') break;
  }
  assert(job.state === 'completed', `Job state was ${job.state}, expected completed`);
  console.log("Job successfully executed and transitioned to 'completed'!");

  res = runCli(['worker', 'stop']);
  assert(res.code === 0, `Worker stop failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  // 3. Retries & DLQ
  printHeader('3. Retries & Dead Letter Queue (DLQ)');
  const failPayload = JSON.stringify({ id: 'job_fail', command: 'exit 1' });
  res = runCli(['enqueue', failPayload]);
  assert(res.code === 0, `Enqueue failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  res = runCli(['worker', 'start', '--count', '1']);
  assert(res.code === 0, `Worker start failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  console.log('Waiting for job_fail to run, retry, and transition to DLQ...');
  const startTime = Date.now();
  let dead = false;
  for (let i = 0; i < 25; i++) {
    await sleep(500);
    db = new QueueDB();
    job = db.getJob('job_fail');
    db.close();
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`  Time elapsed: ${elapsed}s | Attempts: ${job.attempts} | State: ${job.state}`);
    if (job.state === 'dead') {
      dead = true;
      break;
    }
  }
  assert(dead, 'Job did not transition to Dead Letter Queue (DLQ)');
  console.log("Job successfully transitioned to 'dead' (DLQ) after exhausting retries!");

  res = runCli(['dlq', 'list']);
  assert(res.code === 0, `dlq list failed: ${res.stderr}`);
  assert(res.stdout.includes('job_fail'), 'job_fail not found in DLQ list');
  console.log('DLQ list command shows dead job:');
  console.log(res.stdout.trim());

  console.log('Retrying dead job from DLQ...');
  res = runCli(['dlq', 'retry', 'job_fail']);
  assert(res.code === 0, `dlq retry failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  db = new QueueDB();
  job = db.getJob('job_fail');
  assert(job.state === 'pending', `Job state was ${job.state}, expected pending`);
  assert(job.attempts === 0, `Attempts was ${job.attempts}, expected 0`);
  console.log("Job successfully moved from DLQ back to 'pending' state.");
  db.close();

  res = runCli(['worker', 'stop']);
  assert(res.code === 0, `Worker stop failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  // 4. Concurrency
  printHeader('4. Concurrency & Parallel Execution');
  for (let i = 1; i <= 5; i++) {
    const payload = JSON.stringify({ id: `slow_job_${i}`, command: 'sleep 1.5' });
    res = runCli(['enqueue', payload]);
    assert(res.code === 0);
  }

  res = runCli(['worker', 'start', '--count', '3']);
  assert(res.code === 0, `Worker start failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  await sleep(600);

  res = runCli(['status']);
  assert(res.code === 0);
  console.log(res.stdout.trim());

  db = new QueueDB();
  const workers = db.getActiveWorkers();
  assert(workers.length === 3, `Expected 3 active workers, got ${workers.length}`);

  const jobIds = workers.filter((w) => w.current_job_id).map((w) => w.current_job_id);
  assert(jobIds.length === new Set(jobIds).size, `Duplicate job processing detected! IDs: ${jobIds}`);
  console.log(`Multiple workers are running in parallel. Processing jobs: ${JSON.stringify(jobIds)}`);
  db.close();

  console.log('Waiting for all concurrent jobs to complete...');
  for (let i = 0; i < 20; i++) {
    await sleep(500);
    db = new QueueDB();
    let allDone = true;
    for (let j = 1; j <= 5; j++) {
      const sj = db.getJob(`slow_job_${j}`);
      if (sj.state !== 'completed') { allDone = false; break; }
    }
    db.close();
    if (allDone) break;
  }

  db = new QueueDB();
  for (let j = 1; j <= 5; j++) {
    const sj = db.getJob(`slow_job_${j}`);
    assert(sj.state === 'completed', `slow_job_${j} is in state ${sj.state}`);
  }
  db.close();
  console.log('All concurrent jobs successfully completed without duplicate execution!');

  res = runCli(['worker', 'stop']);
  assert(res.code === 0, `Worker stop failed: ${res.stderr}`);
  console.log(res.stdout.trim());

  // 5. Invalid Command
  printHeader('5. Invalid Command Handling');
  const invalidPayload = JSON.stringify({ id: 'job_invalid', command: 'thiscommanddoesnotexist_abc' });
  res = runCli(['enqueue', invalidPayload]);
  assert(res.code === 0);

  res = runCli(['worker', 'start', '--count', '1']);
  assert(res.code === 0);

  await sleep(1000);
  db = new QueueDB();
  job = db.getJob('job_invalid');
  assert(job.state === 'failed', `Expected state failed, got ${job.state}`);
  assert(job.attempts === 1);
  assert(job.error_message !== null, 'Error message not captured');
  console.log('Invalid command failed gracefully and logged errors:');
  console.log(`  Captured Error: ${job.error_message.replace(/\n/g, ' | ')}`);
  db.close();

  res = runCli(['worker', 'stop']);
  assert(res.code === 0);

  // 6. Persistence
  printHeader('6. Persistent Storage Verification');
  db = new QueueDB();
  assert(fs.existsSync(db.dbPath), 'Database file does not exist!');
  const allJobs = db.listJobs();
  assert(allJobs.length > 0, 'No jobs found in database on reload');
  console.log(`Data successfully reloaded from persistent storage (${db.dbPath}).`);
  console.log(`Total archived jobs in DB: ${allJobs.length}`);
  db.close();

  console.log('\n' + '='.repeat(55));
  console.log(' SUCCESS: All QueueCTL verification tests passed! ');
  console.log('='.repeat(55));
}

function assert(condition, msg) {
  if (!condition) {
    console.error(`ASSERTION FAILED: ${msg}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Verification failed:', err);
  process.exit(1);
});
