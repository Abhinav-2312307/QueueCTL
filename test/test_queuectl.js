const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const QueueDB = require('../src/database');

let db;
let dbPath;
let tmpDir;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'queuectl-test-'));
  dbPath = path.join(tmpDir, 'test.db');
  db = new QueueDB(dbPath);
});

afterEach(() => {
  db.close();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('Configuration', () => {
  it('should set and get config values', () => {
    db.setConfig('max-retries', '5');
    assert.equal(db.getConfig('max-retries'), '5');

    db.setConfig('backoff-base', '3.5');
    assert.equal(db.getConfig('backoff-base'), '3.5');
  });
});

describe('Enqueue', () => {
  it('should enqueue a job successfully', () => {
    const result = db.enqueueJob('echo hello', 'job1');
    assert.equal(result.success, true);
    assert.equal(result.job.id, 'job1');
    assert.equal(result.job.command, 'echo hello');
    assert.equal(result.job.state, 'pending');
  });

  it('should reject duplicate job IDs', () => {
    db.enqueueJob('echo hello', 'job1');
    const result = db.enqueueJob('echo again', 'job1');
    assert.equal(result.success, false);
    assert.ok(result.error.includes('already exists'));
  });

  it('should auto-generate UUID if no id provided', () => {
    const result = db.enqueueJob('echo hello');
    assert.equal(result.success, true);
    assert.ok(result.job.id.length > 0);
  });
});

describe('Acquire Job', () => {
  it('should acquire a pending job atomically', () => {
    db.enqueueJob('echo hello', 'job1');
    const job = db.acquireJob('worker_1');
    assert.notEqual(job, null);
    assert.equal(job.id, 'job1');
    assert.equal(job.attempts, 1);

    // Verify state is now processing
    const dbJob = db.getJob('job1');
    assert.equal(dbJob.state, 'processing');
  });

  it('should return null when no jobs available', () => {
    const job = db.acquireJob('worker_1');
    assert.equal(job, null);
  });

  it('should not acquire a job already processing', () => {
    db.enqueueJob('echo hello', 'job1');
    db.acquireJob('worker_1');
    const job2 = db.acquireJob('worker_2');
    assert.equal(job2, null);
  });
});

describe('Complete Job', () => {
  it('should mark job as completed', () => {
    db.enqueueJob('echo hello', 'job1');
    db.acquireJob('worker_1');
    db.completeJob('job1');

    const job = db.getJob('job1');
    assert.equal(job.state, 'completed');
    assert.equal(job.error_message, null);
  });
});

describe('Fail Job & Retry', () => {
  it('should mark a failed job with backoff delay', () => {
    db.setConfig('max-retries', '2');
    db.setConfig('backoff-base', '2');
    db.enqueueJob('exit 1', 'job1');

    db.acquireJob('worker_1');
    db.failJob('job1', 'failed first time');

    const job = db.getJob('job1');
    assert.equal(job.state, 'failed');
    assert.notEqual(job.run_at, null);
    assert.equal(job.error_message, 'failed first time');
  });
});

describe('Fail Job to DLQ', () => {
  it('should move to dead state after exhausting retries', () => {
    db.setConfig('max-retries', '1');
    db.enqueueJob('exit 1', 'job1');

    // Attempt 1
    db.acquireJob('worker_1');
    db.failJob('job1', 'fail 1');

    // Fast-forward backoff
    db.db.prepare("UPDATE jobs SET run_at = '1970-01-01T00:00:00Z' WHERE id = ?").run('job1');

    // Attempt 2 (attempts=2 > max_retries=1 → dead)
    db.acquireJob('worker_1');
    db.failJob('job1', 'fail 2');

    const job = db.getJob('job1');
    assert.equal(job.state, 'dead');
    assert.equal(job.run_at, null);
    assert.equal(job.error_message, 'fail 2');
  });
});

describe('DLQ Retry', () => {
  it('should move a dead job back to pending', () => {
    db.setConfig('max-retries', '1');
    db.enqueueJob('exit 1', 'job1');

    db.acquireJob('worker_1');
    db.failJob('job1', 'fail 1');
    db.db.prepare("UPDATE jobs SET run_at = '1970-01-01T00:00:00Z' WHERE id = ?").run('job1');
    db.acquireJob('worker_1');
    db.failJob('job1', 'fail 2');

    assert.equal(db.getJob('job1').state, 'dead');

    const result = db.retryDlqJob('job1');
    assert.equal(result.success, true);

    const job = db.getJob('job1');
    assert.equal(job.state, 'pending');
    assert.equal(job.attempts, 0);
    assert.equal(job.error_message, null);
  });

  it('should reject retry for non-dead job', () => {
    db.enqueueJob('echo hello', 'job1');
    const result = db.retryDlqJob('job1');
    assert.equal(result.success, false);
  });
});

describe('Worker Management', () => {
  it('should prune dead worker PIDs', () => {
    db.registerWorker('worker_dead_1', 99999);
    db.registerWorker('worker_dead_2', 88888);
    db.registerWorker('worker_self', process.pid);

    const workers = db.getActiveWorkers();
    const ids = workers.map((w) => w.id);

    assert.ok(ids.includes('worker_self'));
    assert.ok(!ids.includes('worker_dead_1'));
    assert.ok(!ids.includes('worker_dead_2'));
  });
});
