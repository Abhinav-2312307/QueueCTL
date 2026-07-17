const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

class QueueDB {
  /**
   * @param {string} [dbPath] - Optional path to the SQLite database file.
   *   Defaults to .queuectl/queuectl.db in the current working directory.
   */
  constructor(dbPath) {
    if (!dbPath) {
      this.queuectlDir = path.resolve(process.cwd(), '.queuectl');
      fs.mkdirSync(this.queuectlDir, { recursive: true });
      this.dbPath = path.join(this.queuectlDir, 'queuectl.db');
    } else {
      this.dbPath = path.resolve(dbPath);
      fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
      this.queuectlDir = path.dirname(this.dbPath);
    }

    this.db = new Database(this.dbPath);
    this._initDB();
  }

  _initDB() {
    this.db.pragma('journal_mode = WAL');

    // 1. Create jobs table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        run_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        error_message TEXT
      )
    `);

    // 2. Create workers table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY,
        pid INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'idle',
        current_job_id TEXT,
        last_heartbeat REAL NOT NULL,
        created_at TEXT NOT NULL
      )
    `);

    // 3. Create config table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);

    // 4. Insert default configurations
    const insertDefault = this.db.prepare(
      `INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)`
    );
    insertDefault.run('max-retries', '3');
    insertDefault.run('backoff-base', '2');
  }

  // --- HELPERS ---

  _now() {
    return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  }

  _timestamp() {
    return Date.now() / 1000;
  }

  // --- CONFIGURATION METHODS ---

  getConfig(key, defaultValue = null) {
    const row = this.db.prepare('SELECT value FROM config WHERE key = ?').get(key);
    return row ? row.value : defaultValue;
  }

  setConfig(key, value) {
    this.db.prepare(`
      INSERT INTO config (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `).run(key, String(value));
  }

  // --- JOB METHODS ---

  enqueueJob(command, jobId = null, maxRetries = null) {
    if (!jobId) {
      jobId = crypto.randomUUID();
    }

    if (maxRetries === null || maxRetries === undefined) {
      maxRetries = parseInt(this.getConfig('max-retries', '3'), 10);
    }

    const now = this._now();

    // Check duplicate and insert atomically
    const existing = this.db.prepare('SELECT id FROM jobs WHERE id = ?').get(jobId);
    if (existing) {
      return { success: false, error: `Job with ID '${jobId}' already exists` };
    }

    this.db.prepare(`
      INSERT INTO jobs (id, command, state, attempts, max_retries, run_at, created_at, updated_at)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
    `).run(jobId, command, maxRetries, now, now, now);

    return {
      success: true,
      job: {
        id: jobId,
        command,
        state: 'pending',
        attempts: 0,
        max_retries: maxRetries,
        created_at: now,
        updated_at: now,
      },
    };
  }

  /**
   * Atomically acquire the next available job for a worker.
   * Uses a transaction to prevent duplicate processing by concurrent workers.
   */
  acquireJob(workerId) {
    const txn = this.db.transaction(() => {
      // Self-healing: clean dead workers and recover orphaned jobs
      this._cleanDeadWorkersInternal();

      const now = this._now();

      const row = this.db.prepare(`
        SELECT id, command, attempts, max_retries
        FROM jobs
        WHERE state = 'pending'
           OR (state = 'failed' AND (run_at IS NULL OR run_at <= ?))
        ORDER BY created_at ASC
        LIMIT 1
      `).get(now);

      if (!row) return null;

      const newAttempts = row.attempts + 1;

      this.db.prepare(`
        UPDATE jobs SET state = 'processing', attempts = ?, updated_at = ?
        WHERE id = ?
      `).run(newAttempts, now, row.id);

      return {
        id: row.id,
        command: row.command,
        attempts: newAttempts,
        max_retries: row.max_retries,
      };
    });

    // IMMEDIATE transaction ensures exclusive write lock
    return txn.immediate();
  }

  completeJob(jobId) {
    const now = this._now();
    this.db.prepare(`
      UPDATE jobs SET state = 'completed', error_message = NULL, updated_at = ?
      WHERE id = ?
    `).run(now, jobId);
  }

  failJob(jobId, errorMessage) {
    const txn = this.db.transaction(() => {
      const now = this._now();
      const row = this.db.prepare('SELECT attempts, max_retries FROM jobs WHERE id = ?').get(jobId);
      if (!row) return;

      const backoffBase = parseFloat(this.getConfig('backoff-base', '2'));

      if (row.attempts > row.max_retries) {
        // Move to Dead Letter Queue
        this.db.prepare(`
          UPDATE jobs SET state = 'dead', error_message = ?, updated_at = ?, run_at = NULL
          WHERE id = ?
        `).run(errorMessage, now, jobId);
      } else {
        // Schedule retry with exponential backoff: delay = base ^ attempts
        const delay = Math.pow(backoffBase, row.attempts);
        const runAt = new Date(Date.now() + delay * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

        this.db.prepare(`
          UPDATE jobs SET state = 'failed', error_message = ?, updated_at = ?, run_at = ?
          WHERE id = ?
        `).run(errorMessage, now, runAt, jobId);
      }
    });

    txn();
  }

  listJobs(state = null) {
    if (state) {
      return this.db.prepare('SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC').all(state);
    }
    return this.db.prepare('SELECT * FROM jobs ORDER BY created_at ASC').all();
  }

  getJob(jobId) {
    return this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId) || null;
  }

  // --- DLQ METHODS ---

  retryDlqJob(jobId) {
    const now = this._now();
    const row = this.db.prepare('SELECT state FROM jobs WHERE id = ?').get(jobId);

    if (!row) {
      return { success: false, message: `Job '${jobId}' not found` };
    }
    if (row.state !== 'dead') {
      return { success: false, message: `Job '${jobId}' is not in DLQ (state is '${row.state}')` };
    }

    this.db.prepare(`
      UPDATE jobs SET state = 'pending', attempts = 0, run_at = NULL, error_message = NULL, updated_at = ?
      WHERE id = ?
    `).run(now, jobId);

    return { success: true, message: `Job '${jobId}' successfully moved back to queue` };
  }

  // --- WORKER METHODS ---

  registerWorker(workerId, pid) {
    const now = this._now();
    this.db.prepare(`
      INSERT OR REPLACE INTO workers (id, pid, state, current_job_id, last_heartbeat, created_at)
      VALUES (?, ?, 'idle', NULL, ?, ?)
    `).run(workerId, pid, this._timestamp(), now);
  }

  updateWorkerHeartbeat(workerId) {
    this.db.prepare(`
      UPDATE workers SET last_heartbeat = ? WHERE id = ?
    `).run(this._timestamp(), workerId);
  }

  updateWorkerStatus(workerId, state, currentJobId) {
    this.db.prepare(`
      UPDATE workers SET state = ?, current_job_id = ?, last_heartbeat = ? WHERE id = ?
    `).run(state, currentJobId, this._timestamp(), workerId);
  }

  unregisterWorker(workerId) {
    this.db.prepare('DELETE FROM workers WHERE id = ?').run(workerId);
  }

  getActiveWorkers() {
    this.cleanDeadWorkers();
    return this.db.prepare('SELECT * FROM workers ORDER BY id ASC').all();
  }

  cleanDeadWorkers() {
    const txn = this.db.transaction(() => {
      this._cleanDeadWorkersInternal();
    });
    txn();
  }

  /**
   * Internal helper: finds dead worker processes (PIDs no longer running)
   * and recovers their orphaned jobs to 'failed' state for retry.
   */
  _cleanDeadWorkersInternal() {
    const workers = this.db.prepare('SELECT id, pid, current_job_id FROM workers').all();
    const now = this._now();

    for (const w of workers) {
      let alive = false;
      try {
        process.kill(w.pid, 0); // Signal 0 checks if PID is alive
        alive = true;
      } catch {
        // Process not found — worker is dead
      }

      if (!alive) {
        this.db.prepare('DELETE FROM workers WHERE id = ?').run(w.id);

        if (w.current_job_id) {
          const job = this.db.prepare('SELECT state FROM jobs WHERE id = ?').get(w.current_job_id);
          if (job && job.state === 'processing') {
            this.db.prepare(`
              UPDATE jobs SET state = 'failed', error_message = 'Worker process terminated abruptly', updated_at = ?, run_at = ?
              WHERE id = ?
            `).run(now, now, w.current_job_id);
          }
        }
      }
    }
  }

  close() {
    this.db.close();
  }
}

module.exports = QueueDB;
