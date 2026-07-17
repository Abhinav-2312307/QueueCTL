const { spawn } = require('child_process');
const path = require('path');
const QueueDB = require('./database');

class Worker {
  constructor(workerId, dbPath) {
    this.workerId = workerId;
    this.db = new QueueDB(dbPath);
    this.running = true;
    this.currentProcess = null;
  }

  setupSignals() {
    const handler = (signal) => {
      process.stderr.write(`[Worker ${this.workerId}] Received ${signal}. Graceful shutdown initiated...\n`);
      this.running = false;
    };
    process.on('SIGTERM', handler);
    process.on('SIGINT', handler);
  }

  async run() {
    this.setupSignals();
    const pid = process.pid;
    process.stdout.write(`[Worker ${this.workerId}] Started with PID ${pid}\n`);
    this.db.registerWorker(this.workerId, pid);

    try {
      while (this.running) {
        this.db.updateWorkerHeartbeat(this.workerId);

        const job = this.db.acquireJob(this.workerId);
        if (job) {
          this.db.updateWorkerStatus(this.workerId, 'processing', job.id);
          await this.executeJob(job);
          this.db.updateWorkerStatus(this.workerId, 'idle', null);
        } else {
          // No job available, sleep 1 second before polling again
          await this._sleep(1000);
        }
      }
    } catch (err) {
      process.stderr.write(`[Worker ${this.workerId}] Error in worker loop: ${err.message}\n`);
    } finally {
      process.stdout.write(`[Worker ${this.workerId}] Shutting down and unregistering...\n`);
      this.db.unregisterWorker(this.workerId);
      this.db.close();
    }
  }

  executeJob(job) {
    return new Promise((resolve) => {
      process.stdout.write(`[Worker ${this.workerId}] Processing job ${job.id}: '${job.command}'\n`);

      let stdout = '';
      let stderr = '';

      try {
        this.currentProcess = spawn('sh', ['-c', job.command], {
          stdio: ['ignore', 'pipe', 'pipe'],
        });

        this.currentProcess.stdout.on('data', (data) => {
          stdout += data.toString();
        });

        this.currentProcess.stderr.on('data', (data) => {
          stderr += data.toString();
        });

        // Periodically update heartbeat while the process runs
        const heartbeatInterval = setInterval(() => {
          try {
            this.db.updateWorkerHeartbeat(this.workerId);
          } catch {
            // Ignore heartbeat errors during shutdown
          }
        }, 500);

        this.currentProcess.on('close', (exitCode) => {
          clearInterval(heartbeatInterval);
          this.currentProcess = null;

          if (exitCode === 0) {
            process.stdout.write(`[Worker ${this.workerId}] Job ${job.id} completed successfully.\n`);
            this.db.completeJob(job.id);
          } else {
            const errorMsg = `Exit Code: ${exitCode}\nStdout:\n${stdout}\nStderr:\n${stderr}`;
            process.stdout.write(`[Worker ${this.workerId}] Job ${job.id} failed with exit code ${exitCode}.\n`);
            this.db.failJob(job.id, errorMsg);
          }
          resolve();
        });

        this.currentProcess.on('error', (err) => {
          clearInterval(heartbeatInterval);
          this.currentProcess = null;
          const errorMsg = `Spawn error: ${err.message}`;
          process.stderr.write(`[Worker ${this.workerId}] Job ${job.id} execution error: ${errorMsg}\n`);
          this.db.failJob(job.id, errorMsg);
          resolve();
        });
      } catch (err) {
        this.currentProcess = null;
        const errorMsg = `Exception during execution: ${err.message}`;
        process.stderr.write(`[Worker ${this.workerId}] Job ${job.id} error: ${errorMsg}\n`);
        this.db.failJob(job.id, errorMsg);
        resolve();
      }
    });
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// --- Main entry: run as standalone worker process ---
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    process.stderr.write('Usage: node worker.js <worker_id> [db_path]\n');
    process.exit(1);
  }

  const workerId = args[0];
  const dbPath = args[1] || null;

  const worker = new Worker(workerId, dbPath);
  worker.run();
}

module.exports = Worker;
