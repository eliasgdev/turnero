import { EventEmitter } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { retryDelayMs } from './backoff.js';
import type { Turnero } from './turnero.js';
import type { Handler, Job, WorkOptions } from './types.js';

interface JobRow {
  id: string;
  queue: string;
  data: unknown;
  priority: number;
  attempts: number;
  max_attempts: number;
  lock_token: string;
  created_at: Date;
}

/** How often a worker returns expired leases of crashed workers to the queue. */
const REAPER_INTERVAL_MS = 30_000;

/**
 * Processes one queue. Events: `completed` (job, result), `failed` (job, error, willRetry),
 * `stopped` ().
 *
 * Delivery is at-least-once: a job whose worker dies mid-run is retried once its lease
 * expires, so handlers must tolerate running twice.
 */
export class Worker extends EventEmitter {
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly leaseSeconds: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly onError: (error: unknown) => void;

  private running = false;
  private loop?: Promise<void>;
  private readonly active = new Set<Promise<void>>();
  private wake = new AbortController();
  private lastReap = 0;

  constructor(
    private readonly turnero: Turnero,
    readonly queue: string,
    private readonly handler: Handler,
    options: WorkOptions,
  ) {
    super();
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
    this.leaseSeconds = options.leaseSeconds ?? 300;
    this.baseDelayMs = options.backoff?.baseDelayMs ?? 1000;
    this.maxDelayMs = options.backoff?.maxDelayMs ?? 10 * 60_000;
    this.onError = options.onError ?? ((error) => console.error('[turnero]', error));
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  /** Stops claiming new jobs and resolves when the running ones finish. */
  async stop(): Promise<void> {
    if (!this.running) return this.loop;
    this.running = false;
    this.wake.abort();
    await this.loop;
    await Promise.allSettled(this.active);
    this.emit('stopped');
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.reapExpired();
        const free = this.concurrency - this.active.size;
        const jobs = free > 0 ? await this.claim(free) : [];
        for (const row of jobs) this.track(this.process(row));

        // Keep going while there is work and capacity; otherwise wait for a slot or the next poll.
        if (jobs.length > 0 && jobs.length === free) continue;
        await this.idle();
      } catch (error) {
        this.onError(error);
        await this.idle();
      }
    }
  }

  private async claim(limit: number): Promise<JobRow[]> {
    const { rows } = await this.turnero.pool.query<JobRow>(
      `WITH next AS (
         SELECT id FROM ${this.turnero.schema}.jobs
         WHERE queue = $1 AND state = 'pending' AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       )
       UPDATE ${this.turnero.schema}.jobs j
       SET state = 'running',
           attempts = j.attempts + 1,
           lock_token = gen_random_uuid(),
           locked_until = now() + make_interval(secs => $3),
           updated_at = now()
       FROM next
       WHERE j.id = next.id
       RETURNING j.id, j.queue, j.data, j.priority, j.attempts, j.max_attempts, j.lock_token, j.created_at`,
      [this.queue, limit, this.leaseSeconds],
    );
    return rows;
  }

  private async process(row: JobRow): Promise<void> {
    const job: Job = {
      id: row.id,
      queue: row.queue,
      data: row.data,
      priority: row.priority,
      attempt: row.attempts,
      maxAttempts: row.max_attempts,
      createdAt: row.created_at,
    };
    try {
      const result = await this.handler(job);
      await this.complete(row);
      this.emit('completed', job, result);
    } catch (error) {
      try {
        const willRetry = await this.fail(row, error);
        this.emit('failed', job, error, willRetry);
      } catch (dbError) {
        // The lease will expire and the reaper will retry the job.
        this.onError(dbError);
      }
    }
  }

  // Both updates check lock_token: if this worker's lease expired and another worker
  // claimed the job, the stale result must not touch the new run.

  private async complete(row: JobRow): Promise<void> {
    await this.turnero.pool.query(
      `DELETE FROM ${this.turnero.schema}.jobs WHERE id = $1 AND lock_token = $2`,
      [row.id, row.lock_token],
    );
  }

  private async fail(row: JobRow, error: unknown): Promise<boolean> {
    const willRetry = row.attempts < row.max_attempts;
    const delayMs = willRetry ? retryDelayMs(row.attempts, this.baseDelayMs, this.maxDelayMs) : 0;
    await this.turnero.pool.query(
      `UPDATE ${this.turnero.schema}.jobs
       SET state = $3,
           run_at = now() + make_interval(secs => $4),
           last_error = $5,
           lock_token = NULL,
           locked_until = NULL,
           updated_at = now()
       WHERE id = $1 AND lock_token = $2`,
      [row.id, row.lock_token, willRetry ? 'pending' : 'dead', delayMs / 1000, describe(error)],
    );
    return willRetry;
  }

  /** Returns jobs whose worker vanished (crash, kill -9, lost network) to the queue. */
  private async reapExpired(): Promise<void> {
    if (Date.now() - this.lastReap < REAPER_INTERVAL_MS) return;
    this.lastReap = Date.now();
    await this.turnero.pool.query(
      `UPDATE ${this.turnero.schema}.jobs
       SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
           last_error = 'lease expired: the worker stopped responding',
           lock_token = NULL,
           locked_until = NULL,
           updated_at = now()
       WHERE queue = $1 AND state = 'running' AND locked_until < now()`,
      [this.queue],
    );
  }

  private track(task: Promise<void>): void {
    this.active.add(task);
    task.finally(() => {
      this.active.delete(task);
      this.wake.abort(); // a slot is free: claim again without waiting for the poll
    });
  }

  private async idle(): Promise<void> {
    if (!this.running) return;
    const jitter = 0.8 + Math.random() * 0.4;
    try {
      await sleep(this.pollIntervalMs * jitter, undefined, { signal: this.wake.signal });
    } catch {
      // woken up early
    }
    if (this.wake.signal.aborted && this.running) this.wake = new AbortController();
  }
}

function describe(error: unknown): string {
  if (error instanceof Error) return error.stack ?? `${error.name}: ${error.message}`;
  return String(error);
}
