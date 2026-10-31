import pg from 'pg';
import { migrate } from './migrations.js';
import { Worker } from './worker.js';
import type { AddOptions, Handler, JobState, Queryable, WorkOptions } from './types.js';

export interface TurneroOptions {
  /** Pool to use. Turnero never closes a pool it did not create. */
  pool?: pg.Pool;
  /** Used to create a pool when `pool` is not given. */
  connectionString?: string;
  /** Postgres schema for Turnero's tables. Default "turnero". */
  schema?: string;
}

const IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/;

export class Turnero {
  readonly pool: pg.Pool;
  /** Quoted schema name, safe to interpolate in SQL. */
  readonly schema: string;
  private readonly ownsPool: boolean;
  private readonly workers = new Set<Worker>();

  constructor(options: TurneroOptions = {}) {
    const schema = options.schema ?? 'turnero';
    if (!IDENTIFIER.test(schema)) {
      throw new Error(`Invalid schema name "${schema}": use lowercase letters, digits and _`);
    }
    this.schema = `"${schema}"`;
    this.ownsPool = !options.pool;
    this.pool = options.pool ?? new pg.Pool({ connectionString: options.connectionString });
  }

  /** Creates or updates Turnero's tables. Safe to call on every boot. */
  async migrate(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await migrate(client, this.schema);
    } finally {
      client.release();
    }
  }

  /**
   * Adds a job and returns its id. Pass `{ db: client }` with the client of an open
   * transaction to enqueue atomically with your own writes: if the transaction rolls
   * back, the job never existed.
   */
  async add<T>(queue: string, data: T, options: AddOptions = {}): Promise<string> {
    const db: Queryable = options.db ?? this.pool;
    const runAt = options.runAt ?? (options.delayMs ? new Date(Date.now() + options.delayMs) : null);
    const { rows } = await db.query<{ id: string }>(
      `INSERT INTO ${this.schema}.jobs (queue, data, priority, run_at, max_attempts)
       VALUES ($1, $2, $3, coalesce($4, now()), $5)
       RETURNING id`,
      [queue, JSON.stringify(data ?? null), options.priority ?? 0, runAt, options.maxAttempts ?? 5],
    );
    return rows[0]!.id;
  }

  /** Starts processing `queue` with `handler`. Call `stop()` on the result to shut down cleanly. */
  work<T>(queue: string, handler: Handler<T>, options: WorkOptions = {}): Worker {
    const worker = new Worker(this, queue, handler as Handler, options);
    this.workers.add(worker);
    worker.once('stopped', () => this.workers.delete(worker));
    worker.start();
    return worker;
  }

  /** Number of jobs per state, optionally for one queue. */
  async counts(queue?: string): Promise<Record<JobState, number>> {
    const { rows } = await this.pool.query<{ state: JobState; n: string }>(
      `SELECT state, count(*) AS n FROM ${this.schema}.jobs
       WHERE $1::text IS NULL OR queue = $1
       GROUP BY state`,
      [queue ?? null],
    );
    const counts: Record<JobState, number> = { pending: 0, running: 0, dead: 0 };
    for (const row of rows) counts[row.state] = Number(row.n);
    return counts;
  }

  /** Stops every worker, waiting for running jobs, and closes the pool if Turnero created it. */
  async close(): Promise<void> {
    await Promise.all([...this.workers].map((w) => w.stop()));
    if (this.ownsPool) await this.pool.end();
  }
}
