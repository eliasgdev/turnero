import type { QueryResult, QueryResultRow } from 'pg';

/**
 * Anything that can run a query: a `pg.Pool`, a `pg.Client` or a checked-out `PoolClient`.
 * Passing the client of an open transaction makes the operation part of that transaction.
 */
export interface Queryable {
  query<R extends QueryResultRow = any>(text: string, values?: unknown[]): Promise<QueryResult<R>>;
}

export type JobState = 'pending' | 'running' | 'dead';

export interface Job<T = unknown> {
  id: string;
  queue: string;
  data: T;
  priority: number;
  /** Attempt number of the current run, starting at 1. */
  attempt: number;
  maxAttempts: number;
  createdAt: Date;
}

export interface AddOptions {
  /** Run the job inside this connection or transaction instead of the queue's pool. */
  db?: Queryable;
  /** Higher runs first. Default 0. */
  priority?: number;
  /** Do not run before this date. */
  runAt?: Date;
  /** Delay in milliseconds, ignored when `runAt` is set. */
  delayMs?: number;
  /** Total attempts before the job is marked dead. Default 5. */
  maxAttempts?: number;
}

export type Handler<T = unknown> = (job: Job<T>) => Promise<unknown> | unknown;

export interface WorkOptions {
  /** Jobs processed at the same time by this worker. Default 1. */
  concurrency?: number;
  /** Wait between polls when the queue is empty. Default 1000 ms, with ±20 % jitter. */
  pollIntervalMs?: number;
  /** How long a claimed job belongs to this worker before others may retry it. Default 300 s. */
  leaseSeconds?: number;
  /** Retry delay for attempt n is a random value up to min(maxDelay, baseDelay · 2^(n-1)). */
  backoff?: { baseDelayMs?: number; maxDelayMs?: number };
  /** Called with errors of the worker loop itself (not job failures). Default: console.error. */
  onError?: (error: unknown) => void;
}
