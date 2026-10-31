import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type pg from 'pg';
import { Turnero } from '../src/index.js';
import { createTurnero, waitFor } from './helpers.js';

let turnero: Turnero;
let pool: pg.Pool;
let schema: string;
let cleanup: () => Promise<void>;

beforeEach(async () => {
  ({ turnero, pool, schema, cleanup } = await createTurnero());
});

afterEach(async () => {
  await cleanup();
});

const fast = { pollIntervalMs: 20, backoff: { baseDelayMs: 1, maxDelayMs: 5 } };

describe('migrate', () => {
  it('can run many times, even at the same moment', async () => {
    await Promise.all([turnero.migrate(), turnero.migrate(), turnero.migrate()]);
    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM ${schema}.migrations`);
    expect(rows[0].n).toBe(1);
  });

  it('rejects schema names that could inject SQL', () => {
    expect(() => new Turnero({ pool, schema: 'x; DROP TABLE users' })).toThrow(/Invalid schema/);
  });
});

describe('add', () => {
  it('stores the job as pending', async () => {
    const id = await turnero.add('email', { to: 'ana@example.com' });
    const { rows } = await pool.query(`SELECT * FROM ${schema}.jobs WHERE id = $1`, [id]);
    expect(rows[0]).toMatchObject({ queue: 'email', state: 'pending', data: { to: 'ana@example.com' } });
  });

  it('disappears with a rolled back transaction', async () => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await turnero.add('email', {}, { db: client });
      await client.query('ROLLBACK');
    } finally {
      client.release();
    }
    expect(await turnero.counts()).toEqual({ pending: 0, running: 0, dead: 0 });
  });

  it('is visible once the transaction commits', async () => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await turnero.add('email', {}, { db: client });
      expect((await turnero.counts()).pending).toBe(0); // not committed yet
      await client.query('COMMIT');
    } finally {
      client.release();
    }
    expect((await turnero.counts()).pending).toBe(1);
  });
});

describe('work', () => {
  it('runs a job and removes it when it succeeds', async () => {
    await turnero.add('email', { to: 'ana@example.com' });
    const seen: unknown[] = [];
    turnero.work('email', (job) => void seen.push(job.data), fast);

    await waitFor(() => seen.length === 1);
    await waitFor(async () => (await turnero.counts()).pending + (await turnero.counts()).running === 0);
    expect(seen).toEqual([{ to: 'ana@example.com' }]);
  });

  it('only takes jobs from its own queue', async () => {
    await turnero.add('sms', {});
    const seen: string[] = [];
    turnero.work('email', (job) => void seen.push(job.queue), fast);
    await new Promise((r) => setTimeout(r, 200));
    expect(seen).toEqual([]);
    expect((await turnero.counts('sms')).pending).toBe(1);
  });

  it('runs higher priority first', async () => {
    await turnero.add('q', 'low', { priority: 0 });
    await turnero.add('q', 'high', { priority: 10 });
    await turnero.add('q', 'mid', { priority: 5 });
    const order: unknown[] = [];
    turnero.work('q', (job) => void order.push(job.data), fast);
    await waitFor(() => order.length === 3);
    expect(order).toEqual(['high', 'mid', 'low']);
  });

  it('waits for runAt', async () => {
    await turnero.add('q', 'later', { delayMs: 400 });
    const seenAt: number[] = [];
    const start = Date.now();
    turnero.work('q', () => void seenAt.push(Date.now() - start), fast);
    await waitFor(() => seenAt.length === 1);
    expect(seenAt[0]).toBeGreaterThanOrEqual(350);
  });

  it('retries failures until the handler succeeds', async () => {
    await turnero.add('q', {});
    const attempts: number[] = [];
    turnero.work(
      'q',
      (job) => {
        attempts.push(job.attempt);
        if (job.attempt < 3) throw new Error('temporary');
      },
      fast,
    );
    await waitFor(() => attempts.length === 3);
    await waitFor(async () => (await turnero.counts()).pending === 0);
    expect(attempts).toEqual([1, 2, 3]);
  });

  it('marks the job dead after maxAttempts, keeping the last error', async () => {
    const id = await turnero.add('q', {}, { maxAttempts: 2 });
    const worker = turnero.work(
      'q',
      () => {
        throw new Error('SMTP down');
      },
      fast,
    );
    const retries: boolean[] = [];
    worker.on('failed', (_job, _error, willRetry) => retries.push(willRetry));

    await waitFor(async () => (await turnero.counts()).dead === 1);
    const { rows } = await pool.query(`SELECT attempts, last_error FROM ${schema}.jobs WHERE id = $1`, [id]);
    expect(rows[0].attempts).toBe(2);
    expect(rows[0].last_error).toContain('SMTP down');
    expect(retries).toEqual([true, false]);
  });

  it('never gives the same job to two workers', async () => {
    const total = 300;
    for (let i = 0; i < total; i += 100) {
      await pool.query(
        `INSERT INTO ${schema}.jobs (queue, data) SELECT 'q', to_jsonb(g) FROM generate_series($1::int, $2::int) g`,
        [i, i + 99],
      );
    }
    const runs = new Map<number, number>();
    const handler = async (job: { data: unknown }) => {
      runs.set(job.data as number, (runs.get(job.data as number) ?? 0) + 1);
      await new Promise((r) => setTimeout(r, Math.random() * 3));
    };
    for (let w = 0; w < 4; w++) turnero.work('q', handler, { ...fast, concurrency: 5 });

    await waitFor(() => runs.size === total, 20_000);
    await turnero.close();
    expect([...runs.values()].every((n) => n === 1)).toBe(true);
  });

  it('retries jobs whose worker died (expired lease)', async () => {
    const id = await turnero.add('q', 'orphan');
    // Simulate a worker that claimed the job and then crashed.
    await pool.query(
      `UPDATE ${schema}.jobs SET state = 'running', attempts = 1, lock_token = gen_random_uuid(),
       locked_until = now() - interval '1 second' WHERE id = $1`,
      [id],
    );
    const seen: number[] = [];
    turnero.work('q', (job) => void seen.push(job.attempt), fast);
    await waitFor(() => seen.length === 1);
    expect(seen).toEqual([2]);
  });

  it('ignores the late result of a worker that lost its lease', async () => {
    const id = await turnero.add('q', {});
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const worker = turnero.work('q', () => gate, fast);
    await waitFor(async () => (await turnero.counts()).running === 1);

    // Another worker takes over after the lease expired.
    await pool.query(`UPDATE ${schema}.jobs SET lock_token = gen_random_uuid() WHERE id = $1`, [id]);
    release();
    await worker.stop();

    const { rows } = await pool.query(`SELECT state FROM ${schema}.jobs WHERE id = $1`, [id]);
    expect(rows[0]?.state).toBe('running'); // still owned by the new run, not deleted
  });

  it('stop() waits for the running job to finish', async () => {
    await turnero.add('q', {});
    let finished = false;
    const worker = turnero.work(
      'q',
      async () => {
        await new Promise((r) => setTimeout(r, 200));
        finished = true;
      },
      fast,
    );
    await waitFor(async () => (await turnero.counts()).running === 1);
    await worker.stop();
    expect(finished).toBe(true);
  });
});
