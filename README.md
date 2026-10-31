# turnero

[![CI](https://github.com/eliasgdev/turnero/actions/workflows/ci.yml/badge.svg)](https://github.com/eliasgdev/turnero/actions/workflows/ci.yml)
![Postgres 14+](https://img.shields.io/badge/postgres-14%2B-336791?logo=postgresql&logoColor=white)
![Node 20+](https://img.shields.io/badge/node-20%2B-339933?logo=node.js&logoColor=white)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A job queue for Node.js that lives in the PostgreSQL you already have.** No Redis, no broker, and jobs can be enqueued inside your own transactions.

> *Turnero* is the Colombian word for the ticket dispenser that hands out turns in a bank or a clinic: everyone gets exactly one number and is served once.

```ts
const turnero = new Turnero({ pool });
await turnero.migrate();

await turnero.add('send-email', { to: 'ana@example.com' });

turnero.work('send-email', async (job) => {
  await mailer.send(job.data.to);
});
```

## Why a queue in Postgres?

The usual way to send an email after saving an order has a hole in it:

```ts
await db.query('INSERT INTO orders ...');   // 1. commit the order
await redisQueue.add('send-email', ...);    // 2. crash here and the email is never sent
```

Two systems cannot commit together. With turnero the job is a row in the same database, so it commits or rolls back **with** your data:

```ts
await client.query('BEGIN');
const order = await client.query('INSERT INTO orders ... RETURNING id');
await turnero.add('send-email', { orderId: order.rows[0].id }, { db: client });
await client.query('COMMIT'); // both rows or neither
```

It also means one less service to run, monitor and pay for. A Postgres queue comfortably handles thousands of jobs per second; if you need far more, or fan-out to many consumers, use a real broker.

## Features

- **Transactional enqueue** with `{ db: client }`.
- **Safe concurrency** with `FOR UPDATE SKIP LOCKED`: any number of workers and processes, each job claimed once, workers never block each other.
- **Retries** with exponential backoff and full jitter, then a `dead` state that keeps the last error.
- **Crash recovery**: jobs of a worker that died are retried when their lease expires.
- **Stale-result protection**: every claim gets a `lock_token`, so a slow worker whose lease expired cannot delete or overwrite the new run.
- **Priorities and delayed jobs** (`priority`, `runAt`, `delayMs`).
- **Graceful shutdown**: `stop()` waits for running jobs.
- **Versioned migrations** that are safe to run from several instances at boot.
- Written in TypeScript, `pg` is the only (peer) dependency.

## Install

```bash
npm install turnero pg
```

Requires Node.js 20+ and PostgreSQL 14+.

## Usage

```ts
import pg from 'pg';
import { Turnero } from 'turnero';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const turnero = new Turnero({ pool });
await turnero.migrate(); // creates the "turnero" schema; safe on every boot

// Enqueue
await turnero.add('resize-image', { key: 'photos/1.jpg' }, {
  priority: 10,            // higher runs first
  delayMs: 60_000,         // or runAt: new Date(...)
  maxAttempts: 3,          // default 5
});

// Process
const worker = turnero.work('resize-image', async (job) => {
  console.log(job.id, job.data, `attempt ${job.attempt}/${job.maxAttempts}`);
  // throw to retry
}, {
  concurrency: 5,          // default 1
  leaseSeconds: 300,       // time a job may run before it is considered lost
});

worker.on('failed', (job, error, willRetry) => console.warn(job.id, error, willRetry));

// Shutdown
process.on('SIGTERM', () => turnero.close());
```

A complete example is in [`examples/orders.mjs`](examples/orders.mjs).

### API

| | |
|---|---|
| `new Turnero({ pool?, connectionString?, schema? })` | Uses your pool or creates one. Tables go to `schema` (default `turnero`). |
| `migrate()` | Creates or updates the tables. |
| `add(queue, data, options?)` | Enqueues and returns the job id. Options: `db`, `priority`, `runAt`, `delayMs`, `maxAttempts`. |
| `work(queue, handler, options?)` | Starts a `Worker`. Options: `concurrency`, `pollIntervalMs`, `leaseSeconds`, `backoff`, `onError`. |
| `counts(queue?)` | `{ pending, running, dead }`. |
| `close()` | Stops all workers and closes the pool if turnero created it. |
| `worker.stop()` | Stops claiming jobs and waits for the running ones. |
| Worker events | `completed (job, result)`, `failed (job, error, willRetry)`, `stopped ()`. |

## Guarantees

Delivery is **at least once**. If a worker finishes the work and dies before recording it, the job runs again after its lease expires. Write handlers so that running twice is harmless, for example by checking whether the email for that order was already sent.

Successful jobs are deleted. Dead jobs stay in the table with `last_error` until you remove them.

## How it works

Each job is a row in `turnero.jobs`. A worker claims a batch in one statement:

```sql
WITH next AS (
  SELECT id FROM turnero.jobs
  WHERE queue = $1 AND state = 'pending' AND run_at <= now()
  ORDER BY priority DESC, run_at, id
  LIMIT $2
  FOR UPDATE SKIP LOCKED          -- rows locked by another worker are skipped, not waited on
)
UPDATE turnero.jobs j
SET state = 'running', attempts = attempts + 1,
    lock_token = gen_random_uuid(), locked_until = now() + make_interval(secs => $3)
FROM next WHERE j.id = next.id
RETURNING j.*;
```

The claim commits right away, so no transaction stays open while your handler runs. Completing or failing a job only touches the row if its `lock_token` still matches. The table uses a partial index on pending jobs, `fillfactor = 70` and aggressive autovacuum, because a queue table is rewritten constantly.

## Roadmap

- [ ] `LISTEN/NOTIFY` so idle workers wake up instantly instead of polling
- [ ] Recurring jobs (cron syntax)
- [ ] Unique jobs with a deduplication key
- [ ] Heartbeat to extend the lease of long jobs
- [ ] Retry and delete dead jobs from the API
- [ ] Web dashboard
- [ ] CLI (`turnero stats`, `turnero retry`)
- [ ] Benchmarks
- [ ] Docs in Spanish

## Development

```bash
npm install
npm run db:start   # Postgres 17 on port 55433 (podman or docker)
npm test
```

Tests run against a real Postgres, each on its own schema. Set `DATABASE_URL` to use another server.

## License

[MIT](LICENSE) © Elias Gutiérrez
