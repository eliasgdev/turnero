import pg from 'pg';
import { randomBytes } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { Turnero } from '../src/index.js';

export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://turnero:turnero@localhost:55433/turnero';

/** A Turnero on its own schema, so tests can run in parallel without seeing each other. */
export async function createTurnero(): Promise<{ turnero: Turnero; pool: pg.Pool; schema: string; cleanup: () => Promise<void> }> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });
  const schema = `test_${randomBytes(4).toString('hex')}`;
  const turnero = new Turnero({ pool, schema });
  await turnero.migrate();
  return {
    turnero,
    pool,
    schema,
    cleanup: async () => {
      await turnero.close();
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
      await pool.end();
    },
  };
}

export async function waitFor(condition: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error('waitFor: timed out');
    await sleep(20);
  }
}
