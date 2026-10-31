import type { Queryable } from './types.js';

/**
 * Schema changes in order. Never edit a published entry: append a new one instead,
 * because databases that already ran it will not run it again.
 */
const migrations: ((schema: string) => string)[] = [
  (s) => `
    CREATE TABLE ${s}.jobs (
      id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      queue         text        NOT NULL,
      data          jsonb       NOT NULL DEFAULT '{}',
      state         text        NOT NULL DEFAULT 'pending'
                                CHECK (state IN ('pending', 'running', 'dead')),
      priority      integer     NOT NULL DEFAULT 0,
      run_at        timestamptz NOT NULL DEFAULT now(),
      attempts      integer     NOT NULL DEFAULT 0,
      max_attempts  integer     NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
      lock_token    uuid,
      locked_until  timestamptz,
      last_error    text,
      created_at    timestamptz NOT NULL DEFAULT now(),
      updated_at    timestamptz NOT NULL DEFAULT now()
    ) WITH (
      -- A queue table is rewritten constantly: leave room for HOT updates
      -- and vacuum it far more often than the default.
      fillfactor = 70,
      autovacuum_vacuum_scale_factor = 0.01,
      autovacuum_vacuum_cost_delay = 0
    );

    -- Partial: in a healthy queue almost no rows are pending at any time.
    CREATE INDEX jobs_ready_idx ON ${s}.jobs (queue, priority DESC, run_at, id)
      WHERE state = 'pending';

    CREATE INDEX jobs_expired_idx ON ${s}.jobs (locked_until)
      WHERE state = 'running';
  `,
];

/** Any constant works; it only has to be the same for every process running migrate(). */
const MIGRATION_LOCK_ID = 7_481_239_017;

export async function migrate(db: Queryable, schema: string): Promise<void> {
  await db.query('BEGIN');
  try {
    // Serializes concurrent migrate() calls, e.g. several instances booting at once.
    await db.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
    await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
    await db.query(`CREATE TABLE IF NOT EXISTS ${schema}.migrations (
      version    integer PRIMARY KEY,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    const { rows } = await db.query<{ version: number | null }>(
      `SELECT max(version) AS version FROM ${schema}.migrations`,
    );
    const current = rows[0]?.version ?? 0;
    for (let v = current + 1; v <= migrations.length; v++) {
      await db.query(migrations[v - 1]!(schema));
      await db.query(`INSERT INTO ${schema}.migrations (version) VALUES ($1)`, [v]);
    }
    await db.query('COMMIT');
  } catch (error) {
    await db.query('ROLLBACK');
    throw error;
  }
}
