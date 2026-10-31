// Saves an order and its confirmation email in the same transaction, then processes the email.
// Run: npm run build && node examples/orders.mjs
import pg from 'pg';
import { Turnero } from 'turnero';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://turnero:turnero@localhost:55433/turnero',
});
const turnero = new Turnero({ pool });
await turnero.migrate();
await pool.query('CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, customer text NOT NULL)');

async function placeOrder(customer) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('INSERT INTO orders (customer) VALUES ($1) RETURNING id', [customer]);
    // Same transaction: no order without its email, and no email for an order that rolled back.
    await turnero.add('send-email', { orderId: rows[0].id, to: customer }, { db: client });
    await client.query('COMMIT');
    return rows[0].id;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

const worker = turnero.work('send-email', async (job) => {
  console.log(`attempt ${job.attempt}: emailing ${job.data.to} about order #${job.data.orderId}`);
});

await placeOrder('ana@example.com');
await placeOrder('luis@example.com');

await new Promise((resolve) => setTimeout(resolve, 1500));
await worker.stop();
console.log('pending jobs:', await turnero.counts('send-email'));
await pool.query('DROP TABLE orders');
await pool.end();
