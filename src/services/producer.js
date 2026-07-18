import express from 'express';
import crypto from 'crypto';
import pool from '../config/db.js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT_PRODUCER || 3000;

// Register Merchant
app.post('/merchants', async (req, res) => {
  const { name } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'name is required' });
  }

  try {
    const webhookSecret = 'whsec_' + crypto.randomBytes(24).toString('hex');
    const result = await pool.query(
      'INSERT INTO merchants (name, webhook_secret) VALUES ($1, $2) RETURNING *',
      [name, webhookSecret]
    );
    console.log(`[Producer] Registered merchant: ${name} (ID: ${result.rows[0].id})`);
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('[Producer] Error registering merchant:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Register Webhook Endpoint
app.post('/endpoints', async (req, res) => {
  const { merchant_id, url } = req.body;
  if (!merchant_id || !url) {
    return res.status(400).json({ error: 'merchant_id and url are required' });
  }

  try {
    // Verify merchant exists
    const merchantCheck = await pool.query('SELECT id FROM merchants WHERE id = $1', [merchant_id]);
    if (merchantCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Merchant not found' });
    }

    const result = await pool.query(
      'INSERT INTO webhook_endpoints (merchant_id, url) VALUES ($1, $2) RETURNING *',
      [merchant_id, url]
    );
    console.log(`[Producer] Registered webhook endpoint: ${url} for merchant ${merchant_id}`);
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('[Producer] Error registering endpoint:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Produce Webhook Event (Transactional Outbox Pattern)
app.post('/events', async (req, res) => {
  const { merchant_id, event_type, payload } = req.body;
  if (!merchant_id || !event_type || !payload) {
    return res.status(400).json({ error: 'merchant_id, event_type, and payload are required' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Insert Event into the events table
    const eventQuery = `
      INSERT INTO events (merchant_id, event_type, payload)
      VALUES ($1, $2, $3)
      RETURNING *
    `;
    const eventRes = await client.query(eventQuery, [merchant_id, event_type, JSON.stringify(payload)]);
    const event = eventRes.rows[0];

    // 2. Fetch all active endpoints for this merchant
    const endpointsRes = await client.query(
      'SELECT id FROM webhook_endpoints WHERE merchant_id = $1 AND is_active = true',
      [merchant_id]
    );
    const endpoints = endpointsRes.rows;

    // 3. Create Outbox Tasks for each active endpoint
    const outboxTasks = [];
    for (const endpoint of endpoints) {
      const outboxQuery = `
        INSERT INTO outbox_tasks (event_id, endpoint_id)
        VALUES ($1, $2)
        RETURNING *
      `;
      const outboxRes = await client.query(outboxQuery, [event.id, endpoint.id]);
      outboxTasks.push(outboxRes.rows[0]);
    }

    await client.query('COMMIT');

    console.log(`[Producer] Event triggered: ${event_type} (ID: ${event.id}). Created ${outboxTasks.length} outbox tasks.`);
    res.status(201).json({
      message: 'Event generated successfully',
      event_id: event.id,
      outbox_tasks_count: outboxTasks.length
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[Producer] Transaction rolled back. Failed to trigger event:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    client.release();
  }
});

app.listen(PORT, () => {
  console.log(`[Producer] Service started on port ${PORT}`);
});
