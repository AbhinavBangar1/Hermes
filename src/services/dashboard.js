import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import pool from '../config/db.js';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json());

const PORT = process.env.PORT_DASHBOARD || 5000;

// Serve static dashboard files
app.use(express.static(path.join(__dirname, '../public')));

/**
 * API: Fetch system observability metrics.
 * Calculates P50/P99 latency, success rate, and active circuit breakers.
 */
app.get('/api/metrics', async (req, res) => {
  try {
    // 1. Success Rate
    const rateRes = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE status = 'delivered') * 100.0 / NULLIF(COUNT(*), 0) as success_rate,
        COUNT(*) as total_events
      FROM outbox_tasks
    `);
    const successRate = parseFloat(rateRes.rows[0].success_rate || 0).toFixed(1);
    const totalEvents = parseInt(rateRes.rows[0].total_events || 0, 10);

    // 2. Status Counts
    const statusRes = await pool.query(`
      SELECT status, COUNT(*) as count 
      FROM outbox_tasks 
      GROUP BY status
    `);
    const statusCounts = { pending: 0, sending: 0, delivered: 0, failed: 0, dlq: 0 };
    statusRes.rows.forEach(row => {
      statusCounts[row.status] = parseInt(row.count, 10);
    });

    // 3. P50 / P99 Latencies
    const latencyRes = await pool.query(`
      SELECT 
        percentile_cont(0.5) WITHIN GROUP (ORDER BY execution_duration_ms) as p50,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY execution_duration_ms) as p99
      FROM delivery_attempts
      WHERE execution_duration_ms IS NOT NULL
    `);
    const p50 = parseFloat(latencyRes.rows[0].p50 || 0).toFixed(0);
    const p99 = parseFloat(latencyRes.rows[0].p99 || 0).toFixed(0);

    // 4. Retry Counts Distribution
    const retryRes = await pool.query(`
      SELECT attempts_count, COUNT(*) as count 
      FROM outbox_tasks 
      GROUP BY attempts_count 
      ORDER BY attempts_count ASC
    `);
    const retryDistribution = retryRes.rows.map(row => ({
      attempts: parseInt(row.attempts_count, 10),
      count: parseInt(row.count, 10)
    }));

    // 5. Active Circuit Breakers
    const cbRes = await pool.query(`
      SELECT id, url, consecutive_failures, cooldown_until 
      FROM webhook_endpoints 
      WHERE circuit_breaker_state = 'OPEN'
    `);
    const activeCircuitBreakers = cbRes.rows;

    // 6. DLQ Events
    const dlqRes = await pool.query(`
      SELECT 
        e.id, 
        e.event_type, 
        e.payload, 
        ot.attempts_count, 
        ot.created_at,
        da.response_status, 
        da.error_message, 
        ep.url
      FROM outbox_tasks ot
      JOIN events e ON e.id = ot.event_id
      JOIN webhook_endpoints ep ON ep.id = ot.endpoint_id
      LEFT JOIN LATERAL (
        SELECT response_status, error_message
        FROM delivery_attempts
        WHERE event_id = ot.event_id AND endpoint_id = ot.endpoint_id
        ORDER BY attempt_number DESC
        LIMIT 1
      ) da ON true
      WHERE ot.status = 'dlq'
      ORDER BY ot.created_at DESC
    `);
    const dlqEvents = dlqRes.rows;

    res.json({
      successRate,
      totalEvents,
      statusCounts,
      latencies: { p50, p99 },
      retryDistribution,
      activeCircuitBreakers,
      dlqEvents
    });
  } catch (error) {
    console.error('[DashboardAPI] Error fetching metrics:', error);
    res.status(500).json({ error: 'Database error' });
  }
});

/**
 * API: Redrive / re-enqueue a DLQ event.
 * Atomically updates status to pending and queues it back in outbox_tasks.
 */
app.post('/api/redrive/:eventId', async (req, res) => {
  const { eventId } = req.params;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Verify event is in DLQ
    const eventRes = await client.query(
      'SELECT e.id, e.merchant_id FROM events e JOIN outbox_tasks ot ON e.id = ot.event_id WHERE e.id = $1 AND ot.status = \'dlq\' LIMIT 1',
      [eventId]
    );
    if (eventRes.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'DLQ event not found' });
    }

    const event = eventRes.rows[0];

    // Find which endpoint failed for this event
    const failedEndpointsRes = await client.query(`
      SELECT endpoint_id 
      FROM outbox_tasks 
      WHERE event_id = $1 AND status = 'dlq'
    `, [eventId]);

    let endpoints = failedEndpointsRes.rows;
    if (endpoints.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'No DLQ tasks found for this event' });
    }

    // Reset task status to pending & attempts_count to 0
    await client.query(
      'UPDATE outbox_tasks SET status = \'pending\', attempts_count = 0 WHERE event_id = $1 AND status = \'dlq\'',
      [eventId]
    );

    await client.query('COMMIT');
    console.log(`[DashboardAPI] Successfully triggered DLQ redrive for Event: ${eventId}`);
    res.json({ message: 'Event successfully rescheduled for delivery' });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[DashboardAPI] Error redriving event:', error);
    res.status(500).json({ error: 'Failed to redrive event' });
  } finally {
    client.release();
  }
});

// Fallback index.html router
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, () => {
  console.log(`[Dashboard] Observability panel started on port ${PORT}`);
});
