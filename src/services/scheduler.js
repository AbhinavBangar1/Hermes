import pool from '../config/db.js';
import redisClient from '../config/redis.js';
import dotenv from 'dotenv';

dotenv.config();

const STREAM_NAME = process.env.REDIS_STREAM_NAME || 'webhook_deliveries';
const DELAY_KEY = process.env.REDIS_DELAY_KEY || 'webhook_delays';

/**
 * Polls the outbox_tasks table in PostgreSQL using SELECT FOR UPDATE SKIP LOCKED
 * and pushes pending events to the Redis Stream.
 */
async function pollOutbox() {
  const client = await pool.connect();
  const errorHandler = (err) => console.error('[OutboxPublisher] Client error:', err.message);
  client.on('error', errorHandler);
  try {
    await client.query('BEGIN');

    // Fetch up to 50 tasks concurrently without blocking other scheduler instances
    const fetchQuery = `
      SELECT id, event_id, endpoint_id, attempts_count
      FROM outbox_tasks
      WHERE status = 'pending'
      ORDER BY created_at ASC
      LIMIT 50
      FOR UPDATE SKIP LOCKED
    `;
    const res = await client.query(fetchQuery);

    if (res.rows.length === 0) {
      await client.query('COMMIT');
      return;
    }

    for (const task of res.rows) {
      // Pushing payload metadata onto Redis Stream
      await redisClient.xAdd(STREAM_NAME, '*', {
        event_id: task.event_id,
        endpoint_id: task.endpoint_id,
        attempt_number: task.attempts_count ? task.attempts_count.toString() : '0'
      });

      // Update task to 'sending' so it's not polled again
      await client.query('UPDATE outbox_tasks SET status = \'sending\' WHERE id = $1', [task.id]);
    }

    await client.query('COMMIT');
    console.log(`[OutboxPublisher] Successfully pushed ${res.rows.length} tasks to Redis Stream.`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[OutboxPublisher] Error during outbox processing:', error);
  } finally {
    client.removeListener('error', errorHandler);
    client.release();
  }
}

/**
 * Sweeps the Redis Sorted Set (ZSet) delay queue for expired webhook retries
 * and pushes them back into the active Redis Stream.
 */
async function sweepDelayQueue() {
  const now = Date.now();
  try {
    // Fetch expired items where score (scheduled time) <= now
    const expiredJobs = await redisClient.zRange(DELAY_KEY, 0, now, {
      BY: 'SCORE',
      LIMIT: { offset: 0, count: 50 }
    });

    if (expiredJobs.length === 0) return;

    for (const jobString of expiredJobs) {
      const task = JSON.parse(jobString);

      // Atomically remove from ZSet to ensure single worker processing
      const removedCount = await redisClient.zRem(DELAY_KEY, jobString);
      if (removedCount > 0) {
        // Enqueue to the active Stream for immediate execution
        await redisClient.xAdd(STREAM_NAME, '*', {
          event_id: task.event_id,
          endpoint_id: task.endpoint_id,
          attempt_number: task.attempt_number.toString()
        });
        console.log(`[DelayScheduler] Re-queued retry: Event ${task.event_id} -> Endpoint ${task.endpoint_id} (Attempt #${task.attempt_number})`);
      }
    }
  } catch (error) {
    console.error('[DelayScheduler] Error sweeping delay queue:', error);
  }
}

// Daemon Loops
const startOutboxPublisher = () => {
  setInterval(async () => {
    await pollOutbox();
  }, 100); // Poll every 100ms
  console.log('[OutboxPublisher] Outbox publisher daemon started.');
};

const startDelayScheduler = () => {
  setInterval(async () => {
    await sweepDelayQueue();
  }, 500); // Sweep every 500ms
  console.log('[DelayScheduler] Delay queue sweeper daemon started.');
};

// Start both
startOutboxPublisher();
startDelayScheduler();
