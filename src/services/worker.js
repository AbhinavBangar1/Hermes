import pool from '../config/db.js';
import redisClient from '../config/redis.js';
import axios from 'axios';
import crypto from 'crypto';
import dotenv from 'dotenv';
import { validateSSRF, generateSignature } from '../utils/security.js';
import { getEndpointCircuitStatus, recordSuccess, recordFailure } from '../utils/circuitBreaker.js';

dotenv.config();

const STREAM_NAME = process.env.REDIS_STREAM_NAME || 'webhook_deliveries';
const GROUP_NAME = process.env.REDIS_GROUP_NAME || 'worker_group';
const DELAY_KEY = process.env.REDIS_DELAY_KEY || 'webhook_delays';
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '5', 10);

const CONSUMER_NAME = `worker_${crypto.randomBytes(4).toString('hex')}`;

/**
 * Calculates exponential backoff with full jitter based on target intervals.
 * Standard intervals: 1s, 5s, 30s, 2m, 10m
 * @param {number} attempt
 * @returns {number} delay in milliseconds
 */
function calculateBackoff(attempt) {
  const intervals = [1000, 5000, 30000, 120000, 600000];
  const base = intervals[Math.min(attempt - 1, intervals.length - 1)] || 600000;
  
  // Full Jitter: random value between 0 and base
  return Math.floor(Math.random() * base);
}

/**
 * Executes a single webhook delivery attempt.
 */
async function processDelivery(messageId, eventId, endpointId, attemptNumber) {
  console.log(`[Worker] Claimed message ${messageId} -> Event ${eventId} for endpoint ${endpointId} (Attempt #${attemptNumber})`);

  const client = await pool.connect();
  const errorHandler = (err) => console.error(`[Worker] Client error for event ${eventId}:`, err.message);
  client.on('error', errorHandler);

  let responseStatus = null;
  let responseHeaders = null;
  let responseBody = null;
  let errorMessage = null;
  const startTime = Date.now();

  try {

    const query = `
      SELECT 
        e.payload, 
        e.event_type,
        ep.url, 
        ep.is_active, 
        m.webhook_secret
      FROM events e
      JOIN webhook_endpoints ep ON ep.id = $2
      JOIN merchants m ON m.id = e.merchant_id
      WHERE e.id = $1
    `;
    const res = await client.query(query, [eventId, endpointId]);
    if (res.rows.length === 0) {
      console.warn(`[Worker] Event ${eventId} or Endpoint ${endpointId} not found. Acknowledging.`);
      await redisClient.xAck(STREAM_NAME, GROUP_NAME, messageId);
      return;
    }

    const { payload, event_type, url, is_active, webhook_secret } = res.rows[0];

    // If endpoint has been deactivated manually, drop delivery
    if (!is_active) {
      console.log(`[Worker] Endpoint ${endpointId} is inactive. Aborting delivery.`);
      await redisClient.xAck(STREAM_NAME, GROUP_NAME, messageId);
      return;
    }


    const cbStatus = await getEndpointCircuitStatus(client, endpointId);
    if (cbStatus.circuit_breaker_state === 'OPEN') {
      console.warn(`[Worker] Fast-failing Event ${eventId} because Circuit Breaker is OPEN for Endpoint ${endpointId}`);
      throw new Error('CIRCUIT_BREAKER_OPEN');
    }


    if (process.env.SSRF_PREVENTION_ENABLED === 'true') {
      try {
        await validateSSRF(url);
      } catch (ssrfError) {
        console.error(`[Worker] SSRF Validation failed: ${ssrfError.message}`);
        errorMessage = `Security Block: ${ssrfError.message}`;
        
        // Permanent failure for security violations - transition directly to failed/DLQ
        await client.query('BEGIN');
        await client.query(
          'INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_number, response_status, error_message) VALUES ($1, $2, $3, $4, $5)',
          [eventId, endpointId, attemptNumber, 403, errorMessage]
        );
        await client.query(
          'UPDATE outbox_tasks SET status = \'dlq\', attempts_count = $3 WHERE event_id = $1 AND endpoint_id = $2',
          [eventId, endpointId, attemptNumber]
        );
        await client.query('COMMIT');
        await redisClient.xAck(STREAM_NAME, GROUP_NAME, messageId);
        return;
      }
    }


    const timestamp = Math.floor(Date.now() / 1000);
    const signatureHeader = generateSignature(payload, webhook_secret, timestamp);


    console.log(`[Worker] Dispatching request to ${url}`);
    
    // Begin DB transaction to block concurrent updates while HTTP fires
    await client.query('BEGIN');

    let response;
    try {
      response = await axios.post(url, payload, {
        headers: {
          'Content-Type': 'application/json',
          'Hermes-Signature': signatureHeader,
          'Hermes-Event-Id': eventId
        },
        timeout: 5000, // 5s timeout
        validateStatus: () => true // Allow any response status code
      });
      
      responseStatus = response.status;
      responseHeaders = JSON.stringify(response.headers);
      responseBody = typeof response.data === 'object' ? JSON.stringify(response.data) : response.data.toString();
    } catch (httpError) {
      errorMessage = httpError.message;
    }

    const duration = Date.now() - startTime;


    if (responseStatus && responseStatus >= 200 && responseStatus < 300) {
      // SUCCESS
      console.log(`[Worker] Delivery SUCCESS. Status: ${responseStatus} in ${duration}ms`);
      
      // Update Circuit state if it was HALF-OPEN
      if (cbStatus.circuit_breaker_state === 'HALF-OPEN') {
        await recordSuccess(client, endpointId);
      }

      // Record successful attempt & update event status
      await client.query(
        'INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_number, response_status, response_headers, response_body, execution_duration_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)',
        [eventId, endpointId, attemptNumber, responseStatus, responseHeaders, responseBody, duration]
      );
      await client.query(
        'UPDATE outbox_tasks SET status = \'delivered\', attempts_count = $3 WHERE event_id = $1 AND endpoint_id = $2',
        [eventId, endpointId, attemptNumber]
      );
      await client.query('COMMIT');
      
      // Acknowledge Redis message
      await redisClient.xAck(STREAM_NAME, GROUP_NAME, messageId);
    } else {
      // HTTP FAILURE (e.g. 500, 404, or Network Timeout)
      const errorMsg = errorMessage || `HTTP status ${responseStatus}`;
      console.warn(`[Worker] Delivery FAILED. Reason: ${errorMsg} in ${duration}ms`);

      throw new Error(errorMsg);
    }
  } catch (error) {
    const duration = Date.now() - startTime;
    const finalErrorMsg = error.message;

    // Log the failed attempt to database
    await client.query('BEGIN');
    await client.query(
      'INSERT INTO delivery_attempts (event_id, endpoint_id, attempt_number, response_status, response_body, execution_duration_ms, error_message) VALUES ($1, $2, $3, $4, $5, $6, $7)',
      [eventId, endpointId, attemptNumber, responseStatus || null, responseBody || null, duration, finalErrorMsg]
    );

    // Record failure in circuit breaker
    if (finalErrorMsg !== 'CIRCUIT_BREAKER_OPEN') {
      await recordFailure(client, endpointId);
    }

    // Handle retry logic
    if (attemptNumber < MAX_ATTEMPTS) {
      const delayMs = calculateBackoff(attemptNumber);
      const scheduledTime = Date.now() + delayMs;
      
      // Update attempts_count for the task (remains 'sending' while in Redis ZSet)
      await client.query(
        'UPDATE outbox_tasks SET attempts_count = $3 WHERE event_id = $1 AND endpoint_id = $2',
        [eventId, endpointId, attemptNumber]
      );

      // Reschedule attempt in Redis Sorted Set (ZSet)
      const retryTask = {
        event_id: eventId,
        endpoint_id: endpointId,
        attempt_number: attemptNumber + 1
      };
      await redisClient.zAdd(DELAY_KEY, {
        score: scheduledTime,
        value: JSON.stringify(retryTask)
      });

      console.log(`[Worker] Retrying Event ${eventId}. Scheduled attempt #${attemptNumber + 1} in ${delayMs}ms.`);
    } else {
      // Exhausted all retries - move to DLQ status
      console.error(`[Worker] Event ${eventId} exhausted all ${MAX_ATTEMPTS} attempts for endpoint ${endpointId}. Moving to DLQ.`);
      await client.query(
        'UPDATE outbox_tasks SET status = \'dlq\', attempts_count = $3 WHERE event_id = $1 AND endpoint_id = $2',
        [eventId, endpointId, attemptNumber]
      );
    }

    await client.query('COMMIT');
    
    // Always acknowledge stream task, as we either queued a retry task or sent it to DLQ
    await redisClient.xAck(STREAM_NAME, GROUP_NAME, messageId);
  } finally {
    client.removeListener('error', errorHandler);
    client.release();
  }
}

/**
 * Listens for new messages from the Redis Stream consumer group in a blocking loop.
 */
async function run() {
  await setupConsumerGroup();
  console.log(`[Worker] Starting message poll loop: consumer name: ${CONSUMER_NAME}`);

  while (true) {
    try {
      // Read new messages (id: '>') from stream
      const response = await redisClient.xReadGroup(
        GROUP_NAME,
        CONSUMER_NAME,
        { key: STREAM_NAME, id: '>' },
        { COUNT: 1, BLOCK: 2000 }
      );

      if (!response || response.length === 0) continue;

      const streamData = response[0];
      const message = streamData.messages[0];
      const messageId = message.id;
      const { event_id, endpoint_id, attempt_number } = message.message;

      await processDelivery(messageId, event_id, endpoint_id, parseInt(attempt_number, 10));
    } catch (err) {
      console.error('[Worker] Fatal error in loop step:', err);
      // Brief sleep on error to prevent CPU spinning
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

async function setupConsumerGroup() {
  try {
    await redisClient.xGroupCreate(STREAM_NAME, GROUP_NAME, '$', { MKSTREAM: true });
    console.log(`[WorkerSetup] Created Redis Consumer Group: ${GROUP_NAME} on stream: ${STREAM_NAME}`);
  } catch (err) {
    if (err.message && err.message.includes('BUSYGROUP')) {
      // Consumer group already exists - safe to ignore
    } else {
      console.error('[WorkerSetup] Error setting up consumer group:', err);
    }
  }
}

run();
