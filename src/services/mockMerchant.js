import express from 'express';
import pool from '../config/db.js';
import { verifySignature } from '../utils/security.js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT_MOCK || 4000;

// In-memory store to manage chaos test configs
// Structure: { [endpoint_id]: { fail_count: X, current_fails: Y } }
const chaosConfigs = new Map();

/**
 * Middleware to verify Hermes-Signature headers on incoming deliveries.
 * Fetches the merchant's secret from PostgreSQL.
 */
async function signatureVerificationMiddleware(req, res, next) {
  const eventId = req.headers['hermes-event-id'];
  const signatureHeader = req.headers['hermes-signature'];

  if (!eventId || !signatureHeader) {
    console.warn('[MockMerchant] Access denied: Missing signature headers.');
    return res.status(401).json({ error: 'Missing Hermes-Event-Id or Hermes-Signature headers' });
  }

  try {
    // Fetch merchant's signing secret by event ID
    const query = `
      SELECT m.webhook_secret 
      FROM events e
      JOIN merchants m ON m.id = e.merchant_id
      WHERE e.id = $1
    `;
    const dbRes = await pool.query(query, [eventId]);
    if (dbRes.rows.length === 0) {
      console.warn(`[MockMerchant] Signature verification failed: Event ${eventId} not found in DB`);
      return res.status(401).json({ error: 'Invalid event identity' });
    }

    const secret = dbRes.rows[0].webhook_secret;
    const isValid = verifySignature(req.body, signatureHeader, secret);

    if (!isValid) {
      console.warn('[MockMerchant] Access denied: Invalid or stale HMAC signature.');
      return res.status(401).json({ error: 'Invalid signature verification' });
    }

    // Signature is valid, proceed
    next();
  } catch (error) {
    console.error('[MockMerchant] Error during signature verification:', error);
    res.status(500).json({ error: 'Internal verification error' });
  }
}

// Apply signature verification to all webhooks
app.use('/webhooks', signatureVerificationMiddleware);

// Endpoint 1: Always Success (200 OK)
app.post('/webhooks/success', (req, res) => {
  console.log('[MockMerchant] Received webhook at /success. Signature verified successfully.');
  res.status(200).json({ received: true, status: 'success' });
});

// Endpoint 2: Custom status code failure (e.g. /webhooks/fail/503)
app.post('/webhooks/fail/:code', (req, res) => {
  const code = parseInt(req.params.code, 10) || 500;
  console.log(`[MockMerchant] Received webhook at /fail/${code}. Responding with HTTP status ${code}`);
  res.status(code).json({ received: true, status: `failed_with_${code}` });
});

// Endpoint 3: Artificial Latency / Timeout Simulation
app.post('/webhooks/timeout', async (req, res) => {
  const delayMs = parseInt(req.query.ms || '6000', 10);
  console.log(`[MockMerchant] Received webhook at /timeout. Simulating delay of ${delayMs}ms...`);
  
  await new Promise(r => setTimeout(r, delayMs));
  
  console.log('[MockMerchant] Delay complete. Responding with 200 OK');
  res.status(200).json({ received: true, status: 'delayed_success' });
});

// Endpoint 4: Chaos fail-then-succeed behavior
app.post('/webhooks/chaos/:endpointId', (req, res) => {
  const endpointId = req.params.endpointId;
  const config = chaosConfigs.get(endpointId);

  if (!config) {
    console.log(`[MockMerchant] Received webhook at /chaos/${endpointId}. No chaos configuration found, defaulting to 200 SUCCESS.`);
    return res.status(200).json({ received: true, status: 'no_chaos_config' });
  }

  if (config.current_fails < config.fail_count) {
    config.current_fails++;
    console.log(`[MockMerchant] Chaos mode: failing delivery. Failure ${config.current_fails}/${config.fail_count}. Responding 503 Service Unavailable.`);
    return res.status(503).json({ error: 'Chaos Failure', attempt: config.current_fails });
  }

  console.log(`[MockMerchant] Chaos mode: Target failures (${config.fail_count}) reached. Delivery SUCCESS. Responding 200 OK.`);
  res.status(200).json({ received: true, status: 'chaos_success_after_failures' });
});

// Endpoint 5: Configure chaos endpoint behavior (not part of /webhooks, bypassed from auth)
app.post('/chaos/setup', (req, res) => {
  const { endpoint_id, fail_count } = req.body;
  if (!endpoint_id || fail_count === undefined) {
    return res.status(400).json({ error: 'endpoint_id and fail_count are required' });
  }

  chaosConfigs.set(endpoint_id, {
    fail_count: parseInt(fail_count, 10),
    current_fails: 0
  });

  console.log(`[MockMerchant] Configured Chaos endpoint ${endpoint_id} to fail ${fail_count} times before success.`);
  res.status(200).json({ message: 'Chaos configured successfully', config: chaosConfigs.get(endpoint_id) });
});

app.listen(PORT, () => {
  console.log(`[MockMerchant] Chaos endpoint service started on port ${PORT}`);
});
