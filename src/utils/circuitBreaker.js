import pool from '../config/db.js';

/**
 * Checks the current circuit state of an endpoint, handling the OPEN -> HALF-OPEN transition
 * dynamically if the cooldown period has expired.
 * @param {object} client - pg DB client (supports transaction isolation)
 * @param {string} endpointId - UUID of the endpoint
 * @returns {Promise<object>} - Endpoint circuit status
 */
export async function getEndpointCircuitStatus(client, endpointId) {
  const query = `
    SELECT id, url, is_active, circuit_breaker_state, consecutive_failures, cooldown_until
    FROM webhook_endpoints
    WHERE id = $1
  `;
  const res = await client.query(query, [endpointId]);
  if (res.rows.length === 0) {
    throw new Error('Endpoint not found');
  }

  const endpoint = res.rows[0];

  // If active check for OPEN cooldown transitions
  if (endpoint.circuit_breaker_state === 'OPEN') {
    const now = new Date();
    if (endpoint.cooldown_until && new Date(endpoint.cooldown_until) <= now) {
      // Transition to HALF-OPEN atomically
      const updateQuery = `
        UPDATE webhook_endpoints
        SET circuit_breaker_state = 'HALF-OPEN'
        WHERE id = $1
        RETURNING id, url, is_active, circuit_breaker_state
      `;
      const updateRes = await client.query(updateQuery, [endpointId]);
      return updateRes.rows[0];
    }
  }

  return endpoint;
}

/**
 * Updates the circuit breaker state to CLOSED upon a successful delivery probe.
 * @param {object} client - pg DB client
 * @param {string} endpointId
 */
export async function recordSuccess(client, endpointId) {
  const query = `
    UPDATE webhook_endpoints
    SET circuit_breaker_state = 'CLOSED',
        consecutive_failures = 0,
        cooldown_until = NULL
    WHERE id = $1
  `;
  await client.query(query, [endpointId]);
}

/**
 * Handles consecutive failures, transitioning the circuit to OPEN when thresholds are crossed.
 * Implements an exponential cooldown backoff based on total consecutive failures.
 * @param {object} client - pg DB client
 * @param {string} endpointId
 * @param {number} [failureThreshold=5]
 * @param {number} [baseCooldownSeconds=30]
 */
export async function recordFailure(client, endpointId, failureThreshold = 5, baseCooldownSeconds = 30) {
  // First increment failures
  const fetchQuery = `
    UPDATE webhook_endpoints
    SET consecutive_failures = consecutive_failures + 1
    WHERE id = $1
    RETURNING consecutive_failures, circuit_breaker_state
  `;
  const res = await client.query(fetchQuery, [endpointId]);
  const { consecutive_failures, circuit_breaker_state } = res.rows[0];

  // If consecutive failures exceed threshold, trip the circuit
  if (consecutive_failures >= failureThreshold || circuit_breaker_state === 'HALF-OPEN') {
    // Calculate exponential cooldown: 30s * 2^(multiplier)
    const multiplier = Math.max(0, consecutive_failures - failureThreshold);
    const cooldownDurationSeconds = baseCooldownSeconds * Math.pow(2, multiplier);
    const cooldownUntil = new Date(Date.now() + cooldownDurationSeconds * 1000);

    const tripQuery = `
      UPDATE webhook_endpoints
      SET circuit_breaker_state = 'OPEN',
          cooldown_until = $2
      WHERE id = $1
    `;
    await client.query(tripQuery, [endpointId, cooldownUntil]);
    console.warn(`[CircuitBreaker] Endpoint ${endpointId} TRIPPED to OPEN. Cooldown until ${cooldownUntil.toISOString()} (${cooldownDurationSeconds}s)`);
  }
}
