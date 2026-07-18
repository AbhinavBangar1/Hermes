import crypto from 'crypto';
import dns from 'dns/promises';
import { URL } from 'url';

/**
 * Generates an HMAC-SHA256 signature for webhook verification.
 * Format: t=timestamp,v1=signature
 * @param {object|string} payload - JSON payload or raw string
 * @param {string} secret - Per-merchant signing secret
 * @param {number} timestamp - Epoch timestamp in seconds
 * @returns {string} - Header signature string
 */
export function generateSignature(payload, secret, timestamp) {
  const serialized = typeof payload === 'object' ? JSON.stringify(payload) : payload;
  const signedContent = `${timestamp}.${serialized}`;
  const signature = crypto.createHmac('sha256', secret).update(signedContent).digest('hex');
  return `t=${timestamp},v1=${signature}`;
}

/**
 * Verifies a signature header against the payload.
 * Prevents replay attacks by checking timestamp window (5 min).
 * Prevents timing attacks using crypto.timingSafeEqual.
 * @param {object|string} payload - JSON payload or raw string
 * @param {string} header - The signature header value (t=...,v1=...)
 * @param {string} secret - Per-merchant signing secret
 * @param {number} [toleranceSeconds=300] - Replay attack window
 * @returns {boolean}
 */
export function verifySignature(payload, header, secret, toleranceSeconds = 300) {
  if (!header) return false;

  // Parse header
  const parts = header.split(',');
  const timestampPart = parts.find(p => p.startsWith('t='));
  const signaturePart = parts.find(p => p.startsWith('v1='));

  if (!timestampPart || !signaturePart) return false;

  const timestamp = parseInt(timestampPart.split('=')[1], 10);
  const signature = signaturePart.split('=')[1];

  if (isNaN(timestamp) || !signature) return false;

  // Replay Attack Validation
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - timestamp) > toleranceSeconds) {
    console.warn(`[Security] Stale signature timestamp: diff=${Math.abs(now - timestamp)}s`);
    return false;
  }

  // Recalculate signature
  const serialized = typeof payload === 'object' ? JSON.stringify(payload) : payload;
  const expectedContent = `${timestamp}.${serialized}`;
  const expectedSignature = crypto.createHmac('sha256', secret).update(expectedContent).digest('hex');

  // Timing Attack Protection
  const a = Buffer.from(signature, 'hex');
  const b = Buffer.from(expectedSignature, 'hex');

  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

/**
 * Evaluates whether an IP address belongs to a private/loopback/local network range.
 * @param {string} ip
 * @returns {boolean}
 */
export function isPrivateIp(ip) {
  // IPv4 Loopback and Any
  if (ip === '0.0.0.0' || ip === '127.0.0.1') return true;

  // IPv4 Private ranges
  const ipv4Pattern = /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/;
  const match = ip.match(ipv4Pattern);
  if (match) {
    const [, p1, p2, p3, p4] = match.map(Number);
    if (p1 === 10) return true; // Class A
    if (p1 === 172 && p2 >= 16 && p2 <= 31) return true; // Class B
    if (p1 === 192 && p2 === 168) return true; // Class C
    if (p1 === 169 && p2 === 254) return true; // Link-local
    if (p1 === 127) return true; // Loopback
    if (p1 === 0) return true; // Broadcast/Reserved
  }

  // IPv6 Loopback and Unspecified
  const cleanIp = ip.toLowerCase().trim();
  if (cleanIp === '::' || cleanIp === '::1' || cleanIp === '0:0:0:0:0:0:0:1') return true;

  // Link-local IPv6 (fe80::/10)
  if (cleanIp.startsWith('fe80:')) return true;

  // Unique local address IPv6 (fc00::/7)
  if (cleanIp.startsWith('fc') || cleanIp.startsWith('fd')) return true;

  return false;
}

/**
 * Validates a target URL domain and resolves it to check for SSRF.
 * @param {string} urlString
 * @returns {Promise<string>} - Resolves with target IP on success
 * @throws {Error} - If domain points to private IP
 */
export async function validateSSRF(urlString) {
  const urlObj = new URL(urlString);
  const hostname = urlObj.hostname;

  // Resolve hostname
  const lookupResult = await dns.lookup(hostname);
  const ip = lookupResult.address;

  if (isPrivateIp(ip)) {
    throw new Error(`SSRF Prevention: Destination hostname resolved to private IP (${ip})`);
  }

  return ip;
}
