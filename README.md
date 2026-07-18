# Hermes: Reliable Webhook Delivery Engine

A production-grade, fault-tolerant webhook delivery system built in Node.js (JavaScript) utilizing **PostgreSQL** (via Supabase) and **Redis Streams**. Designed to address key distributed systems and reliability engineering challenges at scale.

## Core Architectural Features

1. **At-Least-Once Delivery (Transactional Outbox Pattern):** Prevents dual-write inconsistencies. API events and webhook tasks are written to PostgreSQL in a single database transaction. An outbox daemon polls tasks using `SELECT FOR UPDATE SKIP LOCKED` and publishes them to Redis Streams.
2. **Exponential Backoff & Jitter:** Deliveries are retried with exponential increments (`1s, 5s, 30s, 2m, 10m`) incorporating **Full Jitter** to prevent the thundering herd problem.
3. **Per-Endpoint Circuit Breaker:** tri-state client circuit (`CLOSED` -> `OPEN` -> `HALF-OPEN`). Tripping blocks downstream delivery attempts for failing merchant endpoints, protecting worker concurrency resources.
4. **HMAC-SHA256 Payload Signing:** Signatures include epoch timestamps to guard receiving endpoints against replay attacks, utilizing timing-safe memory comparisons to prevent timing attacks.
5. **SSRF Prevention:** Workers resolve target webhook domains and block deliveries pointing to loopback, link-local, or private IP networks (`SSRF_PREVENTION_ENABLED=true`).
6. **Observability & Manual Re-drive:** A glassmorphic dashboard visualizing P50/P99 latencies, delivery metrics, active circuit states, and a Dead Letter Queue (DLQ) with one-click redrive capability.

---

## Architecture Flow

![System Architecture](Arc.svg)

---

## Getting Started

### 1. Setup Infrastructure
* **PostgreSQL:** Set up a free project on [Supabase](https://supabase.com).
* **Redis:** Ensure Redis is running locally on port `6379`.

### 2. Configure Environment (`.env`)
Create or edit the `.env` file in the root directory:
```env
# Database connection (Paste your Supabase transactional connection pooler string here)
DATABASE_URL=postgresql://postgres.[YOUR_PROJECT_ID]:[YOUR_PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres?pgbouncer=true

# Redis Address
REDIS_URL=redis://localhost:6379

# Worker configuration
MAX_ATTEMPTS=5
SSRF_PREVENTION_ENABLED=true
```

### 3. Initialize & Start
Install dependencies, initialize the database schema in Supabase, and start the system in development mode:
```bash
# Install dependencies
npm install

# Create tables and types in Supabase
npm run db:init

# Spin up all 5 microservices simultaneously (Producer, Worker, Scheduler, Mock Merchant, Dashboard)
npm run dev
```

---

## Chaos Testing & Verification Guide

Use the following curl commands to verify reliability behaviors in real-time on your dashboard (`http://localhost:5000`):

### 1. Setup a Merchant & Endpoint
Register a merchant and register the Mock Merchant Server's `/success` endpoint:
```bash
# Register Merchant
curl -X POST http://localhost:3000/merchants \
  -H "Content-Type: application/json" \
  -d '{"name": "Stripe Summer Merchant"}'

# (Copy the generated merchant ID from response)

# Register Webhook Endpoint pointing to Mock Merchant
curl -X POST http://localhost:3000/endpoints \
  -H "Content-Type: application/json" \
  -d '{"merchant_id": "YOUR_MERCHANT_ID", "url": "http://localhost:4000/webhooks/success"}'

# (Copy the generated endpoint ID from response)
```

### 2. Verify Successful Delivery
Trigger an event. The worker should sign it, verify it at the mock merchant, and mark it `delivered` instantly:
```bash
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{
    "merchant_id": "YOUR_MERCHANT_ID",
    "event_type": "payment.succeeded",
    "payload": {"amount": 2000, "currency": "usd"}
  }'
```

### 3. Test Retries & Backoff with Jitter (Fail-then-Succeed Chaos)
Configure the chaos endpoint on the mock server to fail 3 times before succeeding, then register and trigger:
```bash
# Setup Chaos endpoint to fail 3 times
curl -X POST http://localhost:4000/chaos/setup \
  -H "Content-Type: application/json" \
  -d '{"endpoint_id": "YOUR_ENDPOINT_ID", "fail_count": 3}'

# Register the Chaos Endpoint URL
curl -X POST http://localhost:3000/endpoints \
  -H "Content-Type: application/json" \
  -d '{"merchant_id": "YOUR_MERCHANT_ID", "url": "http://localhost:4000/webhooks/chaos/YOUR_ENDPOINT_ID"}'

# Trigger the event
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{
    "merchant_id": "YOUR_MERCHANT_ID",
    "event_type": "subscription.created",
    "payload": {"subscription_id": "sub_123"}
  }'
```
*Observe the dashboard:* You will see the event status transition through retries with exponential timings (1s, 5s, 30s) before succeeding on the 4th attempt.

### 4. Test Circuit Breaker Tripping
Register a broken endpoint that always returns `503 Service Unavailable`:
```bash
# Register a broken endpoint URL
curl -X POST http://localhost:3000/endpoints \
  -H "Content-Type: application/json" \
  -d '{"merchant_id": "YOUR_MERCHANT_ID", "url": "http://localhost:4000/webhooks/fail/503"}'

# (Copy this endpoint ID)
```
Trigger 5 events to this endpoint. On the 5th failure, you will see the endpoint's circuit breaker transition to **OPEN** on the dashboard status panel. Any further events sent to this endpoint will fast-fail instantly to protect resources.

### 5. Verify DLQ & Re-drive
Once an event exhausts all 5 attempts, it is moved to the **Dead Letter Queue (DLQ)**.
1. Inspect the DLQ section on the dashboard (`http://localhost:5000`).
2. Click **Re-drive** next to the failed event.
3. Observe it reset, re-queue, and attempt delivery again!
