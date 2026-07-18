import pool from '../config/db.js';

const run = async () => {
  const client = await pool.connect();
  try {
    console.log('Initializing PostgreSQL database schema...');

    // Enable uuid extensions if available (safe fallback)
    await client.query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`);

    // Drop tables in reverse dependency order
    await client.query(`DROP TABLE IF EXISTS delivery_attempts CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS outbox_tasks CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS events CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS webhook_endpoints CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS merchants CASCADE;`);

    // Drop types
    await client.query(`DROP TYPE IF EXISTS circuit_state CASCADE;`);
    await client.query(`DROP TYPE IF EXISTS event_status CASCADE;`);

    // Create custom types
    await client.query(`CREATE TYPE circuit_state AS ENUM ('CLOSED', 'OPEN', 'HALF-OPEN');`);
    await client.query(`CREATE TYPE event_status AS ENUM ('pending', 'sending', 'delivered', 'failed', 'dlq');`);

    // 1. Create Merchants Table
    await client.query(`
      CREATE TABLE merchants (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          name VARCHAR(255) NOT NULL,
          webhook_secret VARCHAR(255) NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('- Created table: merchants');

    // 2. Create Webhook Endpoints Table
    await client.query(`
      CREATE TABLE webhook_endpoints (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          merchant_id UUID REFERENCES merchants(id) ON DELETE CASCADE,
          url VARCHAR(2048) NOT NULL,
          is_active BOOLEAN DEFAULT TRUE,
          circuit_breaker_state circuit_state DEFAULT 'CLOSED',
          consecutive_failures INTEGER DEFAULT 0,
          cooldown_until TIMESTAMP WITH TIME ZONE,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('- Created table: webhook_endpoints');

    // 3. Create Events Table
    await client.query(`
      CREATE TABLE events (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          merchant_id UUID REFERENCES merchants(id) ON DELETE CASCADE,
          event_type VARCHAR(100) NOT NULL,
          payload JSONB NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('- Created table: events');

    // 4. Create Outbox Tasks Table
    await client.query(`
      CREATE TABLE outbox_tasks (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_id UUID REFERENCES events(id) ON DELETE CASCADE,
          endpoint_id UUID REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
          status event_status DEFAULT 'pending',
          attempts_count INTEGER DEFAULT 0,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('- Created table: outbox_tasks');

    // 5. Create Delivery Attempts Table
    await client.query(`
      CREATE TABLE delivery_attempts (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_id UUID REFERENCES events(id) ON DELETE CASCADE,
          endpoint_id UUID REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
          attempt_number INTEGER NOT NULL,
          response_status INTEGER,
          response_headers JSONB,
          response_body TEXT,
          execution_duration_ms INTEGER,
          error_message TEXT,
          sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('- Created table: delivery_attempts');

    console.log('PostgreSQL schema successfully initialized!');
  } catch (error) {
    console.error('Fatal error during schema initialization:', error);
  } finally {
    client.release();
    await pool.end();
  }
};

run();
