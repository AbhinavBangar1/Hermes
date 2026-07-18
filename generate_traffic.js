async function run() {
  console.log('Generating traffic...');
  try {

    const merchantRes = await fetch('http://localhost:3000/merchants', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Test Merchant for Resume' })
    });
    const merchant = await merchantRes.json();
    console.log('Merchant created:', merchant.id);


    const endpoints = [
      'http://localhost:4000/webhooks/success',
      'http://localhost:4000/webhooks/fail/503', // Will retry 5 times and DLQ
      'http://localhost:4000/webhooks/timeout?ms=6000' // Will timeout if timeout is < 6s
    ];

    const endpointIds = [];
    for (const url of endpoints) {
      const epRes = await fetch('http://localhost:3000/endpoints', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ merchant_id: merchant.id, url })
      });
      const ep = await epRes.json();
      endpointIds.push(ep.id);
      console.log('Endpoint created:', ep.id, url);
    }


    const chaosUrl = `http://localhost:4000/webhooks/chaos/${endpointIds[0]}-chaos`;
    const chaosEpRes = await fetch('http://localhost:3000/endpoints', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ merchant_id: merchant.id, url: chaosUrl })
    });
    const chaosEp = await chaosEpRes.json();
    console.log('Chaos endpoint created:', chaosEp.id);

    // Setup chaos behavior: Fail 3 times, then succeed
    await fetch('http://localhost:4000/chaos/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint_id: `${endpointIds[0]}-chaos`, fail_count: 3 })
    });


    console.log('Firing events...');
    for (let i = 0; i < 50; i++) {
      await fetch('http://localhost:3000/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          merchant_id: merchant.id,
          event_type: 'payment.succeeded',
          payload: { amount: 1000 + i, currency: 'usd' }
        })
      });
      if (i % 10 === 0) console.log(`Fired ${i} events...`);
    }
    console.log('Finished producing events.');
    
    console.log('Waiting for processing to complete... (will take around 15 seconds to allow for retries)');
    await new Promise(r => setTimeout(r, 15000));
    
    // Fetch metrics
    const metricsRes = await fetch('http://localhost:5000/api/metrics');
    const metrics = await metricsRes.json();
    console.log('--- METRICS ---');
    console.log(JSON.stringify(metrics, null, 2));

  } catch (error) {
    console.error('Error:', error);
  }
}
run();
