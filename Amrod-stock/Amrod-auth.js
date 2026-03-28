/**
 * Fetches Amrod auth token and exports it to GITHUB_ENV
 * No external dependencies
 */

const fs = require('fs');

const AUTH_ENDPOINT = 'https://identity.amrod.co.za/VendorLogin';

async function authenticate() {
  const res = await fetch(AUTH_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      Username: process.env.AMROD_USERNAME,
      Password: process.env.AMROD_PASSWORD,
      CustomerCode: process.env.AMROD_CUSTOMER_CODE
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Amrod auth failed (${res.status}): ${text}`
    );
  }

  const data = await res.json();

  if (!data || !data.token) {
    throw new Error('Auth response missing token');
  }

  // Mask token in logs
  console.log(`::add-mask::${data.token}`);

  // Export token for next workflow step
  fs.appendFileSync(
    process.env.GITHUB_ENV,
    `AMROD_TOKEN=${data.token}\n`
  );

  console.log('🔐 Amrod authentication successful');
}

authenticate().catch(err => {
  console.error('🔥 Authentication failed');
  console.error(err.message);
  process.exit(1);
});
