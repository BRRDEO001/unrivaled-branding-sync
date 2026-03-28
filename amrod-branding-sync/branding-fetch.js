// amrod-branding-sync/branding-fetch.js

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const AMROD_AUTH_ENDPOINT = 'https://identity.amrod.co.za/VendorLogin';
const AMROD_BRANDING_ENDPOINT = 'https://vendorapi.amrod.co.za/api/v1/BrandingPrices/';

// ---- ENV ----
const { AMROD_USERNAME, AMROD_PASSWORD, AMROD_CUSTOMER_CODE } = process.env;

if (!AMROD_USERNAME || !AMROD_PASSWORD || !AMROD_CUSTOMER_CODE) {
  console.error('❌ Missing Amrod credentials');
  process.exit(1);
}

// ---- AUTH ----
async function fetchAmrodToken() {
  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      Username: AMROD_USERNAME,
      Password: AMROD_PASSWORD,
      CustomerCode: AMROD_CUSTOMER_CODE
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Auth failed (${res.status}): ${text}`);
  }

  const data = await res.json();
  if (!data?.token) throw new Error('Auth response missing token');
  return data.token;
}

// ---- FETCH BRANDING PRICES ----
async function fetchBrandingPrices(token) {
  const res = await fetch(AMROD_BRANDING_ENDPOINT, {
    headers: { Authorization: `Bearer ${token}` }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Branding price fetch failed (${res.status}): ${text}`);
  }

  return res.json();
}

// ---- MAIN ----
try {
  console.log('🔐 Authenticating with Amrod...');
  const token = await fetchAmrodToken();

  console.log('🎨 Fetching branding prices...');
  const brandingPrices = await fetchBrandingPrices(token);

  if (!Array.isArray(brandingPrices)) {
    throw new Error('Unexpected branding price response format');
  }

  const outputPath = path.join(__dirname, 'branding-prices.json');

  fs.writeFileSync(outputPath, JSON.stringify(brandingPrices, null, 2), 'utf8');

  console.log(`✅ Branding prices saved`);
  console.log(`📄 Records: ${brandingPrices.length}`);
  console.log(`📍 File: ${outputPath}`);

} catch (err) {
  console.error('🔥 Branding price fetch failed');
  console.error(err.message);
  process.exit(1);
}
