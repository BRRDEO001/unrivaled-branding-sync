// Amrod-stock/stock-fetch.js

const AMROD_AUTH_ENDPOINT = 'https://identity.amrod.co.za/VendorLogin';
const AMROD_STOCK_ENDPOINT = 'https://vendorapi.amrod.co.za/api/v1/Stock/';

// ---- ENV ----
const {
  AMROD_USERNAME,
  AMROD_PASSWORD,
  AMROD_CUSTOMER_CODE
} = process.env;

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

  if (!data?.token) {
    throw new Error('Auth response missing token');
  }

  return data.token;
}

// ---- STOCK ----
async function fetchStock(token) {
  const res = await fetch(AMROD_STOCK_ENDPOINT, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Stock fetch failed (${res.status}): ${text}`);
  }

  return res.json();
}

// ---- MAIN ----
(async () => {
  try {
    console.log('🔐 Fetching Amrod token...');
    const token = await fetchAmrodToken();

    console.log('📦 Fetching stock...');
    const stock = await fetchStock(token);

    console.log(`✅ Stock records fetched: ${stock.length}`);

    // TODO: next step → map to Shopify variants
    // console.log(stock[0]);

  } catch (err) {
    console.error('🔥 Stock fetch failed');
    console.error(err.message);
    process.exit(1);
  }
})();
