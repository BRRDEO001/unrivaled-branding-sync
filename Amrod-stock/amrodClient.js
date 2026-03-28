const fetch = require('node-fetch');

const AMROD_STOCK_ENDPOINT =
  'https://vendorapi.amrod.co.za/api/v1/Stock/';

async function fetchAmrodStock(token) {
  const response = await fetch(AMROD_STOCK_ENDPOINT, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/json'
    }
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Amrod stock fetch failed (${response.status}): ${text}`
    );
  }

  return response.json();
}

module.exports = {
  fetchAmrodStock
};
