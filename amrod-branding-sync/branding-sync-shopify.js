// amrod-branding-sync/branding-sync-shopify.js

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---- ENV ----
const { SHOPIFY_STORE, SHOPIFY_TOKEN, SHOPIFY_API_VERSION } = process.env;

if (!SHOPIFY_STORE || !SHOPIFY_TOKEN || !SHOPIFY_API_VERSION) {
  console.error('❌ Missing Shopify environment variables');
  process.exit(1);
}

const SHOPIFY_GRAPHQL_ENDPOINT = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

// ---- LOAD BRANDING JSON ----
const brandingFile = path.join(__dirname, 'branding-prices.json');

if (!fs.existsSync(brandingFile)) {
  console.error('❌ branding-prices.json not found. Run branding-fetch.js first.');
  process.exit(1);
}

const brandingPrices = JSON.parse(fs.readFileSync(brandingFile, 'utf8'));

// ---- GRAPHQL CALL ----
async function shopifyGraphQL(query, variables = {}) {
  const res = await fetch(SHOPIFY_GRAPHQL_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': SHOPIFY_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });

  const json = await res.json();
  if (json.errors) throw new Error(JSON.stringify(json.errors, null, 2));
  return json.data;
}

// ---- MAIN ----
try {
  console.log('🔍 Fetching Shopify shop GID...');
  const shopData = await shopifyGraphQL(`{ shop { id } }`);
  const shopId = shopData.shop.id;
  console.log('📌 Shop GID:', shopId);

  console.log('🧾 Syncing branding prices to Shopify...');
  const mutation = `
    mutation SetBrandingPrices($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          namespace
          key
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    metafields: [
      {
        ownerId: shopId,
        namespace: "amrod",
        key: "branding_prices",
        type: "json",
        value: JSON.stringify(brandingPrices)
      }
    ]
  };

  const data = await shopifyGraphQL(mutation, variables);

  const errors = data.metafieldsSet.userErrors;
  if (errors.length) throw new Error(JSON.stringify(errors, null, 2));

  const metafield = data.metafieldsSet.metafields[0];
  console.log('✅ Branding prices stored in Shopify');
  console.log(`📍 Metafield: ${metafield.namespace}.${metafield.key}`);

} catch (err) {
  console.error('🔥 Shopify branding sync failed');
  console.error(err.message);
  process.exit(1);
}
