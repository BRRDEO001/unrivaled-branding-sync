import fetch from "node-fetch";
import fs from "fs";
import path from "path";

import {
  AMROD_AUTH_ENDPOINT,
  AMROD_PRODUCTS_ENDPOINT,
  AMROD_AUTH_DETAILS,
  SHOPIFY_STORE,
  SHOPIFY_TOKEN,
  SHOPIFY_API_VERSION,
  AMROD_TEST_LIMIT,
  REQUEST_DELAY_MS,
  LOG_DIR,
} from "./config.js";

/* ---------------- CONFIG ---------------- */

const SOURCE_LOCATION_ID = process.env.SOURCE_LOCATION_ID;
const TARGET_LOCATION_IDS = process.env.TARGET_LOCATION_IDS.split(",");

/* ---------------------------------------- */

const SHOPIFY_ENDPOINT = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

/* ---------------------------------------- */

function delay(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function log(msg) {
  console.log(msg);

  if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR);
  }

  const file = path.join(LOG_DIR, "transfer.log");

  fs.appendFileSync(file, `[${new Date().toISOString()}] ${msg}\n`);
}

/* ---------------------------------------- */
/* AMROD AUTH */
/* ---------------------------------------- */

async function amrodLogin() {
  log("Authenticating with Amrod...");

  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(AMROD_AUTH_DETAILS),
  });

  const data = await res.json();

  if (!data?.Token) {
    throw new Error("Amrod authentication failed");
  }

  log("Amrod authenticated");

  return data.Token;
}

/* ---------------------------------------- */
/* AMROD PRODUCTS */
/* ---------------------------------------- */

async function getAmrodProducts(token) {
  log("Fetching Amrod products...");

  const res = await fetch(AMROD_PRODUCTS_ENDPOINT, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  const data = await res.json();

  let products = data?.Products || [];

  if (AMROD_TEST_LIMIT) {
    products = products.slice(0, AMROD_TEST_LIMIT);
  }

  log(`Fetched ${products.length} Amrod products`);

  return products;
}

/* ---------------------------------------- */
/* SHOPIFY REQUEST */
/* ---------------------------------------- */

async function shopify(query, variables = {}) {
  const res = await fetch(SHOPIFY_ENDPOINT, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": SHOPIFY_TOKEN,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  return res.json();
}

/* ---------------------------------------- */
/* GET VARIANT BY SKU */
/* ---------------------------------------- */

async function findVariantBySku(sku) {
  const query = `
    query ($query: String!) {
      productVariants(first: 1, query: $query) {
        edges {
          node {
            id
            sku
            inventoryItem {
              id
            }
          }
        }
      }
    }
  `;

  const data = await shopify(query, {
    query: `sku:${sku}`,
  });

  return data?.data?.productVariants?.edges?.[0]?.node || null;
}

/* ---------------------------------------- */
/* GET INVENTORY */
/* ---------------------------------------- */

async function getInventory(itemId) {
  const query = `
    query ($id: ID!) {
      inventoryItem(id: $id) {
        inventoryLevels(first: 10) {
          edges {
            node {
              location {
                id
              }
              available
            }
          }
        }
      }
    }
  `;

  const data = await shopify(query, { id: itemId });

  return data?.data?.inventoryItem?.inventoryLevels?.edges || [];
}

/* ---------------------------------------- */
/* SET INVENTORY */
/* ---------------------------------------- */

async function setInventory(itemId, locationId, qty) {
  const mutation = `
    mutation ($itemId: ID!, $locationId: ID!, $qty: Int!) {
      inventorySetQuantityAtLocation(
        inventoryItemId: $itemId
        locationId: $locationId
        availableQuantity: $qty
      ) {
        inventoryLevel {
          id
          available
        }
        userErrors {
          message
        }
      }
    }
  `;

  return shopify(mutation, {
    itemId,
    locationId: `gid://shopify/Location/${locationId}`,
    qty,
  });
}

/* ---------------------------------------- */
/* MAIN */
/* ---------------------------------------- */

async function run() {
  log("=== TRANSFER INVENTORY STARTED ===");

  if (!SOURCE_LOCATION_ID || !TARGET_LOCATION_IDS.length) {
    throw new Error("Missing location IDs");
  }

  /* Login */
  const token = await amrodLogin();

  /* Fetch Amrod */
  const amrodProducts = await getAmrodProducts(token);

  let success = 0;
  let failed = 0;

  for (const product of amrodProducts) {
    const sku = product?.Code;
    const stock = product?.AvailableStock;

    if (!sku || stock === undefined) continue;

    log(`Processing SKU: ${sku} | Stock: ${stock}`);

    /* Find Shopify variant */
    const variant = await findVariantBySku(sku);

    if (!variant) {
      log(`❌ Not found in Shopify: ${sku}`);
      failed++;
      continue;
    }

    const inventoryItemId = variant.inventoryItem.id;

    /* Get source quantity */
    const levels = await getInventory(inventoryItemId);

    const source = levels.find((l) =>
      l.node.location.id.includes(SOURCE_LOCATION_ID)
    );

    const qty = source ? source.node.available : stock;

    /* Sync to targets */
    for (const loc of TARGET_LOCATION_IDS) {
      await setInventory(inventoryItemId, loc, qty);

      log(` → Synced ${sku} → ${loc} (${qty})`);

      await delay(REQUEST_DELAY_MS);
    }

    success++;
  }

  log(`Done: ${success} success | ${failed} failed`);
  log("=== TRANSFER COMPLETE ===");
}

/* ---------------------------------------- */

run().catch((err) => {
  log(`FATAL: ${err.message}`);
  process.exit(1);
});
