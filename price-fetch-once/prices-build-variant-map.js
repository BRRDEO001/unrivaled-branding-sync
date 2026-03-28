#!/usr/bin/env node
import fs from "fs";
import { SHOPIFY_STORE, SHOPIFY_TOKEN, SHOPIFY_API_VERSION } from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

async function shopifyFetch(endpoint, retries = 8) {
  const url = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/${endpoint}`;

  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, {
      headers: {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
      },
    });

    if (res.status === 429 || res.status >= 500) {
      await sleep(500 * (i + 1));
      continue;
    }

    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }

  throw new Error("Shopify fetch failed");
}

async function fetchAllVariants() {
  const limit = 250;
  let since_id = 0;
  const out = [];

  while (true) {
    const data = await shopifyFetch(`variants.json?limit=${limit}&since_id=${since_id}`);
    const batch = data.variants || [];
    if (!batch.length) break;

    out.push(...batch);
    since_id = batch[batch.length - 1].id;
  }

  return out;
}

function toVariantGid(numericId) {
  return `gid://shopify/ProductVariant/${numericId}`;
}

function toProductGid(numericId) {
  return `gid://shopify/Product/${numericId}`;
}

(async () => {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) {
    throw new Error("Missing Shopify credentials");
  }

  ensureDir("data");
  ensureDir("logs");

  console.log("🔎 Fetching all Shopify variants...");
  const variants = await fetchAllVariants();

  // map[sku] = { variantId, productId }
  const map = {};
  for (const v of variants) {
    const sku = String(v.sku || "").trim();
    if (!sku || map[sku]) continue;

    map[sku] = {
      variantId: toVariantGid(v.id),
      productId: toProductGid(v.product_id),
    };
  }

  fs.writeFileSync("data/variant-map.json", JSON.stringify(map, null, 2), "utf8");
  console.log(`✅ variant-map.json created (${Object.keys(map).length} SKUs)`);
})().catch((e) => {
  console.error("🔥 Failed:", e.message);
  process.exit(1);
});
