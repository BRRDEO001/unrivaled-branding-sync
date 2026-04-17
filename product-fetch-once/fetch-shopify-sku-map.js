#!/usr/bin/env node
/**
 * Pre-fetch all Shopify variant SKUs into a JSON map file.
 * Run once before sharded stock-apply jobs so each shard can do
 * local lookups instead of per-SKU GraphQL queries.
 */
import fs from "fs";
import { fetchAllVariantSkuMap } from "./shopify.js";

const OUT = process.env.SHOPIFY_SKU_MAP_OUT || "data/shopify-sku-map.json";

(async () => {
  console.log("📥 Fetching all Shopify variant SKUs...");
  const map = await fetchAllVariantSkuMap();

  const obj = {};
  for (const [sku, info] of map) {
    obj[sku] = info;
  }

  fs.mkdirSync("data", { recursive: true });
  const json = JSON.stringify(obj);
  fs.writeFileSync(OUT, json);

  const mb = (Buffer.byteLength(json) / 1024 / 1024).toFixed(1);
  console.log(`✅ Wrote ${map.size} SKU entries to ${OUT} (${mb} MB)`);
})().catch((e) => {
  console.error("🔥 SKU map fetch failed:", e?.message || e);
  process.exit(1);
});
