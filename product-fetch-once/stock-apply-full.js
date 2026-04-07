#!/usr/bin/env node
/**
 * GET /api/v1/Stock/ (full list) → Shopify inventory at the **primary** location.
 * Optional: SHOPIFY_LOCATION_IDS — only the first ID is used if set.
 */
import fs from "fs";
import { fileURLToPath } from "url";
import { fetchAmrodToken, fetchStockAll } from "./amrod.js";
import {
  findShopifyVariantBySkuCandidates,
  setInventoryLevel,
  getPrimaryLocationId,
} from "./shopify.js";
import { REQUEST_DELAY_MS } from "./config.js";

/** fullCode → total available quantity */
function aggregateStock(rows) {
  const m = new Map();
  for (const item of rows) {
    const code = item.fullCode;
    if (!code) continue;
    const n = Number(item.stock ?? item.quantity ?? 0);
    if (!Number.isFinite(n)) continue;
    m.set(code, (m.get(code) || 0) + n);
  }
  return m;
}

function filterEntriesByShard(entries) {
  const n = Math.max(1, Number(process.env.SHARD_COUNT || 1));
  const i = Number(process.env.SHARD_INDEX || 0);
  if (n <= 1) return entries;
  return entries.filter((_, idx) => idx % n === i);
}

export async function runStockFullSyncToShopify() {
  const locRaw = process.env.SHOPIFY_LOCATION_IDS?.trim();
  let locationId;
  if (locRaw) {
    locationId = Number(locRaw.split(",")[0].trim());
    if (!Number.isFinite(locationId)) {
      console.log("::notice::SHOPIFY_LOCATION_IDS first value invalid — skipping stock apply");
      return;
    }
    console.log(`📍 Stock apply: location ${locationId} (from SHOPIFY_LOCATION_IDS)`);
  } else {
    locationId = await getPrimaryLocationId();
    if (locationId == null) {
      console.log("::notice::No primary Shopify location — skipping stock apply");
      return;
    }
    console.log(`📍 Stock apply: primary location ${locationId}`);
  }

  const fromFile = process.env.AMROD_STOCK_JSON;
  let rows;

  if (fromFile) {
    console.log(`📂 Loading stock from ${fromFile} ...`);
    rows = JSON.parse(fs.readFileSync(fromFile, "utf8"));
    if (!Array.isArray(rows)) throw new Error("AMROD_STOCK_JSON must be a JSON array");
  } else {
    const token = await fetchAmrodToken();
    rows = await fetchStockAll(token);
    console.log(`📬 Stock (all): ${rows.length} row(s) raw`);
  }

  const bySku = aggregateStock(rows);
  console.log(`📊 Unique variant SKUs: ${bySku.size}`);

  let entries = Array.from(bySku.entries()).sort((a, b) =>
    String(a[0]).localeCompare(String(b[0]), "en")
  );
  const before = entries.length;
  entries = filterEntriesByShard(entries);
  console.log(
    `🔢 Stock shard: SHARD_COUNT=${process.env.SHARD_COUNT || 1} SHARD_INDEX=${process.env.SHARD_INDEX || 0} → ${entries.length}/${before} SKUs`
  );

  let ok = 0;
  let miss = 0;

  for (const [fullCode, qty] of entries) {
    const rec = await findShopifyVariantBySkuCandidates([fullCode]);
    if (!rec?.inventoryItemId) {
      miss++;
      continue;
    }

    const q = Math.max(0, Math.floor(qty));
    try {
      await setInventoryLevel(rec.inventoryItemId, locationId, q);
    } catch (e) {
      console.log(
        `::warning title=Stock set failed::${fullCode} loc ${locationId} | ${String(e?.message || e)}`
      );
    }
    ok++;
    await new Promise((r) => setTimeout(r, REQUEST_DELAY_MS));
  }

  console.log(`✅ Stock levels updated: ${ok} SKUs, ${miss} not found in Shopify`);
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  runStockFullSyncToShopify().catch((e) => {
    console.error("🔥 Stock apply failed:", e?.message || e);
    process.exit(1);
  });
}
