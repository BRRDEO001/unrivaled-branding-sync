#!/usr/bin/env node
/**
 * GET /api/v1/Stock/ (full list) → Shopify inventory per SHOPIFY_LOCATION_IDS.
 * Run after Amrod’s daily stock window (e.g. 00:30 SAST); sums `stock` by variant fullCode.
 */
import { fileURLToPath } from "url";
import { fetchAmrodToken, fetchStockAll } from "./amrod.js";
import { findShopifyVariantBySkuCandidates, setInventoryLevel } from "./shopify.js";
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

export async function runStockFullSyncToShopify() {
  const locRaw = process.env.SHOPIFY_LOCATION_IDS;
  if (!locRaw?.trim()) {
    console.log("::notice::SHOPIFY_LOCATION_IDS not set — skipping stock apply");
    return;
  }
  const locations = locRaw.split(",").map((s) => s.trim()).filter(Boolean);

  const token = await fetchAmrodToken();
  const rows = await fetchStockAll(token);
  console.log(`📬 Stock (all): ${rows.length} row(s) raw`);

  const bySku = aggregateStock(rows);
  console.log(`📊 Unique variant SKUs: ${bySku.size}`);

  let ok = 0;
  let miss = 0;

  for (const [fullCode, qty] of bySku) {
    const rec = await findShopifyVariantBySkuCandidates([fullCode]);
    if (!rec?.inventoryItemId) {
      miss++;
      continue;
    }

    const q = Math.max(0, Math.floor(qty));
    for (const loc of locations) {
      try {
        await setInventoryLevel(rec.inventoryItemId, loc, q);
      } catch (e) {
        console.log(
          `::warning title=Stock set failed::${fullCode} loc ${loc} | ${String(e?.message || e)}`
        );
      }
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
