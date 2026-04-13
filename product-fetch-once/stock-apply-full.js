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
  setInventoryItemTracked,
  setInventoryLevel,
  getPrimaryLocationId,
} from "./shopify.js";
import { REQUEST_DELAY_MS } from "./config.js";

const FAILED_ATTEMPT_RETRY_DELAY_MS = 300;

function isTrackingDisabledError(error) {
  return String(error?.message || error)
    .toLowerCase()
    .includes("inventory item does not have inventory tracking enabled");
}

/** fullCode → total available quantity */
function aggregateStock(rows) {
  const m = new Map();
  for (const item of rows) {
    const code = String(
      item.fullCode ??
        item.FullCode ??
        item.full_code ??
        item.code ??
        item.Code ??
        item.sku ??
        item.SKU ??
        ""
    ).trim();
    if (!code) continue;
    const n = Number(
      item.stock ?? item.Stock ?? item.quantity ?? item.Quantity ?? item.available ?? item.Available ?? 0
    );
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

async function applyStockWithRecovery(inventoryItemId, locationId, quantity) {
  try {
    await setInventoryLevel(inventoryItemId, locationId, quantity);
    return { ok: true, trackingEnabled: false };
  } catch (e) {
    if (!isTrackingDisabledError(e)) throw e;
    await setInventoryItemTracked(inventoryItemId, true);
    await setInventoryLevel(inventoryItemId, locationId, quantity);
    return { ok: true, trackingEnabled: true };
  }
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
  if (Array.isArray(rows) && rows.length > 0 && bySku.size === 0) {
    const sample = rows[0];
    const keys = sample && typeof sample === "object" ? Object.keys(sample).sort().join(", ") : "";
    console.log(
      `::warning title=Stock shape mismatch::${rows.length} raw row(s) but 0 SKUs after parse — check Amrod keys (expected fullCode + stock). Sample keys: ${keys || "n/a"}`
    );
  }

  let entries = Array.from(bySku.entries()).sort((a, b) =>
    String(a[0]).localeCompare(String(b[0]), "en")
  );
  const before = entries.length;
  entries = filterEntriesByShard(entries);
  const concurrency = Math.max(1, Number(process.env.STOCK_APPLY_CONCURRENCY || 4));
  console.log(
    `🔢 Stock shard: SHARD_COUNT=${process.env.SHARD_COUNT || 1} SHARD_INDEX=${process.env.SHARD_INDEX || 0} → ${entries.length}/${before} SKUs | CONCURRENCY=${concurrency}`
  );

  let ok = 0;
  let miss = 0;
  let trackingEnabled = 0;
  let failed = 0;
  let index = 0;
  let processed = 0;
  const startedAt = Date.now();

  async function processOne([fullCode, qty]) {
    const rec = await findShopifyVariantBySkuCandidates([fullCode]);
    if (!rec?.inventoryItemId) {
      miss++;
      return;
    }

    const q = Math.max(0, Math.floor(qty));
    let lastErr = null;

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const result = await applyStockWithRecovery(rec.inventoryItemId, locationId, q);
        ok++;
        if (result.trackingEnabled) {
          trackingEnabled++;
          console.log(`::notice title=Inventory tracking enabled::${fullCode} loc ${locationId}`);
        }
        lastErr = null;
        break;
      } catch (e) {
        lastErr = e;
        if (attempt < 2) {
          console.log(
            `::notice title=Retrying stock apply::${fullCode} loc ${locationId} | waiting ${FAILED_ATTEMPT_RETRY_DELAY_MS}ms after ${String(
              e?.message || e
            )}`
          );
          await new Promise((r) => setTimeout(r, FAILED_ATTEMPT_RETRY_DELAY_MS));
          continue;
        }
      }
    }

    if (lastErr) {
      failed++;
      const title = isTrackingDisabledError(lastErr)
        ? "Stock set failed after enabling tracking"
        : "Stock set failed";
      console.log(
        `::warning title=${title}::${fullCode} loc ${locationId} | ${String(
          lastErr?.message || lastErr
        )}`
      );
    }
    try {
      // no-op: keeps structure aligned with progress + pacing in finally
    } finally {
      processed++;
      if (processed % 250 === 0 || processed === entries.length) {
        const elapsedSec = Math.max(1, (Date.now() - startedAt) / 1000);
        const rate = processed / elapsedSec;
        console.log(
          `📦 Stock progress: ${processed}/${entries.length} | ${rate.toFixed(2)} sku/s | ok=${ok} miss=${miss} tracking=${trackingEnabled} failed=${failed}`
        );
      }
      await new Promise((r) => setTimeout(r, REQUEST_DELAY_MS));
    }
  }

  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = index++;
      if (i >= entries.length) break;
      await processOne(entries[i]);
    }
  });

  await Promise.all(workers);

  console.log(
    `✅ Stock levels updated: ${ok} SKUs, ${miss} not found in Shopify, ${trackingEnabled} tracking-enabled, ${failed} failed`
  );
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  runStockFullSyncToShopify().catch((e) => {
    console.error("🔥 Stock apply failed:", e?.message || e);
    process.exit(1);
  });
}
