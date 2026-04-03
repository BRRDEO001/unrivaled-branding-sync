#!/usr/bin/env node
import { fileURLToPath } from "url";
import { fetchAmrodToken, fetchUpdatedPrices } from "./amrod.js";
import { updateShopifyVariant, findShopifyVariantBySkuCandidates } from "./shopify.js";
import {
  MARKUP_BRACKETS,
  DEFAULT_MARKUP_PCT_ABOVE_MAX,
} from "../price-fetch-once/pricing-constants.js";

function pickSkuCandidates(p) {
  const normalizeSku = (s) => String(s || "").trim();
  const full = normalizeSku(p.fullCode);
  const simple = normalizeSku(p.simplecode || p.simpleCode);
  const stripped = full.replace(/-\d+-\d+$/, "");
  return [...new Set([full, simple, stripped].filter(Boolean))];
}

function getMarkupPct(base) {
  for (const b of MARKUP_BRACKETS) {
    if (base >= b.min && base <= b.max) return b.pct;
  }
  return DEFAULT_MARKUP_PCT_ABOVE_MAX;
}

function computeSellPrice(base) {
  const pct = getMarkupPct(base);
  const sell = base * (1 + pct / 100);
  return sell.toFixed(2);
}

export async function runIncrementalPricesSync() {
  const token = await fetchAmrodToken();
  const rows = await fetchUpdatedPrices(token);
  console.log(`📬 Prices GetUpdated: ${rows.length} row(s)`);
  if (!rows.length) {
    console.log("✅ No price updates from Amrod");
    return;
  }

  let ok = 0;
  let miss = 0;
  const delayMs = Number(process.env.SHOPIFY_PRICE_DELAY_MS || 150);

  for (const p of rows) {
    const base = Number(p.price);
    if (!Number.isFinite(base)) continue;

    const sell = computeSellPrice(base);
    const cands = pickSkuCandidates(p);
    const rec = await findShopifyVariantBySkuCandidates(cands);
    if (!rec?.variantId) {
      miss++;
      continue;
    }

    await updateShopifyVariant(rec.variantId, { price: sell });
    ok++;
    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  console.log(`✅ Price updates applied: ${ok} ok, ${miss} variant not found in Shopify`);
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  runIncrementalPricesSync().catch((e) => {
    console.error("🔥 Incremental prices failed:", e?.message || e);
    process.exit(1);
  });
}
