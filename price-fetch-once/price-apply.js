#!/usr/bin/env node
import fs from "fs";
import {
  SHOPIFY_STORE,
  SHOPIFY_TOKEN,
  SHOPIFY_API_VERSION,
  AMROD_AUTH_ENDPOINT,
  AMROD_AUTH_DETAILS,
  AMROD_PRICES_ENDPOINT,
} from "./config.js";

import {
  MARKUP_BRACKETS,
  DEFAULT_MARKUP_PCT_ABOVE_MAX,
} from "./pricing-constants.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getMarkupPct(base) {
  for (const b of MARKUP_BRACKETS) {
    if (base >= b.min && base <= b.max) return b.pct;
  }
  return DEFAULT_MARKUP_PCT_ABOVE_MAX;
}

function computePrice(base) {
  const pct = getMarkupPct(base);
  const sell = base * (1 + pct / 100);
  return Number(sell.toFixed(2));
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

(async () => {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) {
    throw new Error("Missing Shopify credentials");
  }

  if (!fs.existsSync("data/variant-map.json")) {
    throw new Error("Missing data/variant-map.json (run prices-build-variant-map.js)");
  }

  const variantMap = JSON.parse(
    fs.readFileSync("data/variant-map.json", "utf8")
  );

  console.log("🔐 Fetching Amrod token...");
  const auth = await fetchJson(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(AMROD_AUTH_DETAILS),
  });

  console.log("💰 Fetching Amrod prices...");
  const prices = await fetchJson(AMROD_PRICES_ENDPOINT, {
    headers: { Authorization: `Bearer ${auth.token}` },
  });

  for (const p of prices) {
    const sku = String(p.fullCode || "").trim();
    const base = Number(p.price);
    const variantId = variantMap[sku];

    if (!variantId || !Number.isFinite(base)) continue;

    const finalPrice = computePrice(base);

    await fetch(
      `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/variants/${variantId}.json`,
      {
        method: "PUT",
        headers: {
          "X-Shopify-Access-Token": SHOPIFY_TOKEN,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          variant: { id: variantId, price: finalPrice.toFixed(2) },
        }),
      }
    );

    console.log(`💲 ${sku} → ${finalPrice}`);
    await sleep(300); // keep Shopify happy
  }

  console.log("✅ Pricing sync complete (markup only)");
})().catch((e) => {
  console.error("🔥 Failed:", e.message);
  process.exit(1);
});
