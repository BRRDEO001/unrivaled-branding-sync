#!/usr/bin/env node
/**
 * One Amrod /Prices/ fetch → data/amrod-prices.json (for sharded price-apply-bulk runs).
 */
import fs from "fs";
import {
  AMROD_AUTH_ENDPOINT,
  AMROD_AUTH_DETAILS,
  AMROD_PRICES_ENDPOINT,
} from "./config.js";

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

(async () => {
  ensureDir("data");

  console.log("🔐 Fetching Amrod token...");
  const auth = await fetchJson(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(AMROD_AUTH_DETAILS),
  });

  const tok = auth?.token ?? auth?.Token;
  if (!tok) throw new Error("Amrod auth: no token");

  console.log("💰 Fetching Amrod prices (/Prices/)...");
  const raw = await fetchJson(AMROD_PRICES_ENDPOINT, {
    headers: { Authorization: `Bearer ${tok}` },
  });

  const prices = Array.isArray(raw) ? raw : raw?.prices ?? raw?.Prices ?? raw?.data;
  if (!Array.isArray(prices)) {
    throw new Error("Amrod prices response: expected array (or wrapper with prices[])");
  }

  const out = "data/amrod-prices.json";
  fs.writeFileSync(out, JSON.stringify(prices), "utf8");
  console.log(`✅ Wrote ${out} (${prices.length} rows)`);
})().catch((e) => {
  console.error("🔥 Failed:", e?.message || e);
  process.exit(1);
});
