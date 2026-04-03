#!/usr/bin/env node
/**
 * One Amrod GET /Stock/ → data/amrod-stock.json (for sharded stock-apply runs).
 */
import fs from "fs";
import { fetchAmrodToken, fetchStockAll } from "./amrod.js";

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

(async () => {
  ensureDir("data");
  console.log("🔐 Amrod token + full stock fetch...");
  const token = await fetchAmrodToken();
  const rows = await fetchStockAll(token);
  if (!Array.isArray(rows)) {
    throw new Error("Stock response was not an array");
  }
  const out = "data/amrod-stock.json";
  fs.writeFileSync(out, JSON.stringify(rows), "utf8");
  console.log(`✅ Wrote ${out} (${rows.length} raw rows)`);
})().catch((e) => {
  console.error("🔥 Failed:", e?.message || e);
  process.exit(1);
});
