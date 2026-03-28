#!/usr/bin/env node
import { syncAllProducts } from "./sync.js";
import { SHOPIFY_TOKEN } from "./config.js";

(async () => {
  try {
    if (!SHOPIFY_TOKEN) throw new Error("SHOPIFY_TOKEN missing");
    await syncAllProducts();
    console.log("🎉 Import complete");
  } catch (err) {
    console.error("🔥 Import failed:");
    console.error(err);
    console.error(err?.message);
    console.error(err?.stack);
    process.exit(1);
  }
})();
