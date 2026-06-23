#!/usr/bin/env node
/**
 * Fetch the full Amrod catalog, filter by exact productName, and sync matches to Shopify.
 *
 * Uses the same import pipeline as the full sync (runSingleProductImportPipeline).
 * Optionally deletes existing Shopify products by SKU first, and applies variant prices
 * from Amrod /Prices/ after import.
 *
 * Usage:
 *   PRODUCT_NAME="My Product" node sync-by-product-name.js
 *   node sync-by-product-name.js --name "My Product" [--dry-run] [--no-delete-existing] [--no-apply-prices]
 *
 * Env: same as sync.js (CONCURRENCY, IMAGES_MODE, SHOPIFY_*, AMROD_*, etc.)
 */
import { fileURLToPath } from "url";
import path from "path";
import { fetchAmrodToken, fetchAmrodProducts, fetchAmrodPricesAll } from "./amrod.js";
import { SHOPIFY_TOKEN } from "./config.js";
import {
  makeLogger,
  writeFailedProductsJson,
  filterProductsByExactName,
  normalizeProductName,
  syncProductList,
} from "./sync.js";
import { deleteShopifyProduct, findShopifyVariantBySkuCandidates } from "./shopify.js";
import { amrodProductSkuCandidates } from "./incremental-products.js";
import { applyPricesForSkuSet } from "./incremental-prices.js";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const out = {
    name: normalizeProductName(process.env.PRODUCT_NAME || ""),
    dryRun: false,
    deleteExisting: true,
    applyPrices: true,
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--name") {
      out.name = normalizeProductName(argv[++i]);
    } else if (a === "--dry-run") {
      out.dryRun = true;
    } else if (a === "--no-delete-existing") {
      out.deleteExisting = false;
    } else if (a === "--no-apply-prices") {
      out.applyPrices = false;
    } else if (a === "--help" || a === "-h") {
      console.log(`sync-by-product-name.js --name "Exact Product Name" [--dry-run] [--no-delete-existing] [--no-apply-prices]
  Also reads PRODUCT_NAME from the environment.
  Match is exact on trimmed productName (case-sensitive).
  Default: delete existing Shopify product by SKU, then import; apply prices after import.`);
      process.exit(0);
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }

  if (!out.name) {
    throw new Error('Product name required — set PRODUCT_NAME or pass --name "..."');
  }

  return out;
}

function buildSkuSetFromProducts(products) {
  const skuSet = new Set();
  for (const product of products) {
    for (const sku of amrodProductSkuCandidates(product)) {
      skuSet.add(sku);
    }
  }
  return skuSet;
}

async function deleteExistingShopifyProduct(product) {
  const candidates = amrodProductSkuCandidates(product);
  if (!candidates.length) return null;

  const rec = await findShopifyVariantBySkuCandidates(candidates);
  const existingPid = rec?.productId ?? null;
  if (!existingPid) return null;

  await deleteShopifyProduct(existingPid);
  return existingPid;
}

function summarizeMatches(products) {
  console.log(`🔎 Found ${products.length} Amrod product(s) with this exact name:`);
  for (const p of products) {
    const code = p.fullCode || p.simpleCode || "?";
    const variants = Array.isArray(p.variants) ? p.variants.length : 0;
    console.log(`  • ${code} (${variants} variant(s))`);
  }
}

async function main() {
  const args = parseArgs(process.argv);
  process.chdir(SCRIPT_DIR);

  if (!SHOPIFY_TOKEN && !args.dryRun) {
    console.error("SHOPIFY_TOKEN missing");
    process.exit(1);
  }

  console.log(`🔍 Searching Amrod catalog for exact productName: "${args.name}"`);

  let allProducts;
  try {
    const token = await fetchAmrodToken();
    allProducts = await fetchAmrodProducts(token);
  } catch (e) {
    console.error("Amrod product fetch failed:", e?.message || e);
    process.exit(1);
  }

  console.log(`📬 Amrod catalog: ${allProducts.length} product(s) total`);

  const matches = filterProductsByExactName(allProducts, args.name);
  if (!matches.length) {
    console.log(`::warning::No products found with exact name "${args.name}"`);
    console.log("Tip: name match is case-sensitive and trimmed. Copy the name exactly from Amrod.");
    process.exit(0);
  }

  summarizeMatches(matches);

  if (args.dryRun) {
    console.log("--dry-run: no Shopify changes made");
    process.exit(0);
  }

  if (args.deleteExisting) {
    console.log("🗑️ Upsert mode: deleting existing Shopify matches before import…");
    for (const product of matches) {
      const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN_CODE";
      try {
        const removedId = await deleteExistingShopifyProduct(product);
        if (removedId) {
          console.log(`  deleted ${amrodCode} → Shopify product ${removedId}`);
        }
      } catch (e) {
        console.log(
          `::warning title=Pre-delete failed::${amrodCode} | ${String(e?.message || e)}`
        );
      }
    }
  }

  const logger = makeLogger();
  console.log(`🧾 Logging to:\n- ${logger.paths.okPath}\n- ${logger.paths.failPath}`);

  const IMAGES_MODE = String(process.env.IMAGES_MODE || "default+colours");
  console.log(
    `⚡ CONCURRENCY=${Number(process.env.CONCURRENCY || 4)} IMAGES_MODE=${IMAGES_MODE} applyPrices=${args.applyPrices}`
  );

  await syncProductList(matches, { logger, stage: "syncByProductName" });

  if (args.applyPrices) {
    console.log("💰 Fetching Amrod prices and applying for matched SKUs…");
    try {
      const token = await fetchAmrodToken();
      const priceRows = await fetchAmrodPricesAll(token);
      const skuSet = buildSkuSetFromProducts(matches);
      const result = await applyPricesForSkuSet(priceRows, skuSet);
      console.log(
        `✅ Prices: ${result.ok} applied, ${result.miss} variant not found, ${result.skipBadPrice} bad price row(s), ${result.matchedRows} price row(s) matched SKU set`
      );
    } catch (e) {
      console.error("::warning title=Price apply failed::", e?.message || e);
    }
  }

  try {
    const summary = writeFailedProductsJson(logger.paths.failPath);
    if (summary) {
      console.log(`📝 Failed-products JSON: ${summary.path} (${summary.count} unique SKU(s))`);
    }
  } catch (e) {
    console.warn(`::warning::Failed to write failed-products JSON: ${e?.message || e}`);
  }

  console.log("🎉 Sync by product name finished");
}

main().catch((err) => {
  console.error("🔥 Sync by product name failed:", err?.message || err);
  console.error(err?.stack);
  process.exit(1);
});
