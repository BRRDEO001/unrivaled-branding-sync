#!/usr/bin/env node
/**
 * Fetch the full Amrod catalog, filter by product code or exact productName, sync to Shopify.
 *
 * Uses the same import pipeline as the full sync (runSingleProductImportPipeline).
 * Prefer --code for a single catalog row (matches preview selection). --name syncs all
 * rows with that exact productName.
 */
import { fileURLToPath } from "url";
import path from "path";
import { fetchAmrodToken, fetchAmrodProducts, fetchAmrodPricesAll, fetchStockAll } from "./amrod.js";
import { SHOPIFY_TOKEN } from "./config.js";
import {
  makeLogger,
  writeFailedProductsJson,
  filterProductsByExactName,
  filterProductsByFullCode,
  normalizeProductName,
  syncProductList,
} from "./sync.js";
import { deleteShopifyProduct, findShopifyVariantBySkuCandidates } from "./shopify.js";
import { amrodProductSkuCandidates } from "./incremental-products.js";
import { applyPricesForSkuSet } from "./incremental-prices.js";
import { applyStockForSkuSet } from "./incremental-stock.js";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const out = {
    name: normalizeProductName(process.env.PRODUCT_NAME || ""),
    code: String(process.env.PRODUCT_CODE || "").trim(),
    dryRun: false,
    deleteExisting: true,
    applyPrices: true,
    applyStock: true,
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--name") {
      out.name = normalizeProductName(argv[++i]);
    } else if (a === "--code") {
      out.code = String(argv[++i] || "").trim();
    } else if (a === "--dry-run") {
      out.dryRun = true;
    } else if (a === "--no-delete-existing") {
      out.deleteExisting = false;
    } else if (a === "--no-apply-prices") {
      out.applyPrices = false;
    } else if (a === "--no-apply-stock") {
      out.applyStock = false;
    } else if (a === "--help" || a === "-h") {
      console.log(`sync-by-product-name.js [--code ALT-1101 | --name "Exact Product Name"] [--dry-run] [--no-delete-existing] [--no-apply-prices] [--no-apply-stock]
  --code   Preferred: sync one Amrod catalog row by fullCode/simpleCode.
  --name   Sync all rows with exact productName (case-sensitive, trimmed).
  Env: PRODUCT_CODE, PRODUCT_NAME, CONCURRENCY, SHOPIFY_*, AMROD_*, etc.`);
      process.exit(0);
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }

  if (!out.code && !out.name) {
    throw new Error('Product code or name required — use --code "ALT-1101" or --name "..."');
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
  console.log(`🔎 Found ${products.length} Amrod product(s) to sync:`);
  for (const p of products) {
    const code = p.fullCode || p.simpleCode || "?";
    const name = normalizeProductName(p.productName) || "(no name)";
    const variants = Array.isArray(p.variants) ? p.variants.length : 0;
    console.log(`  • ${code} — ${name} (${variants} variant(s))`);
  }
}

function resolveMatches(allProducts, args) {
  if (args.code) {
    const matches = filterProductsByFullCode(allProducts, args.code);
    if (!matches.length) {
      console.log(`::warning::No product found with code "${args.code}"`);
    }
    return matches;
  }

  const matches = filterProductsByExactName(allProducts, args.name);
  if (!matches.length) {
    console.log(`::warning::No products found with exact name "${args.name}"`);
    console.log("Tip: name match is case-sensitive and trimmed.");
  }
  return matches;
}

async function main() {
  const args = parseArgs(process.argv);
  process.chdir(SCRIPT_DIR);

  if (!SHOPIFY_TOKEN && !args.dryRun) {
    console.error("SHOPIFY_TOKEN missing");
    process.exit(1);
  }

  if (args.code) {
    console.log(`🔍 Searching Amrod catalog for code: "${args.code}"`);
  } else {
    console.log(`🔍 Searching Amrod catalog for exact productName: "${args.name}"`);
  }

  let allProducts;
  try {
    const token = await fetchAmrodToken();
    allProducts = await fetchAmrodProducts(token);
  } catch (e) {
    console.error("Amrod product fetch failed:", e?.message || e);
    process.exit(1);
  }

  console.log(`📬 Amrod catalog: ${allProducts.length} product(s) total`);

  const matches = resolveMatches(allProducts, args);
  if (!matches.length) {
    process.exit(1);
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
    `⚡ CONCURRENCY=${Number(process.env.CONCURRENCY || 4)} IMAGES_MODE=${IMAGES_MODE} applyPrices=${args.applyPrices} applyStock=${args.applyStock}`
  );

  await syncProductList(matches, { logger, stage: "syncByProductName" });

  const failSummary = writeFailedProductsJson(logger.paths.failPath);
  const failCount = failSummary?.count || 0;

  if (failCount) {
    console.log(
      `::error title=Import failures::${failCount} product(s) failed — see ${failSummary.path}`
    );
  }

  if (args.applyPrices) {
    if (failCount) {
      console.log(
        `::warning::Skipping price apply — ${failCount} product(s) failed import (variants not in Shopify yet)`
      );
    } else {
      console.log("💰 Fetching Amrod prices and applying for matched SKUs…");
      try {
        const token = await fetchAmrodToken();
        const priceRows = await fetchAmrodPricesAll(token);
        const skuSet = buildSkuSetFromProducts(matches);
        const result = await applyPricesForSkuSet(priceRows, skuSet);
        console.log(
          `✅ Prices: ${result.ok} applied, ${result.miss} variant not found, ${result.skipBadPrice} bad price row(s), ${result.matchedRows} price row(s) matched SKU set`
        );
        if (result.miss && !result.ok) {
          console.log(
            "::warning::No prices applied — check that variants exist in Shopify with matching SKUs"
          );
        }
      } catch (e) {
        console.error("::warning title=Price apply failed::", e?.message || e);
      }
    }
  }

  if (args.applyStock) {
    if (failCount) {
      console.log(
        `::warning::Skipping stock apply — ${failCount} product(s) failed import (variants not in Shopify yet)`
      );
    } else {
      console.log("📦 Fetching Amrod stock and applying for matched SKUs…");
      try {
        const token = await fetchAmrodToken();
        const stockRows = await fetchStockAll(token);
        const skuSet = buildSkuSetFromProducts(matches);
        const result = await applyStockForSkuSet(stockRows, skuSet);
        if (result.skipped) {
          console.log("::warning::Stock apply skipped — no Shopify location configured");
        } else {
          console.log(
            `✅ Stock: ${result.ok} updated, ${result.miss} variant not found in Shopify, ${result.failed} failed, ${result.skipNoStock} no Amrod stock row, ${result.matchedRows} SKU(s) with stock data`
          );
          if (result.matchedRows && !result.ok) {
            console.log(
              "::warning::No stock levels applied — check variant SKUs match Amrod stock fullCode"
            );
          }
        }
      } catch (e) {
        console.error("::warning title=Stock apply failed::", e?.message || e);
      }
    }
  }

  if (failSummary) {
    console.log(`📝 Failed-products JSON: ${failSummary.path} (${failSummary.count} unique SKU(s))`);
  }

  if (failCount) {
    console.error(`🔥 Sync finished with ${failCount} failure(s)`);
    process.exit(1);
  }

  console.log("🎉 Sync finished successfully");
}

main().catch((err) => {
  console.error("🔥 Sync failed:", err?.message || err);
  console.error(err?.stack);
  process.exit(1);
});
