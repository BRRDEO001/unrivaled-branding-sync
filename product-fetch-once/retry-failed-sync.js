#!/usr/bin/env node
/**
 * Re-run the full single-product import pipeline for SKUs recorded in failure logs.
 *
 * Reads Amrod failure sources from this folder (after chdir):
 *   - logs/failed-products.json        (consolidated, written at end of sync)
 *   - logs/failed-products-shard-N.json (per-shard variants from CI)
 *   - logs/sync-fail-*.jsonl           (per-day raw fail log)
 * Optionally also logs/failed-products.jsonl (--with-failed-products).
 *
 * Usage (from repo root or product-fetch-once):
 *   node retry-failed-sync.js
 *   node retry-failed-sync.js --with-failed-products
 *   node retry-failed-sync.js --max-rounds 5
 *
 * Multi-round: round 1 reads all logs/sync-fail-*.jsonl (optional failed-products); later rounds
 * only re-read the previous round's sync-fail file so fixed SKUs are not retried forever.
 *
 * Env matches sync: CONCURRENCY, SHARD_COUNT, SHARD_INDEX, etc.
 * CHDIR: defaults to the directory containing this script so logs/ paths match CI.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchAmrodToken, fetchAmrodProducts } from "./amrod.js";
import { SHOPIFY_TOKEN } from "./config.js";
import { logProductFailure } from "./logger.js";
import { runSingleProductImportPipeline } from "./import-single-product.js";
import { makeLogger, runWithConcurrency } from "./sync.js";
import {
  deleteShopifyProduct,
  findShopifyVariantBySkuCandidates,
} from "./shopify.js";
import { amrodProductSkuCandidates } from "./incremental-products.js";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const out = {
    withFailedProducts: false,
    maxRounds: 1,
    dryRun: false,
    deleteExisting: true,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--with-failed-products") out.withFailedProducts = true;
    else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--no-delete-existing") out.deleteExisting = false;
    else if (a === "--max-rounds") {
      const n = Number(argv[++i]);
      if (!Number.isFinite(n) || n < 1) {
        throw new Error("--max-rounds expects a positive integer");
      }
      out.maxRounds = Math.floor(n);
    } else if (a === "--help" || a === "-h") {
      console.log(`retry-failed-sync.js [--with-failed-products] [--max-rounds N] [--dry-run] [--no-delete-existing]
  Default sources: logs/failed-products*.json + logs/sync-fail-*.jsonl (unique amrodCode).
  --with-failed-products also merges logs/failed-products.jsonl (cumulative; may retry stale SKUs).
  --max-rounds N re-reads failure logs and retries until no codes remain or N rounds (default 1).
  --dry-run prints counts only (does not call Amrod or Shopify).
  --no-delete-existing skips the upsert step (default deletes the existing Shopify product by SKU before re-import).`);
      process.exit(0);
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }
  return out;
}

function jsonlLines(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  return text.split(/\r?\n/).filter((line) => line.trim());
}

function mergeCodesFromJsonl(filePath, into) {
  let lines = 0;
  for (const line of jsonlLines(filePath)) {
    lines++;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      console.warn(`skip invalid JSON line in ${path.basename(filePath)} (${lines})`);
      continue;
    }
    const code = obj.amrodCode ?? obj.extra?.amrodCode;
    if (code != null && String(code).trim()) into.add(String(code).trim());
  }
}

function mergeCodesFromJsonFile(filePath, into) {
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (e) {
    console.warn(`skip invalid JSON file ${path.basename(filePath)}: ${e?.message || e}`);
    return;
  }

  const list = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.products)
    ? parsed.products
    : null;

  if (!list) {
    console.warn(`skip ${path.basename(filePath)}: expected an array or { products: [] }`);
    return;
  }

  for (const item of list) {
    const code =
      typeof item === "string"
        ? item
        : item?.amrodCode ?? item?.fullCode ?? item?.simpleCode ?? null;
    if (code != null && String(code).trim()) into.add(String(code).trim());
  }
}

function collectCodesFromLogs(logDir, includeFailedProducts) {
  const codes = new Set();
  if (!fs.existsSync(logDir)) {
    console.warn(`Log dir missing: ${logDir}`);
    return codes;
  }

  const entries = fs.readdirSync(logDir);

  // Prefer the consolidated failed-products[-shard-N].json files when present
  const jsonNames = entries.filter(
    (n) => n === "failed-products.json" || /^failed-products-shard-\d+\.json$/.test(n)
  );
  for (const n of jsonNames) {
    mergeCodesFromJsonFile(path.join(logDir, n), codes);
  }

  // Always fall back / merge with the daily JSONL fail logs
  const failNames = entries.filter((n) => n.startsWith("sync-fail-") && n.endsWith(".jsonl"));
  for (const n of failNames) {
    mergeCodesFromJsonl(path.join(logDir, n), codes);
  }

  if (includeFailedProducts) {
    const fp = path.join(logDir, "failed-products.jsonl");
    if (fs.existsSync(fp)) mergeCodesFromJsonl(fp, codes);
  }

  return codes;
}

/** Codes from one sync-fail JSONL (used for rounds 2+ so we do not re-queue old successes). */
function collectCodesFromFailFile(filePath) {
  const codes = new Set();
  if (!filePath || !fs.existsSync(filePath)) return codes;
  mergeCodesFromJsonl(filePath, codes);
  return codes;
}

function indexProductsBySku(products) {
  const byFull = new Map();
  const bySimple = new Map();
  for (const p of products) {
    const f = p.fullCode != null ? String(p.fullCode).trim() : "";
    const s = p.simpleCode != null ? String(p.simpleCode).trim() : "";
    if (f) byFull.set(f, p);
    if (s) bySimple.set(s, p);
  }
  return { byFull, bySimple };
}

function resolveProductsForCodes(codes, products) {
  const { byFull, bySimple } = indexProductsBySku(products);
  const resolved = [];
  const missing = [];
  for (const code of codes) {
    const p = byFull.get(code) || bySimple.get(code);
    if (p) resolved.push(p);
    else missing.push(code);
  }
  return { resolved, missing };
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

async function runRetryRound({ logger, productsToRetry, concurrency, deleteExisting }) {
  let done = 0;
  const total = productsToRetry.length;
  const start = Date.now();

  await runWithConcurrency(productsToRetry, concurrency, async (product) => {
    const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN_CODE";

    try {
      if (deleteExisting) {
        try {
          const removedId = await deleteExistingShopifyProduct(product);
          if (removedId) {
            console.log(`🗑️ retry ${amrodCode}: deleted existing Shopify product ${removedId}`);
          }
        } catch (e) {
          // Don't fail the whole retry just because lookup/delete had a transient error;
          // log and continue — re-import may still succeed (or duplicate, which we will
          // still surface in the next round's failures).
          console.log(
            `::warning title=Pre-delete failed::${amrodCode} | ${String(e?.message || e)}`
          );
          logger.fail({
            amrodCode,
            step: "retry-pre-delete",
            error: String(e?.message || e),
          });
        }
      }

      await runSingleProductImportPipeline(product, logger);
    } catch (err) {
      logger.fail({
        amrodCode,
        step: "product",
        error: String(err?.message || err),
      });

      logProductFailure({
        amrod: product,
        stage: "retryFailedSync",
        error: err,
        extra: { amrodCode },
      });
    } finally {
      const finished = ++done;
      if (finished % 50 === 0 || finished === total) {
        const elapsedSec = (Date.now() - start) / 1000;
        const rate = finished / Math.max(elapsedSec, 1);
        console.log(`🔁 Retry progress: ${finished}/${total} (~${rate.toFixed(2)} prod/s)`);
      }
    }
  });
}

async function main() {
  const args = parseArgs(process.argv);

  process.chdir(SCRIPT_DIR);

  if (!SHOPIFY_TOKEN) {
    console.error("SHOPIFY_TOKEN missing");
    process.exit(1);
  }

  const logDir = path.resolve(SCRIPT_DIR, process.env.LOG_DIR || "logs");

  let round = 0;
  let previousRoundFailPath = null;

  while (round < args.maxRounds) {
    round++;
    const codes = previousRoundFailPath
      ? collectCodesFromFailFile(previousRoundFailPath)
      : collectCodesFromLogs(logDir, args.withFailedProducts);

    if (!codes.size) {
      console.log(
        round === 1
          ? "No failed SKUs found in log sources — nothing to retry."
          : "No failures left after retry — done."
      );
      break;
    }

    console.log(
      `Round ${round}/${args.maxRounds}: ${codes.size} unique SKU(s) (${previousRoundFailPath ? `from ${path.basename(previousRoundFailPath)}` : args.withFailedProducts ? "failed-products*.json + sync-fail-* + failed-products.jsonl" : "failed-products*.json + sync-fail-*"})`
    );

    if (args.dryRun) {
      console.log(`--dry-run: would attempt ${codes.size} SKU(s) (Amrod catalog not fetched)`);
      const sample = [...codes].slice(0, 20);
      if (sample.length) console.log(`  sample: ${sample.join(", ")}${codes.size > 20 ? " …" : ""}`);
      break;
    }

    let products;
    try {
      const token = await fetchAmrodToken();
      products = await fetchAmrodProducts(token);
    } catch (e) {
      console.error("Amrod fetch failed:", e?.message || e);
      process.exit(1);
    }

    let { resolved: productsToRetry, missing } = resolveProductsForCodes(codes, products);

    const SHARD_COUNT = Number(process.env.SHARD_COUNT || 1);
    const SHARD_INDEX = Number(process.env.SHARD_INDEX || 0);
    if (SHARD_COUNT > 1) {
      productsToRetry = productsToRetry.filter((_, idx) => idx % SHARD_COUNT === SHARD_INDEX);
    }

    if (missing.length) {
      console.warn(
        `::warning::${missing.length} SKU(s) not in current Amrod catalog (skipped): ${missing.slice(0, 20).join(", ")}${missing.length > 20 ? " …" : ""}`
      );
    }

    if (!productsToRetry.length) {
      console.log("No matching products to retry after catalog lookup / sharding.");
      break;
    }


    const logger = makeLogger();
    previousRoundFailPath = logger.paths.failPath;
    console.log(`🧾 This round logs:\n- ${logger.paths.okPath}\n- ${logger.paths.failPath}`);

    const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
    console.log(
      `Mode: ${args.deleteExisting ? "upsert (delete-by-SKU then re-import)" : "create-only (no pre-delete)"} | concurrency=${CONCURRENCY}`
    );
    await runRetryRound({
      logger,
      productsToRetry,
      concurrency: CONCURRENCY,
      deleteExisting: args.deleteExisting,
    });

    console.log(`✅ Retry round ${round} finished (${productsToRetry.length} attempted)`);
  }

  console.log("🎉 Retry script finished");
}

main().catch((err) => {
  console.error("🔥 Retry failed:", err?.message || err);
  console.error(err?.stack);
  process.exit(1);
});
