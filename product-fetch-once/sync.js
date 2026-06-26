// sync.js
import fs from "fs";
import path from "path";
import { AMROD_TEST_LIMIT, LOG_DIR } from "./config.js";
import { fetchAmrodToken, fetchAmrodProducts } from "./amrod.js";
import { logProductFailure } from "./logger.js";
import { runSingleProductImportPipeline } from "./import-single-product.js";

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function nowStamp() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

export function makeLogger() {
  ensureDir(LOG_DIR);
  const stamp = nowStamp();
  const okPath = path.join(LOG_DIR, `sync-ok-${stamp}.jsonl`);
  const failPath = path.join(LOG_DIR, `sync-fail-${stamp}.jsonl`);

  const write = (file, obj) =>
    fs.appendFileSync(file, JSON.stringify(obj) + "\n", "utf8");

  return {
    ok: (o) => write(okPath, { ...o, ts: new Date().toISOString() }),
    fail: (o) => write(failPath, { ...o, ts: new Date().toISOString() }),
    paths: { okPath, failPath },
  };
}

/**
 * Read this run's sync-fail JSONL and write a consolidated JSON array of unique
 * failed SKUs the retry workflow can consume directly.
 *
 * Output: logs/failed-products[-shard-N].json
 *   [{ amrodCode, productName, lastStage, lastMessage, occurrences }]
 */
export function writeFailedProductsJson(failPath, { shardIndex = null } = {}) {
  if (!failPath || !fs.existsSync(failPath)) return null;

  const text = fs.readFileSync(failPath, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) return null;

  const byCode = new Map();
  for (const line of lines) {
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      continue;
    }
    const code = String(entry.amrodCode ?? entry.extra?.amrodCode ?? "").trim();
    if (!code) continue;
    const prev = byCode.get(code);
    if (prev) {
      prev.occurrences++;
      prev.lastStage = entry.step ?? entry.stage ?? prev.lastStage;
      prev.lastMessage = entry.error ?? entry.message ?? prev.lastMessage;
    } else {
      byCode.set(code, {
        amrodCode: code,
        productName: entry.productName ?? null,
        lastStage: entry.step ?? entry.stage ?? null,
        lastMessage: entry.error ?? entry.message ?? null,
        occurrences: 1,
      });
    }
  }

  if (!byCode.size) return null;

  const arr = [...byCode.values()].sort((a, b) => a.amrodCode.localeCompare(b.amrodCode));
  const fileName =
    shardIndex != null && Number.isFinite(Number(shardIndex))
      ? `failed-products-shard-${shardIndex}.json`
      : "failed-products.json";
  const outPath = path.join(LOG_DIR, fileName);

  fs.writeFileSync(
    outPath,
    JSON.stringify(
      { generatedAt: new Date().toISOString(), shardIndex, count: arr.length, products: arr },
      null,
      2
    ) + "\n",
    "utf8"
  );

  return { path: outPath, count: arr.length };
}

export async function runWithConcurrency(items, concurrency, worker) {
  let index = 0;

  const runners = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = index++;
      if (i >= items.length) break;
      await worker(items[i], i);
    }
  });

  await Promise.all(runners);
}

export function normalizeProductName(name) {
  return String(name || "").trim();
}

/** Products whose `productName` exactly matches `searchName` (trimmed, case-sensitive). */
export function filterProductsByExactName(products, searchName) {
  const needle = normalizeProductName(searchName);
  if (!needle || !Array.isArray(products)) return [];
  return products.filter((p) => normalizeProductName(p.productName) === needle);
}

/** Single catalog row by Amrod fullCode or simpleCode. */
export function filterProductsByFullCode(products, productCode) {
  const code = String(productCode || "").trim();
  if (!code || !Array.isArray(products)) return [];
  return products.filter((p) => {
    const full = String(p?.fullCode || "").trim();
    const simple = String(p?.simpleCode || "").trim();
    return full === code || simple === code;
  });
}

export async function syncProductList(products, { logger, stage = "syncAllProducts" } = {}) {
  const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
  let done = 0;
  const total = products.length;
  const start = Date.now();

  await runWithConcurrency(products, CONCURRENCY, async (product) => {
    const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN_CODE";

    try {
      await runSingleProductImportPipeline(product, logger);
    } catch (err) {
      logger.fail({
        amrodCode,
        step: "product",
        error: String(err?.message || err),
      });

      logProductFailure({
        amrod: product,
        stage,
        error: err,
        extra: { amrodCode },
      });
    } finally {
      const finished = ++done;
      if (finished % 100 === 0 || finished === total || total <= 20) {
        const elapsedSec = (Date.now() - start) / 1000;
        const rate = finished / Math.max(elapsedSec, 1);
        const remainingSec = (total - finished) / Math.max(rate, 0.001);
        const eta =
          total > finished ? ` | ETA ~ ${(remainingSec / 60).toFixed(1)}m` : "";
        console.log(`📦 Progress: ${finished}/${total} | ${rate.toFixed(2)} prod/s${eta}`);
      }
    }
  });
}

export const syncAllProducts = async () => {
  const logger = makeLogger();
  console.log(`🧾 Logging to:\n- ${logger.paths.okPath}\n- ${logger.paths.failPath}`);

  let products;
  try {
    const token = await fetchAmrodToken();
    products = await fetchAmrodProducts(token);
  } catch (e) {
    console.error("❌ Amrod fetch failed:", e?.message || e);
    throw e;
  }

  if (AMROD_TEST_LIMIT) {
    products = products.slice(0, AMROD_TEST_LIMIT);
  }

  const SHARD_COUNT = Number(process.env.SHARD_COUNT || 1);
  const SHARD_INDEX = Number(process.env.SHARD_INDEX || 0);

  if (SHARD_COUNT > 1) {
    products = products.filter((_, idx) => idx % SHARD_COUNT === SHARD_INDEX);
  }

  const IMAGES_MODE = String(process.env.IMAGES_MODE || "default+colours");

  console.log(
    `⚡ Speed settings: CONCURRENCY=${Number(process.env.CONCURRENCY || 4)} IMAGES_MODE=${IMAGES_MODE} SHARD_INDEX=${SHARD_INDEX}/${SHARD_COUNT}`
  );

  await syncProductList(products, { logger, stage: "syncAllProducts" });

  try {
    const shardIndex = SHARD_COUNT > 1 ? SHARD_INDEX : null;
    const summary = writeFailedProductsJson(logger.paths.failPath, { shardIndex });
    if (summary) {
      console.log(`📝 Failed-products JSON: ${summary.path} (${summary.count} unique SKU(s))`);
    } else {
      console.log("✅ No failures recorded — failed-products JSON not written");
    }
  } catch (e) {
    console.warn(`::warning::Failed to write failed-products JSON: ${e?.message || e}`);
  }
};
