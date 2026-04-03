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

async function runWithConcurrency(items, concurrency, worker) {
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

  const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
  const IMAGES_MODE = String(process.env.IMAGES_MODE || "default+colours");

  console.log(
    `⚡ Speed settings: CONCURRENCY=${CONCURRENCY} IMAGES_MODE=${IMAGES_MODE} SHARD_INDEX=${SHARD_INDEX}/${SHARD_COUNT}`
  );

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
        stage: "syncAllProducts",
        error: err,
        extra: { amrodCode },
      });
    } finally {
      const finished = ++done;
      if (finished % 100 === 0 || finished === total) {
        const elapsedSec = (Date.now() - start) / 1000;
        const rate = finished / Math.max(elapsedSec, 1);
        const remainingSec = (total - finished) / Math.max(rate, 0.001);
        console.log(
          `📦 Progress: ${finished}/${total} | ${rate.toFixed(
            2
          )} prod/s | ETA ~ ${(remainingSec / 3600).toFixed(2)}h`
        );
      }
    }
  });
};
