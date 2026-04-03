#!/usr/bin/env node
/**
 * Amrod GetUpdatedProductsAndBranding → Shopify
 * actionType 0/1: replace (delete if exists, full import)
 * actionType 2: removed — delete in Shopify; if payload still includes full product data, re-import after delete
 */
import { fileURLToPath } from "url";
import { fetchAmrodToken, fetchUpdatedProductsAndBranding } from "./amrod.js";
import {
  deleteShopifyProduct,
  findShopifyVariantBySkuCandidates,
} from "./shopify.js";
import { logProductFailure } from "./logger.js";
import { makeLogger } from "./sync.js";
import { runSingleProductImportPipeline } from "./import-single-product.js";

function amrodActionType(row) {
  const v = row?.actionType ?? row?.ActionType;
  return v != null ? Number(v) : -1;
}

export function amrodProductSkuCandidates(product) {
  const out = [];
  const add = (s) => {
    const v = String(s || "").trim();
    if (v) out.push(v);
  };
  add(product?.fullCode);
  add(product?.simpleCode);
  for (const v of product?.variants || []) {
    add(v?.fullCode);
    add(v?.simpleCode);
  }
  return [...new Set(out)];
}

function hasFullProductForImport(product) {
  return !!(
    product &&
    typeof product === "object" &&
    (product.productName || product.description) &&
    Array.isArray(product.variants) &&
    product.variants.length > 0
  );
}

async function resolveShopifyProductId(product) {
  const rec = await findShopifyVariantBySkuCandidates(amrodProductSkuCandidates(product));
  return rec?.productId ?? null;
}

export async function runIncrementalProductsSync() {
  const logger = makeLogger();
  console.log(
    `🧾 Incremental products logging:\n- ${logger.paths.okPath}\n- ${logger.paths.failPath}`
  );

  const token = await fetchAmrodToken();
  const rows = await fetchUpdatedProductsAndBranding(token);
  console.log(`📬 GetUpdatedProductsAndBranding: ${rows.length} row(s)`);

  if (!rows.length) {
    console.log("✅ No product updates from Amrod");
    return;
  }

  const concurrency = Math.max(1, Number(process.env.INCREMENTAL_CONCURRENCY || 6));
  let i = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= rows.length) break;
      const product = rows[idx];
      const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN";
      const at = amrodActionType(product);

      try {
        const existingPid = await resolveShopifyProductId(product);

        if (at === 2) {
          if (existingPid) {
            console.log(`🗑️ actionType=2 remove ${amrodCode} → delete Shopify product ${existingPid}`);
            await deleteShopifyProduct(existingPid);
          } else {
            console.log(`::notice::actionType=2 ${amrodCode}: no Shopify match (already gone)`);
          }
          if (hasFullProductForImport(product)) {
            console.log(`🔁 actionType=2 ${amrodCode}: re-importing from payload after delete`);
            await runSingleProductImportPipeline(product, logger);
          }
          continue;
        }

        if (at === 0 || at === 1) {
          if (!hasFullProductForImport(product)) {
            throw new Error(
              `actionType ${at} but payload missing full product/variants — cannot import`
            );
          }
          if (existingPid) {
            console.log(`♻️ actionType=${at} ${amrodCode} → delete ${existingPid} then import`);
            await deleteShopifyProduct(existingPid);
          }
          await runSingleProductImportPipeline(product, logger);
          continue;
        }

        console.log(
          `::warning::Unknown actionType ${at} for ${amrodCode} — skipping (use full sync if needed)`
        );
      } catch (err) {
        logger.fail({
          amrodCode,
          step: "incremental-product",
          actionType: at,
          error: String(err?.message || err),
        });
        logProductFailure({
          amrod: product,
          stage: "incrementalProducts",
          error: err,
          extra: { amrodCode, actionType: at },
        });
      }
    }
  });

  await Promise.all(workers);
  console.log("✅ Incremental product sync pass complete");
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  runIncrementalProductsSync().catch((e) => {
    console.error("🔥 Incremental products failed:", e?.message || e);
    process.exit(1);
  });
}
