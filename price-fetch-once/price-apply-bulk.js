#!/usr/bin/env node
import fs from "fs";
import path from "path";

import {
  AMROD_AUTH_ENDPOINT,
  AMROD_AUTH_DETAILS,
  AMROD_PRICES_ENDPOINT,
} from "./config.js";

import {
  MARKUP_BRACKETS,
  DEFAULT_MARKUP_PCT_ABOVE_MAX,
} from "./pricing-constants.js";

import {
  createBulkVarsStagedUploadTarget,
  uploadJsonlToStagedTarget,
  runBulkMutation,
  pollBulkOperation,
} from "./graphql.js";

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function normalizeSku(s) {
  return String(s || "").trim();
}

// Strips "-0-0" style suffixes (e.g. "-1-0", "-2-3") if present
function stripFullCodeSuffix(fullCode) {
  const s = normalizeSku(fullCode);
  return s.replace(/-\d+-\d+$/, "");
}

function pickSkuCandidates(p) {
  const full = normalizeSku(p.fullCode);
  const simple = normalizeSku(p.simplecode || p.simpleCode);
  const stripped = stripFullCodeSuffix(full);

  // Try exact first, then simple, then stripped
  return [full, simple, stripped].filter(Boolean);
}

function getMarkupPct(base) {
  for (const b of MARKUP_BRACKETS) {
    if (base >= b.min && base <= b.max) return b.pct;
  }
  return DEFAULT_MARKUP_PCT_ABOVE_MAX;
}

function computePrice(base) {
  const pct = getMarkupPct(base);
  const sell = base * (1 + pct / 100);
  // Keep string for GraphQL inputs
  return sell.toFixed(2);
}

/** Same convention as product import matrix: idx % SHARD_COUNT === SHARD_INDEX */
function filterByShard(arr) {
  const n = Math.max(1, Number(process.env.SHARD_COUNT || 1));
  const i = Number(process.env.SHARD_INDEX || 0);
  if (!Array.isArray(arr) || n <= 1) return arr;
  return arr.filter((_, idx) => idx % n === i);
}

function shardSuffix() {
  const n = Math.max(1, Number(process.env.SHARD_COUNT || 1));
  const i = Number(process.env.SHARD_INDEX || 0);
  return n > 1 ? `-shard-${i}` : "";
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

/**
 * Build JSONL lines for bulkOperationRunMutation
 * Each line is the VARIABLES payload for one call(...) execution.
 *
 * Mutation used:
 * productVariantsBulkUpdate(productId:, variants:, allowPartialUpdates:)
 */
function buildJsonlLines({ prices, variantMap, skuNotFoundStream }) {
  const lines = [];
  let skipped = 0;
  let matched = 0;

  let skippedNoSku = 0;
  let skippedNoMap = 0;
  let skippedBadPrice = 0;

  for (const p of prices) {
    const base = Number(p.price);

    const candidates = pickSkuCandidates(p);
    if (!candidates.length) {
      skipped++;
      skippedNoSku++;
      skuNotFoundStream.write(JSON.stringify({ reason: "NO_SKU_FIELDS", item: p }) + "\n");
      continue;
    }

    // Find matching record in SKU map
    let rec = null;
    let matchedSku = null;

    for (const c of candidates) {
      if (variantMap[c]) {
        rec = variantMap[c];
        matchedSku = c;
        break;
      }
    }

    if (!rec?.variantId || !rec?.productId) {
      skipped++;
      skippedNoMap++;
      skuNotFoundStream.write(
        JSON.stringify({
          reason: "SKU_NOT_FOUND_IN_SHOPIFY_MAP",
          candidates,
          simplecode: p.simplecode ?? null,
          fullCode: p.fullCode ?? null,
        }) + "\n"
      );
      continue;
    }

    if (!Number.isFinite(base)) {
      skipped++;
      skippedBadPrice++;
      skuNotFoundStream.write(
        JSON.stringify({
          reason: "BAD_PRICE",
          sku: matchedSku,
          simplecode: p.simplecode ?? null,
          fullCode: p.fullCode ?? null,
          price: p.price,
        }) + "\n"
      );
      continue;
    }

    const finalPrice = computePrice(base);

    // Variables payload for the bulk mutation
    lines.push(
      JSON.stringify({
        productId: rec.productId,
        variants: [{ id: rec.variantId, price: finalPrice }],
        allowPartialUpdates: true,
      })
    );

    matched++;
  }

  return {
    lines,
    matched,
    skipped,
    skippedBreakdown: { skippedNoSku, skippedNoMap, skippedBadPrice },
  };
}

/**
 * Download + parse bulk operation result JSONL and log failures.
 *
 * Shopify bulk result file is JSONL:
 * - { "data": { ... } } per mutation execution
 * - OR { "errors": [ ... ] } if top-level GraphQL errors occurred for that line
 */
async function downloadAndLogBulkFailures({ resultUrl, outErrorsJsonlPath, outSummaryPath }) {
  const res = await fetch(resultUrl);
  if (!res.ok) {
    throw new Error(
      `Failed to download bulk result: HTTP ${res.status} ${await res.text().catch(() => "")}`
    );
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  let totalLines = 0;
  let okCount = 0;
  let failCount = 0;

  const errorStream = fs.createWriteStream(outErrorsJsonlPath, { flags: "w" });

  const writeError = (obj) => {
    errorStream.write(JSON.stringify(obj) + "\n");
  };

  for await (const chunk of res.body) {
    buffer += decoder.decode(chunk, { stream: true });

    let idx;
    while ((idx = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);

      if (!line) continue;
      totalLines++;

      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch {
        failCount++;
        writeError({ type: "INVALID_JSON", raw: line.slice(0, 2000) });
        continue;
      }

      // Top-level GraphQL errors for that line
      if (Array.isArray(parsed?.errors) && parsed.errors.length) {
        failCount++;
        writeError({ type: "TOP_LEVEL_ERRORS", errors: parsed.errors });
        continue;
      }

      const payload = parsed?.data?.productVariantsBulkUpdate;

      if (!payload) {
        failCount++;
        writeError({ type: "MISSING_PAYLOAD", raw: parsed });
        continue;
      }

      const userErrors = payload.userErrors || [];
      if (userErrors.length) {
        failCount++;
        writeError({
          type: "USER_ERRORS",
          userErrors,
          productId: payload?.product?.id || null,
          variantIds: Array.isArray(payload?.productVariants)
            ? payload.productVariants.map((v) => v?.id).filter(Boolean)
            : [],
        });
      } else {
        okCount++;
      }
    }
  }

  // flush remaining tail (if any)
  const tail = buffer.trim();
  if (tail) {
    totalLines++;
    try {
      const parsed = JSON.parse(tail);
      if (Array.isArray(parsed?.errors) && parsed.errors.length) {
        failCount++;
        writeError({ type: "TOP_LEVEL_ERRORS", errors: parsed.errors });
      } else {
        const payload = parsed?.data?.productVariantsBulkUpdate;
        const userErrors = payload?.userErrors || [];
        if (!payload) {
          failCount++;
          writeError({ type: "MISSING_PAYLOAD", raw: parsed });
        } else if (userErrors.length) {
          failCount++;
          writeError({
            type: "USER_ERRORS",
            userErrors,
            productId: payload?.product?.id || null,
            variantIds: Array.isArray(payload?.productVariants)
              ? payload.productVariants.map((v) => v?.id).filter(Boolean)
              : [],
          });
        } else {
          okCount++;
        }
      }
    } catch {
      failCount++;
      writeError({ type: "INVALID_JSON", raw: tail.slice(0, 2000) });
    }
  }

  await new Promise((r) => errorStream.end(r));

  const summary = {
    resultUrl,
    totalLines,
    okCount,
    failCount,
    errorsFile: outErrorsJsonlPath,
  };

  fs.writeFileSync(outSummaryPath, JSON.stringify(summary, null, 2), "utf8");
  return summary;
}

(async () => {
  if (!fs.existsSync("data/variant-map.json")) {
    throw new Error("Missing data/variant-map.json (run prices-build-variant-map.js)");
  }

  ensureDir("data");
  ensureDir("logs");

  const variantMap = JSON.parse(fs.readFileSync("data/variant-map.json", "utf8"));
  const suf = shardSuffix();

  const skuNotFoundPath = path.join("logs", `sku-not-found${suf}.jsonl`);
  const skuNotFoundStream = fs.createWriteStream(skuNotFoundPath, { flags: "w" });

  let prices;

  const fromFile = process.env.AMROD_PRICES_JSON;
  if (fromFile) {
    console.log(`💰 Loading Amrod prices from ${fromFile} ...`);
    const raw = JSON.parse(fs.readFileSync(fromFile, "utf8"));
    prices = Array.isArray(raw) ? raw : raw?.prices ?? raw?.Prices ?? raw?.data;
    if (!Array.isArray(prices)) {
      throw new Error("AMROD_PRICES_JSON must contain an array (or prices[])");
    }
  } else {
    console.log("🔐 Fetching Amrod token...");
    const auth = await fetchJson(AMROD_AUTH_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(AMROD_AUTH_DETAILS),
    });

    const tok = auth?.token ?? auth?.Token;
    if (!tok) throw new Error("Amrod auth: no token");

    console.log("💰 Fetching Amrod prices...");
    const raw = await fetchJson(AMROD_PRICES_ENDPOINT, {
      headers: { Authorization: `Bearer ${tok}` },
    });
    prices = Array.isArray(raw) ? raw : raw?.prices ?? raw?.Prices ?? raw?.data;
    if (!Array.isArray(prices)) {
      throw new Error("Amrod prices response: expected array (or wrapper with prices[])");
    }
  }

  const before = prices.length;
  prices = filterByShard(prices);
  console.log(
    `🔢 Shard filter: SHARD_COUNT=${process.env.SHARD_COUNT || 1} SHARD_INDEX=${process.env.SHARD_INDEX || 0} → ${prices.length}/${before} rows`
  );

  console.log("🧾 Building JSONL updates...");
  const { lines, matched, skipped, skippedBreakdown } = buildJsonlLines({
    prices,
    variantMap,
    skuNotFoundStream,
  });

  await new Promise((r) => skuNotFoundStream.end(r));

  if (!lines.length) {
    console.log(`⚠️ No updates to apply. matched=${matched} skipped=${skipped}`);
    console.log(`🧾 skipped breakdown: ${JSON.stringify(skippedBreakdown)}`);
    console.log(`🧾 not-found log: ${skuNotFoundPath}`);
    process.exit(0);
  }

  const jsonlPath = path.join("data", `variant-price-updates${suf}.jsonl`);
  fs.writeFileSync(jsonlPath, lines.join("\n") + "\n", "utf8");

  console.log(`✅ JSONL created: ${jsonlPath} (updates=${lines.length}, skipped=${skipped})`);
  console.log(`🧾 matched=${matched} skipped breakdown: ${JSON.stringify(skippedBreakdown)}`);
  console.log(`🧾 not-found log: ${skuNotFoundPath}`);

  console.log("📤 Creating staged upload target...");
  const { url, parameters, stagedUploadPath } = await createBulkVarsStagedUploadTarget({
    filename: "variant_price_updates.jsonl",
  });

  console.log("📦 Uploading JSONL to staged target...");
  await uploadJsonlToStagedTarget({ url, parameters, jsonlPath });

  console.log("🚀 Starting bulk mutation...");

  // IMPORTANT: Bulk mutation expects a "mutation call(...)" where variables come from JSONL
  const mutationString = `
    mutation call(
      $productId: ID!,
      $variants: [ProductVariantsBulkInput!]!,
      $allowPartialUpdates: Boolean
    ) {
      productVariantsBulkUpdate(
        productId: $productId,
        variants: $variants,
        allowPartialUpdates: $allowPartialUpdates
      ) {
        product { id }
        productVariants { id }
        userErrors { field message }
      }
    }
  `;

  const op = await runBulkMutation({
    mutationString,
    stagedUploadPath,
    clientIdentifier: "amrod-variant-price-sync",
  });

  console.log(`⏳ Bulk operation started: ${op.id} (status=${op.status})`);

  console.log("🔎 Polling until complete...");
  const finished = await pollBulkOperation({ id: op.id, intervalMs: 4000 });

  console.log(
    `✅ Bulk operation COMPLETED. objectCount=${finished.objectCount} fileSize=${finished.fileSize}`
  );
  console.log(`📄 Results URL: ${finished.url}`);

  // Persist result URL for later inspection
  fs.writeFileSync(
    path.join("logs", `bulk-op-result${suf}.json`),
    JSON.stringify(
      { operationId: finished.id, url: finished.url, completedAt: finished.completedAt },
      null,
      2
    ),
    "utf8"
  );

  // Download results and log failures
  const errorsPath = path.join("logs", `bulk-op-errors${suf}.jsonl`);
  const summaryPath = path.join("logs", `bulk-op-summary${suf}.json`);

  console.log("⬇️ Downloading bulk results + logging failures...");
  const summary = await downloadAndLogBulkFailures({
    resultUrl: finished.url,
    outErrorsJsonlPath: errorsPath,
    outSummaryPath: summaryPath,
  });

  console.log(
    `🧾 Bulk result parsed: ok=${summary.okCount} failed=${summary.failCount} total=${summary.totalLines}`
  );
  console.log(`🧾 Failure log: ${errorsPath}`);
  console.log(`🧾 Summary: ${summaryPath}`);

  console.log("✅ Pricing sync complete (markup only, bulk operation)");
})().catch((e) => {
  console.error("🔥 Failed:", e.message);
  process.exit(1);
});
