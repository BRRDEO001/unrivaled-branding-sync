#!/usr/bin/env node
/**
 * Archive ACTIVE Shopify products that bulk Amrod pricing could never target:
 * every variant with a non-empty SKU is absent from the SKU keys produced by
 * intersecting Amrod /Prices/ rows with variant-map.json — same candidate rules
 * as price-fetch-once/price-apply-bulk.js.
 *
 * This matches “skippedNoMap” failures (SKU mismatch / no map entry), not “Shopify
 * shows $0” (imports rarely leave variants at zero).
 *
 * Env:
 *   VARIANT_MAP_JSON     — default ../price-fetch-once/data/variant-map.json (from this script dir)
 *   AMROD_PRICES_JSON    — default ../price-fetch-once/data/amrod-prices.json
 *   ARCHIVE_DRY_RUN      — true: log only
 *   ARCHIVE_MAX_PRODUCTS — stop after N successful archives (0 = unlimited)
 *   ARCHIVE_DELAY_MS     — pause after each archive
 *   PRODUCTS_PAGE_SIZE   — GraphQL products page size
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { shopifyGraphql } from "./shopify.js";
import { REQUEST_DELAY_MS } from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

function resolvePath(relOrAbs, label) {
  if (!relOrAbs) throw new Error(`${label} path is required`);
  return path.isAbsolute(relOrAbs) ? relOrAbs : path.join(SCRIPT_DIR, relOrAbs);
}

function parseJsonFile(filePath, label) {
  const text = fs.readFileSync(filePath, "utf8").trim();
  if (text.startsWith("#!")) {
    throw new Error(
      `${label}: ${filePath} looks like a script, not JSON — regenerate the map/prices files`
    );
  }
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`${label}: invalid JSON (${filePath}): ${e.message}`);
  }
}

function normalizeSku(s) {
  return String(s || "").trim();
}

/** Keep in sync with price-fetch-once/price-apply-bulk.js pickSkuCandidates */
function pickSkuCandidates(p) {
  const full = normalizeSku(
    p.fullCode ?? p.FullCode ?? p.full_code ?? p.SKU ?? p.sku
  );
  const simple = normalizeSku(
    p.simplecode ?? p.simpleCode ?? p.SimpleCode ?? p.simple_code
  );
  const productFull = normalizeSku(
    p.productFullCode ??
      p.ProductFullCode ??
      p.parentFullCode ??
      p.ParentFullCode ??
      p.masterFullCode ??
      p.MasterFullCode ??
      p.productFull ??
      p.ProductFull ??
      (typeof p.product === "object" && p.product
        ? p.product.fullCode ?? p.product.FullCode
        : null)
  );
  return [...new Set([full, simple, productFull].filter(Boolean))];
}

function loadPricesArray(filePath) {
  const raw = parseJsonFile(filePath, "Amrod prices");
  const prices = Array.isArray(raw) ? raw : raw?.prices ?? raw?.Prices ?? raw?.data;
  if (!Array.isArray(prices)) {
    throw new Error(`Amrod prices JSON must be an array or { prices: [] } (${filePath})`);
  }
  return prices;
}

function variantMapRecordOk(rec) {
  return !!(rec && rec.variantId && rec.productId);
}

/** Every candidate string on Amrod rows that hits variant-map (same as bulk matcher). */
function buildMatchedSkuKeys(variantMap, prices) {
  const matchedSkuKeys = new Set();
  for (const p of prices) {
    for (const c of pickSkuCandidates(p)) {
      if (variantMapRecordOk(variantMap[c])) matchedSkuKeys.add(c);
    }
  }
  return matchedSkuKeys;
}

const LIST_QUERY = `
  query ArchiveMismatchScan($cursor: String, $pageSize: Int!) {
    products(first: $pageSize, after: $cursor, query: "status:active") {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        title
        status
        variants(first: 250) {
          nodes {
            sku
          }
        }
      }
    }
  }
`;

const ARCHIVE_MUTATION = `
  mutation ArchiveProduct($input: ProductInput!) {
    productUpdate(input: $input) {
      product {
        id
        status
        title
      }
      userErrors {
        field
        message
      }
    }
  }
`;

async function main() {
  const dryRun =
    ["1", "true", "yes"].includes(String(process.env.ARCHIVE_DRY_RUN || "").toLowerCase());
  const maxArchive = Math.max(0, Number(process.env.ARCHIVE_MAX_PRODUCTS || 0));
  const delayMs =
    Number(process.env.ARCHIVE_DELAY_MS ?? REQUEST_DELAY_MS ?? 400) || 400;
  const pageSize = Math.min(
    250,
    Math.max(1, Number(process.env.PRODUCTS_PAGE_SIZE || 50) || 50)
  );

  const variantMapPath =
    process.env.VARIANT_MAP_JSON ||
    path.join(SCRIPT_DIR, "..", "price-fetch-once", "data", "variant-map.json");
  const amrodPricesPath =
    process.env.AMROD_PRICES_JSON ||
    path.join(SCRIPT_DIR, "..", "price-fetch-once", "data", "amrod-prices.json");

  const variantMap = parseJsonFile(resolvePath(variantMapPath, "VARIANT_MAP_JSON"), "variant-map");
  const prices = loadPricesArray(resolvePath(amrodPricesPath, "AMROD_PRICES_JSON"));

  const matchedSkuKeys = buildMatchedSkuKeys(variantMap, prices);

  console.log(
    `📎 Amrod price rows=${prices.length} variant-map keys=${Object.keys(variantMap).length} ` +
      `SKU-keys reachable by bulk matcher=${matchedSkuKeys.size}`
  );

  let cursor = null;
  let scanned = 0;
  let skippedHasAmrodMatch = 0;
  let skippedNoVariantSku = 0;
  let candidates = 0;
  let archivedOk = 0;
  let archiveFailed = 0;

  const logDir = path.join(SCRIPT_DIR, "logs");
  fs.mkdirSync(logDir, { recursive: true });
  const logPath = path.join(logDir, `archive-unpriced-${Date.now()}.jsonl`);
  const logStream = fs.createWriteStream(logPath, { flags: "a" });

  const stopAfterLimit = () => maxArchive > 0 && archivedOk >= maxArchive;

  try {
    do {
      const data = await shopifyGraphql(LIST_QUERY, { cursor, pageSize });
      const conn = data?.products;
      const nodes = conn?.nodes || [];

      for (const p of nodes) {
        scanned++;

        if (String(p.status || "").toUpperCase() !== "ACTIVE") continue;

        const variantNodes = p.variants?.nodes || [];
        const variantSkus = variantNodes
          .map((v) => normalizeSku(v.sku))
          .filter(Boolean);

        if (!variantSkus.length) {
          skippedNoVariantSku++;
          continue;
        }

        const anyReachable = variantSkus.some((sku) => matchedSkuKeys.has(sku));
        if (anyReachable) {
          skippedHasAmrodMatch++;
          continue;
        }

        candidates++;
        const row = {
          id: p.id,
          title: p.title,
          variantSkus,
          dryRun,
          ts: new Date().toISOString(),
        };
        logStream.write(JSON.stringify({ ...row, event: dryRun ? "would_archive" : "archive" }) + "\n");

        if (dryRun) continue;

        const mut = await shopifyGraphql(ARCHIVE_MUTATION, {
          input: { id: p.id, status: "ARCHIVED" },
        });

        const errs = mut?.productUpdate?.userErrors || [];
        if (errs.length) {
          archiveFailed++;
          console.log(`::warning title=Archive failed::${p.title} | ${JSON.stringify(errs)}`);
          logStream.write(
            JSON.stringify({ ...row, event: "archive_failed", userErrors: errs }) + "\n"
          );
        } else {
          archivedOk++;
          console.log(`Archived: ${p.title}`);
        }

        if (delayMs > 0) await sleep(delayMs);

        if (stopAfterLimit()) {
          console.log(`Reached ARCHIVE_MAX_PRODUCTS=${maxArchive}, stopping.`);
          break;
        }
      }

      if (stopAfterLimit()) break;

      cursor = conn?.pageInfo?.hasNextPage ? conn.pageInfo.endCursor : null;
    } while (cursor);
  } finally {
    await new Promise((r) => logStream.end(r));
  }

  console.log(
    `Done. scanned=${scanned} candidates=${candidates} skipped_amrod_price_match=${skippedHasAmrodMatch} ` +
      `skipped_no_variant_sku=${skippedNoVariantSku} archived_ok=${archivedOk} archive_failed=${archiveFailed} ` +
      `dry_run=${dryRun} log=${logPath}`
  );
}

main().catch((e) => {
  console.error("🔥", e?.message || e);
  process.exit(1);
});
