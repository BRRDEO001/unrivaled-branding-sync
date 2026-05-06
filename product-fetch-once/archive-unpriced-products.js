#!/usr/bin/env node
/**
 * Archive Shopify products where every variant has no usable price (missing, blank, or <= 0).
 * Skips products that are already not ACTIVE. Uses GraphQL productUpdate(status: ARCHIVED).
 *
 * Env:
 *   ARCHIVE_DRY_RUN=true   — log candidates only, no mutations
 *   ARCHIVE_MAX_PRODUCTS=N — stop after N successful archives (0 = unlimited)
 *   ARCHIVE_DELAY_MS       — pause after each archive (default REQUEST_DELAY_MS or 400)
 *   PRODUCTS_PAGE_SIZE     — products per GraphQL page (default 50)
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { shopifyGraphql } from "./shopify.js";
import { REQUEST_DELAY_MS } from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function variantHasPrice(priceVal) {
  if (priceVal == null || priceVal === "") return false;
  const n = Number.parseFloat(String(priceVal));
  return Number.isFinite(n) && n > 0;
}

function productHasAnyPricedVariant(variants) {
  const nodes = variants?.nodes || [];
  return nodes.some((v) => variantHasPrice(v.price));
}

const LIST_QUERY = `
  query UnpricedScan($cursor: String, $pageSize: Int!) {
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
            price
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

  let cursor = null;
  let scanned = 0;
  let skippedHasPrice = 0;
  let candidates = 0;
  let archivedOk = 0;
  let archiveFailed = 0;

  const dir = path.dirname(fileURLToPath(import.meta.url));
  const logDir = path.join(dir, "logs");
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

        if (productHasAnyPricedVariant(p.variants)) {
          skippedHasPrice++;
          continue;
        }

        candidates++;
        const row = {
          id: p.id,
          title: p.title,
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
    `Done. scanned=${scanned} candidates=${candidates} skipped_had_price=${skippedHasPrice} ` +
      `archived_ok=${archivedOk} archive_failed=${archiveFailed} dry_run=${dryRun} log=${logPath}`
  );
}

main().catch((e) => {
  console.error("🔥", e?.message || e);
  process.exit(1);
});
