import { findShopifyVariantBySkuCandidates } from "./shopify.js";
import { aggregateStock, applyStockWithRecovery, resolveStockLocationId } from "./stock-apply-full.js";

/** Apply Amrod stock levels for variant SKUs present in `skuSet`. */
export async function applyStockForSkuSet(stockRows, skuSet) {
  if (!skuSet?.size) {
    return { ok: 0, miss: 0, failed: 0, matchedRows: 0, skipNoStock: 0 };
  }

  const locationId = await resolveStockLocationId();
  if (locationId == null) {
    console.log("::warning::No Shopify location — skipping stock apply");
    return { ok: 0, miss: 0, failed: 0, matchedRows: 0, skipNoStock: 0, skipped: true };
  }

  console.log(`📍 Stock apply for ${skuSet.size} SKU(s) at location ${locationId}`);

  const bySku = aggregateStock(stockRows);
  const delayMs = Number(process.env.STOCK_APPLY_DELAY_MS || process.env.REQUEST_DELAY_MS || 120);

  let ok = 0;
  let miss = 0;
  let failed = 0;
  let matchedRows = 0;
  let skipNoStock = 0;

  for (const sku of skuSet) {
    const qty = bySku.get(sku);
    if (qty == null) {
      skipNoStock++;
      continue;
    }

    matchedRows++;
    const rec = await findShopifyVariantBySkuCandidates([sku]);
    if (!rec?.inventoryItemId) {
      miss++;
      continue;
    }

    try {
      await applyStockWithRecovery(
        rec.inventoryItemId,
        locationId,
        Math.max(0, Math.floor(qty))
      );
      ok++;
    } catch (e) {
      failed++;
      console.log(
        `::warning title=Stock set failed::${sku} loc ${locationId} | ${String(e?.message || e)}`
      );
    }

    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  return { ok, miss, failed, matchedRows, skipNoStock };
}
