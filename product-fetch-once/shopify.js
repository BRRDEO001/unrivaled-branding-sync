// shopify.js
import { SHOPIFY_STORE, SHOPIFY_TOKEN, SHOPIFY_API_VERSION } from "./config.js";
import { logImageFailure } from "./logger.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const shopifyFetch = async (endpoint, method = "GET", body, opts = {}) => {
  if (!SHOPIFY_TOKEN) throw new Error("SHOPIFY_TOKEN not set");

  const { retries = 8, baseDelayMs = 250, timeoutMs = 60_000 } = opts;

  let lastErr;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(
        `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/${endpoint}`,
        {
          method,
          signal: controller.signal,
          headers: {
            "X-Shopify-Access-Token": SHOPIFY_TOKEN,
            "Content-Type": "application/json",
          },
          body: body ? JSON.stringify(body) : undefined,
        }
      );

      // Dynamic throttling: if near REST call cap, pause briefly
      const callLimit = res.headers.get("x-shopify-shop-api-call-limit");
      if (callLimit) {
        const [used, limit] = callLimit.split("/").map((n) => Number(n));
        if (
          Number.isFinite(used) &&
          Number.isFinite(limit) &&
          used >= limit - 2
        ) {
          await sleep(400 + attempt * 100);
        }
      }

      // Retry on 429/5xx
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        const retryAfter = res.headers.get("retry-after");
        const waitMs = retryAfter
          ? Number(retryAfter) * 1000
          : baseDelayMs * Math.pow(2, attempt);

        const bodyText = await res.text().catch(() => "");

        if (attempt >= retries) {
          throw new Error(`Shopify ${res.status}: ${bodyText}`);
        }

        await sleep(Math.min(waitMs, 30_000));
        continue;
      }

      if (!res.ok) {
        throw new Error(`Shopify error ${res.status}: ${await res.text()}`);
      }

      const text = await res.text();
      if (!text?.trim()) return {};
      return JSON.parse(text);
    } catch (err) {
      lastErr = err;

      const msg = String(err?.message || "").toLowerCase();
      const transient =
        msg.includes("fetch failed") ||
        msg.includes("socket") ||
        msg.includes("econnreset") ||
        msg.includes("etimedout") ||
        msg.includes("aborted");

      if (!transient || attempt >= retries) throw err;

      const waitMs = baseDelayMs * Math.pow(2, attempt);
      await sleep(Math.min(waitMs, 30_000));
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr || new Error("shopifyFetch failed");
};

/**
 * GraphQL helper
 */
export const shopifyGraphql = async (query, variables = {}, opts = {}) => {
  if (!SHOPIFY_TOKEN) throw new Error("SHOPIFY_TOKEN not set");

  const endpoint = `graphql.json`;
  const res = await shopifyFetch(endpoint, "POST", { query, variables }, opts);

  // shopifyFetch already throws on non-200, but GraphQL can still return errors
  if (res?.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(res.errors)}`);
  }

  return res.data;
};

/**
 * Creates a Shopify product.
 * If Shopify rejects images, we log it and retry without images.
 *
 * NOTE: pass the Amrod product object as the 2nd arg so logs contain fullCode/name.
 */
export const createShopifyProduct = async (payload, amrod = null) => {
  const attemptCreate = async (body) => {
    const res = await shopifyFetch("products.json", "POST", body);
    return res.product;
  };

  try {
    return await attemptCreate(payload);
  } catch (err) {
    const msg = String(err?.message || "").toLowerCase();

    const looksLikeImageError =
      msg.includes("image") ||
      msg.includes("images") ||
      msg.includes("src") ||
      msg.includes("url") ||
      msg.includes("invalid");

    if (!looksLikeImageError) throw err;

    console.warn("⚠️ Product image rejected. Retrying without images...");

    // ✅ Log image rejection separately
    logImageFailure({
      amrod,
      error: err,
      extra: {
        endpoint: "POST products.json",
        hadImagesInPayload:
          Array.isArray(payload?.product?.images) &&
          payload.product.images.length > 0,
      },
    });

    const payloadWithoutImages = {
      ...payload,
      product: { ...payload.product, images: [] },
    };

    return await attemptCreate(payloadWithoutImages);
  }
};

export const updateShopifyVariant = async (variantId, variantBody) => {
  const res = await shopifyFetch(`variants/${variantId}.json`, "PUT", {
    variant: { id: variantId, ...variantBody },
  });
  return res.variant;
};

export const createShopifyVariant = async (productId, variantBody) => {
  const res = await shopifyFetch(`products/${productId}/variants.json`, "POST", {
    variant: variantBody,
  });
  return res.variant;
};

export const deleteShopifyProduct = async (legacyProductId) => {
  await shopifyFetch(`products/${legacyProductId}.json`, "DELETE");
};

export const setInventoryLevel = async (inventoryItemId, locationId, available) => {
  await shopifyFetch(`inventory_levels/set.json`, "POST", {
    location_id: locationId,
    inventory_item_id: inventoryItemId,
    available,
  });
};

/** Primary location legacy ID for REST `inventory_levels` (GraphQL, then REST fallback). */
export async function getPrimaryLocationId() {
  const q = `
    query {
      shop {
        primaryLocation {
          legacyResourceId
        }
      }
    }
  `;
  try {
    const data = await shopifyGraphql(q);
    const id = data?.shop?.primaryLocation?.legacyResourceId;
    if (id != null) return Number(id);
  } catch {
    // fall through
  }

  const res = await shopifyFetch("locations.json?limit=250");
  const locs = (res.locations || []).filter((l) => l.active);
  if (!locs.length) return null;
  const primary = locs.find((l) => l.primary === true);
  return primary?.id ?? locs[0]?.id ?? null;
}

function legacyFromGid(gid) {
  if (gid == null) return null;
  if (typeof gid === "number") return gid;
  const m = String(gid).match(/(\d+)$/);
  return m ? Number(m[1]) : null;
}

const VARIANT_LOOKUP = `
  query VariantLookup($q: String!) {
    productVariants(first: 8, query: $q) {
      nodes {
        id
        legacyResourceId
        sku
        inventoryItem {
          id
          legacyResourceId
        }
        product {
          legacyResourceId
        }
      }
    }
  }
`;

/** First matching variant for ordered SKU candidates (prefers exact sku match). */
export const findShopifyVariantBySkuCandidates = async (skus) => {
  const cleaned = [...new Set(skus.map((s) => String(s || "").trim()).filter(Boolean))];
  for (const sku of cleaned) {
    const data = await shopifyGraphql(VARIANT_LOOKUP, { q: `sku:${sku}` });
    const nodes = data?.productVariants?.nodes || [];
    if (!nodes.length) continue;
    const exact = nodes.find((n) => n.sku === sku) || nodes[0];
    const invLegacy =
      exact.inventoryItem?.legacyResourceId != null
        ? Number(exact.inventoryItem.legacyResourceId)
        : legacyFromGid(exact.inventoryItem?.id);
    return {
      variantId: Number(exact.legacyResourceId),
      sku: exact.sku,
      productId: Number(exact.product?.legacyResourceId),
      inventoryItemId: invLegacy,
    };
  }
  return null;
};

/**
 * ✅ Throttled image uploader to avoid Shopify 429 ("Exceeded 2 calls/sec")
 *
 * Control speed with:
 *   IMAGE_MIN_INTERVAL_MS=400   (≈ 2.5/sec; Shopify limit is leaky-bucket per second)
 */
const IMAGE_MIN_INTERVAL_MS = Number(process.env.IMAGE_MIN_INTERVAL_MS ?? 400) || 400;

// Queue to serialize image uploads across concurrent workers
let imageQueue = Promise.resolve();
let lastImageAt = 0;

export const createShopifyProductImage = (productId, imageBody) => {
  const run = async () => {
    const now = Date.now();
    const waitMs = Math.max(0, lastImageAt + IMAGE_MIN_INTERVAL_MS - now);
    if (waitMs) await sleep(waitMs);
    lastImageAt = Date.now();

    const res = await shopifyFetch(`products/${productId}/images.json`, "POST", {
      image: imageBody,
    });
    return res.image;
  };

  const p = imageQueue.then(run, run);
  // keep queue alive even if one upload fails
  imageQueue = p.catch(() => {});
  return p;
};

/**
 * ✅ Courier Guy compatible: update InventoryItem measurement
 *
 * - weightKg: number
 * - shippingPackageId: optional Shopify GID "gid://shopify/ShippingPackage/..."
 *
 * Requires the app token to have write_inventory permissions.
 */
export const updateInventoryItemMeasurement = async ({
  inventoryItemId,
  weightKg,
  shippingPackageId = null,
}) => {
  if (!inventoryItemId) throw new Error("inventoryItemId required");

  const gid = String(inventoryItemId).startsWith("gid://")
    ? inventoryItemId
    : `gid://shopify/InventoryItem/${inventoryItemId}`;

  const mutation = `
    mutation InventoryItemMeasurementUpdate($input: InventoryItemInput!) {
      inventoryItemUpdate(input: $input) {
        inventoryItem { id }
        userErrors { field message }
      }
    }
  `;

  const measurement = {
    weight: { value: Number(weightKg || 0), unit: "KILOGRAMS" },
    ...(shippingPackageId ? { shippingPackageId } : {}),
  };

  const data = await shopifyGraphql(mutation, {
    input: {
      id: gid,
      measurement,
    },
  });

  const errs = data?.inventoryItemUpdate?.userErrors || [];
  if (errs.length) {
    throw new Error(`inventoryItemUpdate userErrors: ${JSON.stringify(errs)}`);
  }

  return data.inventoryItemUpdate.inventoryItem;
};
