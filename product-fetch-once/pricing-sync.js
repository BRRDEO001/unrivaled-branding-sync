// pricing-sync.js
import { SHOPIFY_STORE, SHOPIFY_TOKEN, AMROD_AUTH_DETAILS } from "./config.js";
import { shopifyGraphql } from "./shopify.js";
import { fetchAmrodToken } from "./amrod.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJsonWithRetry(
  url,
  options = {},
  { retries = 6, baseDelayMs = 400 } = {}
) {
  let lastErr;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);

      // Retry on 429/5xx
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        const retryAfter = res.headers.get("retry-after");
        const waitMs = retryAfter
          ? Number(retryAfter) * 1000
          : baseDelayMs * Math.pow(2, attempt);

        const bodyText = await res.text().catch(() => "");

        if (attempt >= retries) {
          throw new Error(`HTTP ${res.status} :: ${bodyText}`);
        }

        await sleep(Math.min(waitMs, 30_000));
        continue;
      }

      if (!res.ok) {
        const bodyText = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${bodyText}`);
      }

      return await res.json();
    } catch (e) {
      lastErr = e;
      if (attempt >= retries) throw e;
      await sleep(Math.min(baseDelayMs * Math.pow(2, attempt), 30_000));
    }
  }

  throw lastErr || new Error("fetchJsonWithRetry failed");
}

/**
 * Transform Amrod pricing array -> { [brandingCode]: [tiers...] }
 * Normalized tier shape:
 * { printCode, min, max, colours, setup, unit }
 */
function normalizePricing(raw) {
  if (!Array.isArray(raw)) {
    throw new Error(
      `Unexpected pricing response (expected array). Got: ${typeof raw}`
    );
  }

  const out = {};

  for (const method of raw) {
    const code = String(method?.brandingCode || "").trim();
    if (!code) continue;

    const tiers = Array.isArray(method?.data) ? method.data : [];

    out[code] = tiers
      .map((t) => ({
        printCode: t?.printCode ?? null,
        min: Number(t?.minQuantity ?? 0),
        max: Number(t?.maxQuantity ?? -1),
        colours: Number(t?.numberOfColours ?? 0),
        setup: Number(t?.setup ?? 0),
        unit: Number(t?.price ?? 0),
      }))
      .sort((a, b) => (a.colours - b.colours) || (a.min - b.min));
  }

  return out;
}

async function getShopGid() {
  const q = `
    query {
      shop { id }
    }
  `;
  const data = await shopifyGraphql(q);
  const id = data?.shop?.id;
  if (!id) throw new Error("Could not fetch shop id (GID)");
  return id;
}

async function upsertShopPricingMetafield(shopId, pricingObj) {
  const mutation = `
    mutation SetPricing($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key }
        userErrors { field message }
      }
    }
  `;

  const value = JSON.stringify(pricingObj);

  const variables = {
    metafields: [
      {
        ownerId: shopId,
        namespace: "custom",
        key: "branding_options_pricing",
        type: "json",
        value,
      },
    ],
  };

  const data = await shopifyGraphql(mutation, variables);
  const errs = data?.metafieldsSet?.userErrors || [];
  if (errs.length) {
    throw new Error(`metafieldsSet userErrors: ${JSON.stringify(errs)}`);
  }

  return data?.metafieldsSet?.metafields?.[0] || null;
}

async function main() {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) {
    throw new Error("Missing SHOPIFY_STORE or SHOPIFY_TOKEN in config/secrets");
  }

  // ✅ CustomerCode is nested in your config.js
  const CUSTOMER_CODE = String(AMROD_AUTH_DETAILS?.CustomerCode || "").trim();
  if (!CUSTOMER_CODE) {
    throw new Error(
      "Missing AMROD_CUSTOMER_CODE (set it in GitHub secrets so config.js receives it)"
    );
  }

  // ✅ Update this if your actual pricing endpoint differs
  const PRICING_ENDPOINT_BASE =
    "https://vendorapi.amrod.co.za/api/v1/Products/GetProductsAndBranding";

  // Most common pattern: CustomerCode as query string
  const PRICING_ENDPOINT = `${PRICING_ENDPOINT_BASE}?CustomerCode=${encodeURIComponent(
    CUSTOMER_CODE
  )}`;

  console.log("🔑 Fetching Amrod token...");
  const token = await fetchAmrodToken();

  console.log("💰 Fetching branding pricing...");
  const raw = await fetchJsonWithRetry(PRICING_ENDPOINT, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      // If Amrod pricing endpoint doesn't require auth, remove this header.
      Authorization: `Bearer ${token}`,
    },
  });

  const normalized = normalizePricing(raw);

  const codes = Object.keys(normalized);
  console.log("🧾 Methods loaded:", codes.length);
  if (codes.length) console.log("🧾 Codes:", codes.join(", "));

  console.log("🛍️ Fetching Shopify Shop ID...");
  const shopId = await getShopGid();

  console.log("⬆️ Upserting shop metafield amrod.branding_pricing ...");
  const mf = await upsertShopPricingMetafield(shopId, normalized);

  console.log(
    "✅ Done. Metafield saved:",
    mf ? `${mf.namespace}.${mf.key}` : "unknown"
  );
}

main().catch((e) => {
  console.error("❌ Pricing sync failed:", e?.message || e);
  process.exit(1);
});
