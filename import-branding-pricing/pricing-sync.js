// pricing-sync.js
import { SHOPIFY_STORE, SHOPIFY_TOKEN } from "./config.js";
import { shopifyGraphql } from "./shopify.js";
import { fetchAmrodToken } from "./amrod.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJsonWithRetry(url, options = {}, { retries = 6, baseDelayMs = 400 } = {}) {
  let lastErr;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);

      // Retry on 429/5xx
      if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
        const retryAfter = res.headers.get("retry-after");
        const waitMs = retryAfter ? Number(retryAfter) * 1000 : baseDelayMs * Math.pow(2, attempt);
        if (attempt >= retries) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
        await sleep(Math.min(waitMs, 30_000));
        continue;
      }

      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);

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
 */
function normalizePricing(raw) {
  if (!Array.isArray(raw)) {
    throw new Error(`Unexpected pricing response (expected array). Got: ${typeof raw}`);
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
        namespace: "amrod",
        key: "branding_pricing",
        type: "json",
        value,
      },
    ],
  };

  const data = await shopifyGraphql(mutation, variables);
  const errs = data?.metafieldsSet?.userErrors || [];
  if (errs.length) throw new Error(`metafieldsSet userErrors: ${JSON.stringify(errs)}`);

  return data?.metafieldsSet?.metafields?.[0] || null;
}

async function main() {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) {
    throw new Error("Missing SHOPIFY_STORE or SHOPIFY_TOKEN in config/secrets");
  }

  // ✅ Put your real pricing endpoint here (FULL URL)
  // Example: https://vendorapi.amrod.co.za/api/v1/Branding/GetBrandingPricing
  const PRICING_ENDPOINT =
    process.env.AMROD_PRICING_ENDPOINT ||
    "https://vendorapi.amrod.co.za/api/v1/Branding/GetBrandingPricing";

  console.log("🔑 Fetching Amrod token...");
  const token = await fetchAmrodToken();

  console.log("💰 Fetching branding pricing...");
  const raw = await fetchJsonWithRetry(PRICING_ENDPOINT, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      // If Amrod pricing endpoint does NOT need auth, you can remove this header.
      Authorization: `Bearer ${token}`,
    },
  });

  const normalized = normalizePricing(raw);

  console.log("🧾 Methods loaded:", Object.keys(normalized).length);

  console.log("🛍️ Fetching Shopify Shop ID...");
  const shopId = await getShopGid();

  console.log("⬆️ Upserting shop metafield amrod.branding_pricing ...");
  const mf = await upsertShopPricingMetafield(shopId, normalized);

  console.log("✅ Done. Metafield saved:", mf ? `${mf.namespace}.${mf.key}` : "unknown");
}

main().catch((e) => {
  console.error("❌ Pricing sync failed:", e?.message || e);
  process.exit(1);
});
