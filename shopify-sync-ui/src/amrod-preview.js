const AMROD_AUTH_ENDPOINT = "https://identity.amrod.co.za/VendorLogin";
const AMROD_PRODUCTS_ENDPOINT =
  "https://vendorapi.amrod.co.za/api/v1/Products/GetProductsAndBranding";

const CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_PREVIEW_MATCHES = 50;

let catalogCache = { at: 0, products: null };

export function normalizeProductName(name) {
  return String(name || "").trim();
}

/** Case-insensitive contains match on productName. */
export function filterProductsByNameContains(products, searchName) {
  const needle = normalizeProductName(searchName).toLowerCase();
  if (!needle || !Array.isArray(products)) return [];

  return products
    .filter((p) => normalizeProductName(p?.productName).toLowerCase().includes(needle))
    .sort((a, b) =>
      normalizeProductName(a?.productName).localeCompare(normalizeProductName(b?.productName))
    )
    .slice(0, MAX_PREVIEW_MATCHES);
}

function pickBestImageUrl(urls) {
  if (!Array.isArray(urls) || urls.length === 0) return null;
  const sorted = [...urls].sort(
    (a, b) => Number(b?.width ?? 0) - Number(a?.width ?? 0)
  );
  return sorted[0]?.url ?? null;
}

function pickProductImageUrl(product) {
  const images = Array.isArray(product?.images) ? product.images : [];
  const defaultImg = images.find((i) => i?.isDefault) || images[0];
  const fromDefault = pickBestImageUrl(defaultImg?.urls);
  if (fromDefault) return fromDefault;

  for (const img of images) {
    const url = pickBestImageUrl(img?.urls);
    if (url) return url;
  }

  for (const c of product?.colourImages || []) {
    const colourImg = (c?.images || []).find((i) => i?.isDefault) || (c?.images || [])[0];
    const url = pickBestImageUrl(colourImg?.urls);
    if (url) return url;
  }

  return null;
}

function pickVariantColour(variant) {
  const value =
    variant?.codeColourName ||
    variant?.colourName ||
    variant?.colorName ||
    variant?.codeColour ||
    variant?.colour ||
    null;
  return value ? String(value).trim() : null;
}

function pickVariantSize(variant) {
  const value =
    variant?.codeSizeName || variant?.sizeName || variant?.codeSize || variant?.size || null;
  return value ? String(value).trim() : null;
}

function uniqueVariantValues(variants, pick) {
  const seen = new Set();
  const out = [];

  for (const variant of variants) {
    const value = pick(variant);
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }

  return out;
}

function summarizeProduct(product) {
  const variants = Array.isArray(product?.variants) ? product.variants : [];
  const imageUrl = pickProductImageUrl(product);

  return {
    productName: normalizeProductName(product?.productName),
    fullCode: product?.fullCode || product?.simpleCode || null,
    imageUrl,
    variantCount: variants.length || 1,
    colours: uniqueVariantValues(variants, pickVariantColour),
    sizes: uniqueVariantValues(variants, pickVariantSize),
  };
}

async function fetchAmrodToken(env) {
  const username = env.AMROD_USERNAME;
  const password = env.AMROD_PASSWORD;
  const customerCode = env.AMROD_CUSTOMER_CODE;

  if (!username || !password || !customerCode) {
    throw new Error(
      "Amrod credentials not configured. Run: npx wrangler secret put AMROD_USERNAME (and AMROD_PASSWORD, AMROD_CUSTOMER_CODE)"
    );
  }

  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({
      Username: username,
      Password: password,
      CustomerCode: customerCode,
    }),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Amrod auth failed (${res.status}): ${text || res.statusText}`);
  }

  const data = await res.json();
  const token = data?.token ?? data?.Token;
  if (!token) throw new Error("Amrod auth returned no token");
  return token;
}

async function fetchAmrodCatalog(env) {
  const now = Date.now();
  if (catalogCache.products && now - catalogCache.at < CATALOG_CACHE_TTL_MS) {
    return catalogCache.products;
  }

  const customerCode = String(env.AMROD_CUSTOMER_CODE || "").trim();
  const token = await fetchAmrodToken(env);
  const url = `${AMROD_PRODUCTS_ENDPOINT}?CustomerCode=${encodeURIComponent(customerCode)}`;

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Amrod catalog fetch failed (${res.status}): ${text || res.statusText}`);
  }

  const raw = await res.json();
  const products = Array.isArray(raw)
    ? raw
    : Array.isArray(raw?.Products)
      ? raw.Products
      : Array.isArray(raw?.products)
        ? raw.products
        : null;

  if (!products) {
    throw new Error("Amrod catalog response was not a product array");
  }

  catalogCache = { at: now, products };
  return products;
}

export async function previewAmrodProducts(env, productName) {
  const searchName = normalizeProductName(productName);
  if (!searchName) {
    throw new Error("Product name is required");
  }

  const catalog = await fetchAmrodCatalog(env);
  const allMatches = filterProductsByNameContains(catalog, searchName);
  const truncated = allMatches.length >= MAX_PREVIEW_MATCHES;

  return {
    ok: true,
    searchName,
    searchMode: "contains",
    catalogSize: catalog.length,
    matchCount: allMatches.length,
    truncated,
    matches: allMatches.map(summarizeProduct),
  };
}
