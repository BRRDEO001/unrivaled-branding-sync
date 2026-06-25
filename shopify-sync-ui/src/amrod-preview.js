const AMROD_AUTH_ENDPOINT = "https://identity.amrod.co.za/VendorLogin";
const AMROD_PRODUCTS_ENDPOINT =
  "https://vendorapi.amrod.co.za/api/v1/Products/GetProductsAndBranding";

const CATALOG_CACHE_TTL_MS = 5 * 60 * 1000;
let catalogCache = { at: 0, products: null };

export function normalizeProductName(name) {
  return String(name || "").trim();
}

export function filterProductsByExactName(products, searchName) {
  const needle = normalizeProductName(searchName);
  if (!needle || !Array.isArray(products)) return [];
  return products.filter((p) => normalizeProductName(p.productName) === needle);
}

function skuCandidates(product) {
  const out = new Set();
  const add = (s) => {
    const v = String(s || "").trim();
    if (v) out.add(v);
  };
  add(product?.fullCode);
  add(product?.simpleCode);
  for (const v of product?.variants || []) {
    add(v?.fullCode);
    add(v?.simpleCode);
  }
  return [...out];
}

function summarizeProduct(product) {
  const variants = Array.isArray(product?.variants) ? product.variants : [];
  return {
    productName: normalizeProductName(product?.productName),
    fullCode: product?.fullCode || product?.simpleCode || null,
    simpleCode: product?.simpleCode || null,
    brand: product?.brandName || product?.brand || null,
    variantCount: variants.length,
    skus: skuCandidates(product),
    variants: variants.slice(0, 12).map((v) => ({
      fullCode: v?.fullCode || v?.simpleCode || null,
      colour: v?.colourName || v?.colorName || v?.colour || null,
      size: v?.sizeName || v?.size || null,
    })),
  };
}

function findSimilarNames(products, searchName, limit = 8) {
  const needle = normalizeProductName(searchName).toLowerCase();
  if (!needle) return [];

  const names = new Set();
  for (const p of products) {
    const name = normalizeProductName(p?.productName);
    if (!name) continue;
    const lower = name.toLowerCase();
    if (lower.includes(needle) || needle.includes(lower)) {
      names.add(name);
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b)).slice(0, limit);
}

function findCaseMismatchName(products, searchName) {
  const needle = normalizeProductName(searchName).toLowerCase();
  if (!needle) return null;

  for (const p of products) {
    const name = normalizeProductName(p?.productName);
    if (name && name.toLowerCase() === needle && name !== normalizeProductName(searchName)) {
      return name;
    }
  }
  return null;
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
  const matches = filterProductsByExactName(catalog, searchName).map(summarizeProduct);
  const caseMismatch = matches.length ? null : findCaseMismatchName(catalog, searchName);
  const similarNames =
    matches.length || caseMismatch ? [] : findSimilarNames(catalog, searchName);

  return {
    ok: true,
    searchName,
    catalogSize: catalog.length,
    matchCount: matches.length,
    matches,
    caseMismatch,
    similarNames,
  };
}
