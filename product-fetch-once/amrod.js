// amrod.js
import {
  AMROD_AUTH_ENDPOINT,
  AMROD_PRODUCTS_ENDPOINT,
  AMROD_AUTH_DETAILS,
  AMROD_UPDATED_PRODUCTS_ENDPOINT,
  AMROD_UPDATED_PRICES_ENDPOINT,
  AMROD_STOCK_ALL_ENDPOINT,
  AMROD_STOCK_UPDATED_ENDPOINT,
} from "./config.js";

function requireCustomerCodeUrl(baseUrl) {
  const code = String(AMROD_AUTH_DETAILS?.CustomerCode || "").trim();
  if (!code) {
    throw new Error("AMROD_CUSTOMER_CODE missing (required for Amrod vendor API query)");
  }
  const sep = baseUrl.includes("?") ? "&" : "?";
  return `${baseUrl}${sep}CustomerCode=${encodeURIComponent(code)}`;
}

async function readJsonResponse(res, context) {
  const text = await res.text();
  const ct = res.headers.get("content-type") || "";
  if (!text?.trim()) {
    throw new Error(
      `${context}: empty response body (status ${res.status}, content-type: ${ct || "none"})`
    );
  }
  try {
    return JSON.parse(text);
  } catch (e) {
    const preview = text.length > 400 ? `${text.slice(0, 400)}…` : text;
    throw new Error(
      `${context}: invalid JSON (${e?.message || e}). Body (${text.length} chars): ${preview}`
    );
  }
}

/** 204 / empty body → null; otherwise parsed JSON (object or array). */
async function readJsonOptional(res, context) {
  if (res.status === 204) return null;
  const text = await res.text();
  if (!text?.trim()) return null;
  try {
    return JSON.parse(text);
  } catch (e) {
    const preview = text.length > 400 ? `${text.slice(0, 400)}…` : text;
    throw new Error(
      `${context}: invalid JSON (${e?.message || e}). Body (${text.length} chars): ${preview}`
    );
  }
}

function coerceJsonArray(data, context) {
  if (data == null) return [];
  if (Array.isArray(data)) return data;
  const inner = data?.Products ?? data?.products ?? data?.items ?? data?.data;
  if (Array.isArray(inner)) return inner;
  throw new Error(`${context}: expected a JSON array or wrapper with Products[]`);
}

function normalizeAmrodProductsList(data) {
  if (Array.isArray(data)) return data;
  const fromProducts = data?.Products ?? data?.products;
  if (Array.isArray(fromProducts)) return fromProducts;
  throw new Error(
    "Amrod products response was not an array and had no Products[] — check API shape"
  );
}

// Built-in Node fetch retry wrapper (no undici dependency required)
async function fetchWithRetry(url, options = {}, opts = {}) {
  const { retries = 6, baseDelayMs = 800, timeoutMs = 60_000 } = opts;

  let lastErr;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "User-Agent": "amrod-product-sync/1.0 (+node20)",
          ...(options.headers || {}),
        },
      });

      // Retry on 429/5xx
      if (!res.ok) {
        if ((res.status === 429 || res.status >= 500) && attempt < retries) {
          const delay = baseDelayMs * Math.pow(2, attempt);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }

        const body = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${body}`);
      }

      return res;
    } catch (err) {
      lastErr = err;

      const msg = String(err?.message || "").toLowerCase();
      const causeCode = err?.cause?.code || err?.code;

      const transient =
        causeCode === "UND_ERR_SOCKET" ||
        msg.includes("fetch failed") ||
        msg.includes("socket") ||
        msg.includes("econnreset") ||
        msg.includes("etimedout") ||
        msg.includes("aborted");

      if (!transient || attempt >= retries) throw err;

      const delay = baseDelayMs * Math.pow(2, attempt);
      await new Promise((r) => setTimeout(r, delay));
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr || new Error("fetchWithRetry failed");
}

export const fetchAmrodToken = async () => {
  const res = await fetchWithRetry(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(AMROD_AUTH_DETAILS),
  });

  const data = await readJsonResponse(res, "Amrod auth");
  const tok = data?.token ?? data?.Token;
  if (!tok) throw new Error("No Amrod token returned");
  return tok;
};

export const fetchAmrodProducts = async (token) => {
  const code = String(AMROD_AUTH_DETAILS?.CustomerCode || "").trim();
  if (!code) {
    throw new Error(
      "AMROD_CUSTOMER_CODE missing — GetProductsAndBranding needs ?CustomerCode= in GitHub/env"
    );
  }

  const url = `${AMROD_PRODUCTS_ENDPOINT}?CustomerCode=${encodeURIComponent(code)}`;
  const res = await fetchWithRetry(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  const raw = await readJsonResponse(res, "Amrod GetProductsAndBranding");
  return normalizeAmrodProductsList(raw);
};

const amrodGetJson = (url, token) =>
  fetchWithRetry(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

export const fetchUpdatedProductsAndBranding = async (token) => {
  const url = requireCustomerCodeUrl(AMROD_UPDATED_PRODUCTS_ENDPOINT);
  const res = await amrodGetJson(url, token);
  const raw = await readJsonOptional(res, "Amrod GetUpdatedProductsAndBranding");
  return coerceJsonArray(raw, "Amrod GetUpdatedProductsAndBranding");
};

export const fetchUpdatedPrices = async (token) => {
  const url = requireCustomerCodeUrl(AMROD_UPDATED_PRICES_ENDPOINT);
  const res = await amrodGetJson(url, token);
  const raw = await readJsonOptional(res, "Amrod Prices GetUpdated");
  return coerceJsonArray(raw, "Amrod Prices GetUpdated");
};

export const fetchStockAll = async (token) => {
  const res = await amrodGetJson(AMROD_STOCK_ALL_ENDPOINT, token);
  const raw = await readJsonOptional(res, "Amrod Stock (all)");
  return coerceJsonArray(raw, "Amrod Stock (all)");
};

export const fetchStockUpdated = async (token) => {
  const res = await amrodGetJson(AMROD_STOCK_UPDATED_ENDPOINT, token);
  const raw = await readJsonOptional(res, "Amrod Stock GetUpdated");
  return coerceJsonArray(raw, "Amrod Stock GetUpdated");
};
