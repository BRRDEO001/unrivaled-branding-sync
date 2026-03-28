// amrod.js
import {
  AMROD_AUTH_ENDPOINT,
  AMROD_PRODUCTS_ENDPOINT,
  AMROD_AUTH_DETAILS,
} from "./config.js";

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

  const data = await res.json();
  if (!data.token) throw new Error("No Amrod token returned");
  return data.token;
};

export const fetchAmrodProducts = async (token) => {
  const res = await fetchWithRetry(AMROD_PRODUCTS_ENDPOINT, {
    method: "GET",
    headers: { Authorization: `Bearer ${token}` },
  });

  return res.json();
};
