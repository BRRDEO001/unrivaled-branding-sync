// tree-map-import/amrod-categories.js
import {
  AMROD_AUTH_ENDPOINT,
  AMROD_CATEGORIES_ENDPOINT,
  AMROD_AUTH_DETAILS,
} from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
          "User-Agent": "amrod-category-tree-sync/1.0 (+node20)",
          ...(options.headers || {}),
        },
      });

      if (!res.ok) {
        if ((res.status === 429 || res.status >= 500) && attempt < retries) {
          await sleep(Math.min(baseDelayMs * Math.pow(2, attempt), 30_000));
          continue;
        }

        const body = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${body}`);
      }

      return res;
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
      await sleep(Math.min(baseDelayMs * Math.pow(2, attempt), 30_000));
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr || new Error("fetchWithRetry failed");
}

export async function fetchAmrodToken() {
  const res = await fetchWithRetry(AMROD_AUTH_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(AMROD_AUTH_DETAILS),
  });

  const data = await res.json();
  if (!data.token) throw new Error("No Amrod token returned");
  return data.token;
}

export async function fetchAmrodCategories(token) {
  if (!AMROD_CATEGORIES_ENDPOINT) {
    throw new Error("AMROD_CATEGORIES_ENDPOINT missing in config.js");
  }

  const res = await fetchWithRetry(AMROD_CATEGORIES_ENDPOINT, {
    method: "GET",
    headers: { Authorization: `Bearer ${token}` },
  });

  return res.json();
}
