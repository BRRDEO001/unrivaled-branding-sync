// tree-map-import/shopify-graphql.js
import https from "https";
import { SHOPIFY_STORE, SHOPIFY_TOKEN, SHOPIFY_API_VERSION } from "./config.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isThrottledErrors(errors) {
  return (
    Array.isArray(errors) &&
    errors.some((e) => {
      const msg = String(e?.message || "").toLowerCase();
      const code = String(e?.extensions?.code || "");
      return msg.includes("throttled") || code === "THROTTLED";
    })
  );
}

export async function graphqlFetch(query, variables = {}, opts = {}) {
  const {
    retries = 14,
    baseDelayMs = 600,
    maxDelayMs = 25_000,
  } = opts;

  if (!SHOPIFY_STORE) throw new Error("SHOPIFY_STORE missing");
  if (!SHOPIFY_TOKEN) throw new Error("SHOPIFY_TOKEN missing");
  if (!SHOPIFY_API_VERSION) throw new Error("SHOPIFY_API_VERSION missing");

  const payload = JSON.stringify({ query, variables });

  const options = {
    hostname: SHOPIFY_STORE,
    path: `/admin/api/${SHOPIFY_API_VERSION}/graphql.json`,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(payload),
      "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    },
  };

  let lastErr;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const json = await new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () => {
            try {
              resolve(JSON.parse(body));
            } catch (e) {
              reject(e);
            }
          });
        });

        req.on("error", reject);
        req.write(payload);
        req.end();
      });

      if (!json) throw new Error("Empty GraphQL response");

      // ✅ Retry on THROTTLED
      if (isThrottledErrors(json.errors)) {
        const delay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
        await sleep(delay);
        continue;
      }

      if (json.errors?.length) {
        throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`);
      }

      // userErrors handling (many Shopify mutations return userErrors)
      const firstPayload = json.data && Object.values(json.data)[0];
      const userErrors = firstPayload?.userErrors;
      if (Array.isArray(userErrors) && userErrors.length) {
        throw new Error(`UserErrors: ${JSON.stringify(userErrors)}`);
      }

      return json.data;
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || "").toLowerCase();
      const transient =
        msg.includes("socket") ||
        msg.includes("econnreset") ||
        msg.includes("etimedout") ||
        msg.includes("fetch failed") ||
        msg.includes("throttled") ||
        msg.includes("unexpected end of json input");

      if (!transient || attempt >= retries) throw err;

      const delay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
      await sleep(delay);
    }
  }

  throw lastErr || new Error("graphqlFetch failed after retries");
}
