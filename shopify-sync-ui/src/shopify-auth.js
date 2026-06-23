/**
 * Verify Shopify App Bridge session token (JWT, HS256).
 * https://shopify.dev/docs/apps/build/authentication-authorization/session-tokens
 */
function base64UrlDecode(str) {
  const pad = str.length % 4 === 0 ? "" : "=".repeat(4 - (str.length % 4));
  const b64 = str.replace(/-/g, "+").replace(/_/g, "/") + pad;
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function decodeJwtPayload(token) {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("Invalid session token format");
  const json = new TextDecoder().decode(base64UrlDecode(parts[1]));
  return JSON.parse(json);
}

async function verifyHs256(token, secret) {
  const parts = token.split(".");
  if (parts.length !== 3) return false;

  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"]
  );

  const data = new TextEncoder().encode(`${parts[0]}.${parts[1]}`);
  const sig = base64UrlDecode(parts[2]);

  return crypto.subtle.verify("HMAC", key, sig, data);
}

export async function verifyShopifySessionToken(token, apiSecret, { allowedShop = null } = {}) {
  if (!token || !apiSecret) return { ok: false, reason: "missing token or secret" };

  const valid = await verifyHs256(token, apiSecret);
  if (!valid) return { ok: false, reason: "invalid signature" };

  let payload;
  try {
    payload = decodeJwtPayload(token);
  } catch {
    return { ok: false, reason: "invalid payload" };
  }

  const now = Math.floor(Date.now() / 1000);
  if (payload.nbf && now < payload.nbf - 10) return { ok: false, reason: "token not yet valid" };
  if (payload.exp && now > payload.exp + 10) return { ok: false, reason: "token expired" };

  const dest = String(payload.dest || "");
  const shop = dest.replace(/^https:\/\//, "").replace(/\/$/, "");
  if (!shop.endsWith(".myshopify.com")) {
    return { ok: false, reason: "unexpected shop domain" };
  }

  if (allowedShop && shop !== allowedShop) {
    return { ok: false, reason: "shop not allowed" };
  }

  return { ok: true, shop, payload };
}

export async function authorizeRequest(request, env) {
  const auth = request.headers.get("Authorization") || "";
  const bearer = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";

  if (bearer && env.SHOPIFY_API_SECRET) {
    const v = await verifyShopifySessionToken(bearer, env.SHOPIFY_API_SECRET, {
      allowedShop: env.ALLOWED_SHOP || null,
    });
    if (v.ok) return { ok: true, method: "shopify-session", shop: v.shop };
  }

  const apiKey = request.headers.get("X-Sync-Api-Key") || "";
  if (env.SYNC_API_KEY && apiKey && apiKey === env.SYNC_API_KEY) {
    return { ok: true, method: "api-key" };
  }

  return { ok: false, reason: "Unauthorized — open this page from Shopify Admin Apps, or provide the staff API key." };
}
