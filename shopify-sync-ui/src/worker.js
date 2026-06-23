import { authorizeRequest } from "./shopify-auth.js";
import { triggerSyncWorkflow } from "./github-trigger.js";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Sync-Api-Key",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (url.pathname === "/api/config" && request.method === "GET") {
      return json({
        shopifyApiKey: env.SHOPIFY_API_KEY || "",
        requiresApiKey: Boolean(env.SYNC_API_KEY),
      });
    }

    if (url.pathname === "/api/sync" && request.method === "POST") {
      const auth = await authorizeRequest(request, env);
      if (!auth.ok) return json({ ok: false, error: auth.reason }, 401);

      let body;
      try {
        body = await request.json();
      } catch {
        return json({ ok: false, error: "Invalid JSON body" }, 400);
      }

      try {
        const result = await triggerSyncWorkflow(env, body.productName);
        return json({ ...result, triggeredBy: auth.method, shop: auth.shop || null });
      } catch (e) {
        return json({ ok: false, error: String(e?.message || e) }, 502);
      }
    }

    if (url.pathname === "/api/health") {
      return json({ ok: true, service: "amrod-sync-ui" });
    }

    return env.ASSETS.fetch(request);
  },
};
