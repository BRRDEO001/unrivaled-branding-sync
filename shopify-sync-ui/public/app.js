const productNameEl = document.getElementById("productName");
const syncBtn = document.getElementById("syncBtn");
const statusEl = document.getElementById("status");
const staffKeyRow = document.getElementById("staffKeyRow");
const staffKeyEl = document.getElementById("staffKey");
const apiKeyMeta = document.getElementById("shopify-api-key-meta");

let shopifyApiKey = "";
let requiresApiKey = false;
let sessionTokenGetter = null;

function showStatus(message, type = "success", html = false) {
  statusEl.classList.remove("hidden", "success", "error");
  statusEl.classList.add(type);
  if (html) {
    statusEl.innerHTML = message;
  } else {
    statusEl.textContent = message;
  }
}

function hideStatus() {
  statusEl.classList.add("hidden");
}

async function loadConfig() {
  const res = await fetch("/api/config");
  const cfg = await res.json();
  shopifyApiKey = cfg.shopifyApiKey || "";
  requiresApiKey = Boolean(cfg.requiresApiKey);
  if (shopifyApiKey) {
    apiKeyMeta.setAttribute("content", shopifyApiKey);
  }
  if (requiresApiKey && !sessionTokenGetter) {
    staffKeyRow.classList.remove("hidden");
  }
}

async function initAppBridge() {
  const params = new URLSearchParams(window.location.search);
  const shop = params.get("shop");
  if (!shopifyApiKey || !shop || typeof shopify === "undefined") {
    return;
  }

  try {
    await shopify.config({
      apiKey: shopifyApiKey,
      shop,
      host: params.get("host") || undefined,
    });

    sessionTokenGetter = async () => {
      const token = await shopify.idToken();
      return token;
    };
  } catch (e) {
    console.warn("App Bridge init failed:", e);
  }
}

async function authHeaders() {
  const headers = { "Content-Type": "application/json" };

  if (sessionTokenGetter) {
    try {
      const token = await sessionTokenGetter();
      if (token) {
        headers.Authorization = `Bearer ${token}`;
        return headers;
      }
    } catch (e) {
      console.warn("Session token unavailable:", e);
    }
  }

  const key = staffKeyEl.value.trim();
  if (key) {
    headers["X-Sync-Api-Key"] = key;
  }

  return headers;
}

async function runSync() {
  hideStatus();

  const productName = productNameEl.value.trim();
  if (!productName) {
    showStatus("Enter the exact Amrod product name.", "error");
    productNameEl.focus();
    return;
  }

  syncBtn.disabled = true;
  syncBtn.textContent = "Starting sync…";

  try {
    const headers = await authHeaders();
    const res = await fetch("/api/sync", {
      method: "POST",
      headers,
      body: JSON.stringify({ productName }),
    });

    const data = await res.json();

    if (!res.ok || !data.ok) {
      throw new Error(data.error || `Request failed (${res.status})`);
    }

    const link = data.actionsUrl
      ? `<a href="${data.actionsUrl}" target="_blank" rel="noopener">View progress</a>`
      : "";

    showStatus(
      `${data.message || "Sync started."} ${link}`.trim(),
      "success",
      Boolean(link)
    );

    productNameEl.value = "";
  } catch (e) {
    showStatus(String(e.message || e), "error");
  } finally {
    syncBtn.disabled = false;
    syncBtn.textContent = "Sync to Shopify";
  }
}

syncBtn.addEventListener("click", runSync);
productNameEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter") runSync();
});

await loadConfig();
await initAppBridge();
