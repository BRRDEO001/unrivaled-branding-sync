const productNameEl = document.getElementById("productName");
const previewBtn = document.getElementById("previewBtn");
const syncBtn = document.getElementById("syncBtn");
const statusEl = document.getElementById("status");
const previewResultsEl = document.getElementById("previewResults");
const staffKeyRow = document.getElementById("staffKeyRow");
const staffKeyEl = document.getElementById("staffKey");

let requiresApiKey = false;
let appBridgeReady = false;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

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

function hidePreview() {
  previewResultsEl.classList.add("hidden");
  previewResultsEl.innerHTML = "";
}

function isEmbeddedInShopify() {
  const params = new URLSearchParams(window.location.search);
  return Boolean(params.get("host") || params.get("shop"));
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), {
        once: true,
      });
      if (existing.dataset.loaded === "true") resolve();
      return;
    }

    const script = document.createElement("script");
    script.src = src;
    script.async = false;
    script.addEventListener(
      "load",
      () => {
        script.dataset.loaded = "true";
        resolve();
      },
      { once: true }
    );
    script.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), {
      once: true,
    });
    document.head.appendChild(script);
  });
}

async function waitForShopify(maxAttempts = 40) {
  for (let i = 0; i < maxAttempts; i++) {
    if (typeof shopify !== "undefined" && typeof shopify.idToken === "function") {
      return shopify;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return null;
}

async function initAppBridge(apiKey) {
  if (!apiKey) {
    throw new Error("SHOPIFY_API_KEY is not configured on the worker.");
  }

  const meta = document.querySelector('meta[name="shopify-api-key"]');
  if (meta) meta.setAttribute("content", apiKey);

  await loadScript("https://cdn.shopify.com/shopifycloud/app-bridge.js");
  const bridge = await waitForShopify();
  if (!bridge) {
    throw new Error("App Bridge script loaded but shopify global is unavailable.");
  }

  appBridgeReady = true;
}

async function loadConfig() {
  const res = await fetch("/api/config");
  const cfg = await res.json();
  requiresApiKey = Boolean(cfg.requiresApiKey);
  return cfg;
}

async function getSessionToken() {
  if (appBridgeReady) {
    try {
      const token = await shopify.idToken();
      if (token) return token;
    } catch (e) {
      console.warn("Shopify idToken failed:", e);
    }
  }

  return new URLSearchParams(window.location.search).get("id_token");
}

async function requestHeaders() {
  const headers = { "Content-Type": "application/json" };

  const token = await getSessionToken();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
    return headers;
  }

  const key = staffKeyEl.value.trim();
  if (key) {
    headers["X-Sync-Api-Key"] = key;
  }

  return headers;
}

function renderPreviewItem(item) {
  const variantLines = (item.variants || [])
    .map((v) => {
      const bits = [v.fullCode, v.colour, v.size].filter(Boolean);
      return bits.length ? `<li>${escapeHtml(bits.join(" · "))}</li>` : "";
    })
    .filter(Boolean)
    .join("");

  return `
    <article class="preview-item">
      <strong>${escapeHtml(item.fullCode || item.simpleCode || "Unknown code")}</strong>
      <div>${escapeHtml(item.productName || "")}</div>
      <dl>
        <dt>Brand</dt><dd>${escapeHtml(item.brand || "—")}</dd>
        <dt>Variants</dt><dd>${escapeHtml(item.variantCount)}</dd>
        <dt>SKUs</dt><dd>${escapeHtml((item.skus || []).join(", ") || "—")}</dd>
      </dl>
      ${variantLines ? `<ul class="preview-suggestions">${variantLines}</ul>` : ""}
    </article>
  `;
}

function renderPreview(data) {
  const suggestions = [];

  if (data.caseMismatch) {
    suggestions.push(
      `<p class="preview-meta">No exact match, but found the same name with different casing: <button type="button" data-name="${escapeHtml(data.caseMismatch)}">${escapeHtml(data.caseMismatch)}</button></p>`
    );
  }

  if (data.similarNames?.length) {
    suggestions.push(
      `<p class="preview-meta">Similar Amrod names:</p><ul class="preview-suggestions">${data.similarNames
        .map(
          (name) =>
            `<li><button type="button" data-name="${escapeHtml(name)}">${escapeHtml(name)}</button></li>`
        )
        .join("")}</ul>`
    );
  }

  if (!data.matchCount) {
    previewResultsEl.innerHTML = `
      <h2>No Amrod matches</h2>
      <p class="preview-meta">Searched ${escapeHtml(data.catalogSize)} product(s) for "${escapeHtml(data.searchName)}".</p>
      ${suggestions.join("")}
    `;
    previewResultsEl.classList.remove("hidden");
    return;
  }

  previewResultsEl.innerHTML = `
    <h2>${escapeHtml(data.matchCount)} Amrod match(es)</h2>
    <p class="preview-meta">From ${escapeHtml(data.catalogSize)} product(s) in the Amrod catalog.</p>
    <div class="preview-list">${data.matches.map(renderPreviewItem).join("")}</div>
  `;
  previewResultsEl.classList.remove("hidden");
}

previewResultsEl.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-name]");
  if (!btn) return;
  productNameEl.value = btn.dataset.name || "";
  hidePreview();
  productNameEl.focus();
});

async function runPreview() {
  hideStatus();
  hidePreview();

  const productName = productNameEl.value.trim();
  if (!productName) {
    showStatus("Enter the exact Amrod product name.", "error");
    productNameEl.focus();
    return;
  }

  previewBtn.disabled = true;
  syncBtn.disabled = true;
  previewBtn.textContent = "Loading Amrod…";

  try {
    const headers = await requestHeaders();
    if (!headers.Authorization && !headers["X-Sync-Api-Key"]) {
      throw new Error(
        "Could not get a Shopify session token. If this persists, set SYNC_API_KEY on the worker and enter it below."
      );
    }

    const res = await fetch("/api/preview", {
      method: "POST",
      headers,
      body: JSON.stringify({ productName }),
    });

    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || `Preview failed (${res.status})`);
    }

    renderPreview(data);
    if (data.matchCount) {
      showStatus(`Found ${data.matchCount} Amrod product(s). Review below, then click Sync.`, "success");
    } else {
      showStatus("No exact Amrod match. Check similar names below.", "error");
    }
  } catch (e) {
    showStatus(String(e.message || e), "error");
  } finally {
    previewBtn.disabled = false;
    syncBtn.disabled = false;
    previewBtn.textContent = "Preview from Amrod";
  }
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
  previewBtn.disabled = true;
  syncBtn.textContent = "Starting sync…";

  try {
    const headers = await requestHeaders();
    if (!headers.Authorization && !headers["X-Sync-Api-Key"]) {
      throw new Error(
        "Could not get a Shopify session token. If this persists, set SYNC_API_KEY on the worker and enter it below."
      );
    }

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
  } catch (e) {
    showStatus(String(e.message || e), "error");
  } finally {
    syncBtn.disabled = false;
    previewBtn.disabled = false;
    syncBtn.textContent = "Sync to Shopify";
  }
}

previewBtn.addEventListener("click", runPreview);
syncBtn.addEventListener("click", runSync);
productNameEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter") runPreview();
});

const cfg = await loadConfig();

try {
  await initAppBridge(cfg.shopifyApiKey);
} catch (e) {
  console.warn("App Bridge init failed:", e);
  if (requiresApiKey || !isEmbeddedInShopify()) {
    staffKeyRow.classList.remove("hidden");
  }
  showStatus(
    isEmbeddedInShopify()
      ? "Shopify auth did not initialize. Reload from Apps, or use a staff access key."
      : "Open this app from Shopify Admin → Apps for automatic auth, or enter a staff access key.",
    "error"
  );
}

if (requiresApiKey) {
  staffKeyRow.classList.remove("hidden");
}

previewBtn.disabled = false;
syncBtn.disabled = false;
previewBtn.textContent = "Preview from Amrod";
syncBtn.textContent = "Sync to Shopify";
