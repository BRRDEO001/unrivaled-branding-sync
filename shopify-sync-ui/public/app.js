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

function getSelectedProducts() {
  return [...previewResultsEl.querySelectorAll(".preview-select:checked")].map((el) => ({
    fullCode: el.value,
    productName: el.dataset.productName || "",
  }));
}

function updatePreviewSelectionSummary() {
  const summaryEl = previewResultsEl.querySelector("[data-preview-summary]");
  const selectAllEl = previewResultsEl.querySelector("#previewSelectAll");
  if (!summaryEl) return;

  const boxes = [...previewResultsEl.querySelectorAll(".preview-select")];
  const selected = boxes.filter((el) => el.checked).length;
  const total = boxes.length;

  summaryEl.textContent = `Found ${total} product(s). ${selected} selected for sync.`;

  if (selectAllEl) {
    selectAllEl.checked = selected > 0 && selected === total;
    selectAllEl.indeterminate = selected > 0 && selected < total;
  }
}

function renderVariantSummary(item) {
  const lines = [];

  if (item.colours?.length) {
    lines.push(
      `<p class="preview-variant-summary"><strong>Color:</strong> ${escapeHtml(item.colours.join(", "))}</p>`
    );
  }
  if (item.sizes?.length) {
    lines.push(
      `<p class="preview-variant-summary"><strong>Size:</strong> ${escapeHtml(item.sizes.join(", "))}</p>`
    );
  }

  if (!lines.length) {
    return `<p class="preview-meta-inline">${escapeHtml(item.variantCount)} variant(s)</p>`;
  }

  return `<div class="preview-variant-summary-wrap">${lines.join("")}</div>`;
}

function renderPreviewItem(item, index) {
  const imageHtml = item.imageUrl
    ? `<img class="preview-image" src="${escapeHtml(item.imageUrl)}" alt="" loading="lazy" />`
    : `<div class="preview-image preview-image--empty">No image</div>`;

  return `
    <article class="preview-item">
      <div class="preview-item-main">
        <label class="preview-checkbox">
          <input
            type="checkbox"
            class="preview-select"
            id="preview-select-${index}"
            value="${escapeHtml(item.fullCode || "")}"
            data-product-name="${escapeHtml(item.productName)}"
            checked
          />
        </label>
        ${imageHtml}
        <div class="preview-item-copy">
          <label class="preview-name" for="preview-select-${index}">
            ${escapeHtml(item.productName || "Unnamed product")}
          </label>
          <p class="preview-code">${escapeHtml(item.fullCode || "")}</p>
          <p class="preview-meta-inline">${escapeHtml(item.variantCount)} variant combination(s)</p>
        </div>
      </div>
      ${renderVariantSummary(item)}
    </article>
  `;
}

function renderPreview(data) {
  if (!data.matchCount) {
    previewResultsEl.innerHTML = `
      <div class="preview-summary preview-summary--empty">
        <strong>No Amrod matches</strong>
        <span>Searched ${escapeHtml(data.catalogSize)} product(s) for names containing "${escapeHtml(data.searchName)}".</span>
      </div>
    `;
    previewResultsEl.classList.remove("hidden");
    return;
  }

  const truncatedNote = data.truncated
    ? `<p class="preview-meta">Showing first ${escapeHtml(data.matchCount)} matches. Refine your search for more specific results.</p>`
    : "";

  previewResultsEl.innerHTML = `
    <div class="preview-summary" data-preview-summary>
      Found ${escapeHtml(data.matchCount)} product(s). ${escapeHtml(data.matchCount)} selected for sync.
    </div>
    <div class="preview-toolbar">
      <label class="preview-select-all">
        <input type="checkbox" id="previewSelectAll" checked />
        Select all
      </label>
      <span class="preview-meta">Names containing "${escapeHtml(data.searchName)}" · ${escapeHtml(data.catalogSize)} in catalog</span>
    </div>
    ${truncatedNote}
    <div class="preview-list">${data.matches.map(renderPreviewItem).join("")}</div>
  `;
  previewResultsEl.classList.remove("hidden");

  previewResultsEl.querySelector("#previewSelectAll")?.addEventListener("change", (e) => {
    const checked = e.target.checked;
    previewResultsEl.querySelectorAll(".preview-select").forEach((box) => {
      box.checked = checked;
    });
    updatePreviewSelectionSummary();
  });

  previewResultsEl.querySelectorAll(".preview-select").forEach((box) => {
    box.addEventListener("change", updatePreviewSelectionSummary);
  });
}

previewResultsEl.addEventListener("change", (e) => {
  if (e.target.matches(".preview-select")) {
    updatePreviewSelectionSummary();
  }
});

async function runPreview() {
  hideStatus();
  hidePreview();

  const productName = productNameEl.value.trim();
  if (!productName) {
    showStatus("Enter a product name to search.", "error");
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
    if (!data.matchCount) {
      showStatus("No matches. Try a shorter or different search term.", "error");
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

  const products = getSelectedProducts().filter((p) => p.fullCode || p.productName);
  if (!products.length) {
    showStatus("Preview products first, then select at least one checkbox to sync.", "error");
    return;
  }

  syncBtn.disabled = true;
  previewBtn.disabled = true;
  syncBtn.textContent = `Starting ${products.length} sync(s)…`;

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
      body: JSON.stringify({ products }),
    });

    const data = await res.json();

    if (!res.ok || !data.ok) {
      throw new Error(data.error || `Request failed (${res.status})`);
    }

    const link = data.actionsUrl
      ? `<a href="${data.actionsUrl}" target="_blank" rel="noopener">View on GitHub Actions</a>`
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
