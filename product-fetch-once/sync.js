// sync.js
import fs from "fs";
import path from "path";
import { AMROD_TEST_LIMIT, CATEGORY_SEO_SUFFIX, LOG_DIR } from "./config.js";
import { fetchAmrodToken, fetchAmrodProducts } from "./amrod.js";
import {
  createShopifyProduct,
  updateShopifyVariant,
  createShopifyVariant,
  createShopifyProductImage,
  updateInventoryItemMeasurement,
} from "./shopify.js";
import { mapAmrodToShopifyProduct } from "./mapper.js";
import { logImageFailure, logProductFailure } from "./logger.js";

import { REQUEST_DELAY_MS } from "./config.js";
import { SHOPIFY_STORE, SHOPIFY_TOKEN, SHOPIFY_API_VERSION } from "./config.js";
/* -------------------------
   Helpers
-------------------------- */

function slugify(input) {
  return String(input || "")
    .toLowerCase()
    .trim()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function splitPathSegments(p) {
  return String(p || "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .replace(/\/{2,}/g, "/")
    .split("/")
    .map((s) => s.trim())
    .filter(Boolean);
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function nowStamp() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

function makeLogger() {
  ensureDir(LOG_DIR);
  const stamp = nowStamp();
  const okPath = path.join(LOG_DIR, `sync-ok-${stamp}.jsonl`);
  const failPath = path.join(LOG_DIR, `sync-fail-${stamp}.jsonl`);

  const write = (file, obj) =>
    fs.appendFileSync(file, JSON.stringify(obj) + "\n", "utf8");

  return {
    ok: (o) => write(okPath, { ...o, ts: new Date().toISOString() }),
    fail: (o) => write(failPath, { ...o, ts: new Date().toISOString() }),
    paths: { okPath, failPath },
  };
}

async function runWithConcurrency(items, concurrency, worker) {
  let index = 0;

  const runners = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = index++;
      if (i >= items.length) break;
      await worker(items[i], i);
    }
  });

  await Promise.all(runners);
}

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/?[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncate(str, max = 160) {
  const s = String(str || "").trim();
  if (s.length <= max) return s;
  return s.slice(0, max - 1).trimEnd() + "…";
}

/* -------------------------
   CATEGORY TAGGING
-------------------------- */

// Text-only category tags:
// - EACH segment in category.path independently
// - PLUS SEO tag from category.name
function buildCategoryTags(categories) {
  const tags = new Set();

  for (const c of categories || []) {
    const name = c?.name ?? c?.categoryName;
    const rawPath = c?.path;

    if (rawPath) {
      for (const seg of splitPathSegments(rawPath)) {
        tags.add(seg);
      }
    }

    if (name && String(name).trim()) {
      const seoTag = `${slugify(name)}-${CATEGORY_SEO_SUFFIX}`.replace(/-+/g, "-");
      tags.add(seoTag.slice(0, 255));
    }
  }

  return Array.from(tags);
}

/* -------------------------
   Images
-------------------------- */

function collectGeneralImageUrlsAll(amrod) {
  const urls = new Set();

  for (const img of amrod.images || []) {
    for (const u of img.urls || []) {
      if (u?.url) urls.add(u.url);
    }
  }

  for (const c of amrod.colourImages || []) {
    for (const img of c.images || []) {
      for (const u of img.urls || []) {
        if (u?.url) urls.add(u.url);
      }
    }
  }

  return Array.from(urls);
}

function pickImageUrls(amrod) {
  const mode = String(process.env.IMAGES_MODE || "default+colours").toLowerCase();

  if (mode === "all") return collectGeneralImageUrlsAll(amrod);

  const urls = new Set();

  // product default image (or first)
  const defaultImg =
    (amrod.images || []).find((i) => i.isDefault) || (amrod.images || [])[0];
  const defaultUrl = defaultImg?.urls?.[0]?.url;
  if (defaultUrl) urls.add(defaultUrl);

  if (mode === "default") return Array.from(urls);

  // one default image per colour
  for (const c of amrod.colourImages || []) {
    const def = (c.images || []).find((i) => i.isDefault) || (c.images || [])[0];
    const url = def?.urls?.[0]?.url;
    if (url) urls.add(url);
  }

  return Array.from(urls);
}

function buildColourToImagesMap(amrod) {
  const map = new Map();

  for (const c of amrod.colourImages || []) {
    const key = String(c?.name || c?.code || "").trim().toLowerCase();
    if (!key) continue;

    const def = (c.images || []).find((i) => i.isDefault) || (c.images || [])[0];
    const url = def?.urls?.[0]?.url;
    if (url) map.set(key, [url]);
  }

  return map;
}

/* -------------------------
   Variants
-------------------------- */

function buildDesiredVariants(amrod) {
  const list = Array.isArray(amrod.variants) ? amrod.variants : [];
  if (!list.length) return [];

  const hasColour = list.some((v) => v.codeColourName || v.codeColour);
  const hasSize = list.some((v) => v.codeSizeName || v.codeSize);

  return list.map((v) => {
    const colour = String(v.codeColourName || v.codeColour || "").trim();
    const size = String(v.codeSizeName || v.codeSize || "").trim();

    // Weight for variant shipping (Shopify supports weight on variant).
    // Safety buffer: +0.2kg
    const weightKgRaw =
      Number(v.packagingAndDimension?.cartonWeight ?? v.productDimension?.weight ?? 0) || 0;
    const weightKg = Math.max(0, weightKgRaw + 0.2);

    const out = {
      sku: v.fullCode || v.simpleCode || amrod.fullCode,
      inventory_management: null,
      weight: Number.isFinite(weightKg) ? weightKg : 0,
      weight_unit: "kg",
    };

    out.option1 = hasColour ? colour || "Default" : "Default";
    if (hasSize) out.option2 = size || "One Size";

    return out;
  });
}

/* -------------------------
   Courier Guy compatible "package"
-------------------------- */
/**
 * Shopify carrier apps generally use:
 * - variant weight (we set this)
 * - inventory item measurement (weight + optional shippingPackageId)
 *
 * IMPORTANT:
 * Shopify does NOT let us set raw L/W/H per inventory item directly.
 * Instead, you create Shipping Packages in Shopify Admin (with L/W/H),
 * then we assign one via shippingPackageId.
 *
 * Set this env var if you want to assign a package:
 *   COURIER_GUY_SHIPPING_PACKAGE_ID="gid://shopify/ShippingPackage/12345"
 */
const COURIER_GUY_SHIPPING_PACKAGE_ID =
  process.env.COURIER_GUY_SHIPPING_PACKAGE_ID || null;

// Live image debug
const DEBUG_IMAGES =
  String(process.env.DEBUG_IMAGES || "").toLowerCase() === "true";


/* -------------------------
   SHOPIFY HELPERS
-------------------------- */

const shopifyFetch = async (endpoint, method = "GET", body) => {
  const url = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/${endpoint}`;

  const res = await fetch(url, {
    method,
    headers: {
      "X-Shopify-Access-Token": SHOPIFY_TOKEN,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!res.ok) {
    throw new Error(`Shopify error ${res.status}: ${await res.text()}`);
  }

  return await res.json();
};


const setInventoryLevel = async (inventoryItemId, locationId, available) => {
  const body = {location_id: locationId, inventory_item_id: inventoryItemId, available: available}
  const res = await shopifyFetch(`inventory_levels/set.json`, "POST", body)

  return res
}

/* -------------------------
   MAIN SYNC
-------------------------- */

export const syncAllProducts = async () => {
  const logger = makeLogger();
  console.log(`🧾 Logging to:\n- ${logger.paths.okPath}\n- ${logger.paths.failPath}`);

  let products;
  try {
    const token = await fetchAmrodToken();
    products = await fetchAmrodProducts(token);
  } catch (e) {
    console.error("❌ Amrod fetch failed:", e?.message || e);
    throw e;
  }

  if (AMROD_TEST_LIMIT) {
    products = products.slice(0, AMROD_TEST_LIMIT);
  }

  const LOCATION_IDS = process.env.SHOPIFY_LOCATION_IDS?.split(',')
  // --- Sharding for GitHub Actions matrix ---
  const SHARD_COUNT = Number(process.env.SHARD_COUNT || 1);
  const SHARD_INDEX = Number(process.env.SHARD_INDEX || 0);

  if (SHARD_COUNT > 1) {
    products = products.filter((_, idx) => idx % SHARD_COUNT === SHARD_INDEX);
  }

  const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
  const IMAGES_MODE = String(process.env.IMAGES_MODE || "default+colours");

  console.log(
    `⚡ Speed settings: CONCURRENCY=${CONCURRENCY} IMAGES_MODE=${IMAGES_MODE} SHARD_INDEX=${SHARD_INDEX}/${SHARD_COUNT}`
  );

  let done = 0;
  const total = products.length;
  const start = Date.now();

  await runWithConcurrency(products, CONCURRENCY, async (product) => {
    const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN_CODE";

    try {
      const categoryTags = buildCategoryTags(product.categories || []);

      // 1) Build product payload
      const payload = mapAmrodToShopifyProduct(product, categoryTags);

      // --- SEO (best-practice place: right here, before create) ---
      const handleBase = slugify(product.productName || amrodCode);
      payload.product.handle = `${handleBase}-${slugify(amrodCode)}`.slice(0, 255);

      const seoTitle = truncate(`${product.productName || amrodCode} | Amrod`, 70);
      const seoDesc = truncate(stripHtml(product.description), 160);

      payload.product.metafields = payload.product.metafields || [];
      payload.product.metafields.push(
        {
          namespace: "custom",
          key: "seo_title",
          value: seoTitle,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "seo_description",
          value: seoDesc,
          type: "multi_line_text_field",
        }
      );

      // 2) Create product
      const shopifyProduct = await createShopifyProduct(payload, product);

      const productId = shopifyProduct.id;
      const defaultVariantId = shopifyProduct.variants?.[0]?.id;

      // 3) Variants
      const desiredVariants = buildDesiredVariants(product);
      let variants = [];

      if (!desiredVariants.length) {
        if (defaultVariantId) {
          const v = await updateShopifyVariant(defaultVariantId, {
            sku: amrodCode,
            option1: "Default",
            weight: 0.2,
            weight_unit: "kg",
          });
          variants = [v];
        }
      } else {
        const first = await updateShopifyVariant(defaultVariantId, desiredVariants[0]);
        variants.push(first);

        for (let i = 1; i < desiredVariants.length; i++) {
          variants.push(await createShopifyVariant(productId, desiredVariants[i]));
        }
      }

      // 3.5) Inventory measurements (Courier Guy compatibility)
      const amrodVariantList = Array.isArray(product.variants) ? product.variants : [];
      const amrodBySku = new Map();
      for (const av of amrodVariantList) {
        const sku = av.fullCode || av.simpleCode || product.fullCode || null;
        if (sku) amrodBySku.set(sku, av);
      }

      for (const v of variants) {
        try {
          const inventoryItemId = v.inventory_item_id;
          if (!inventoryItemId) {
            logger.fail({
              amrodCode,
              productId,
              step: "inventory_measurement",
              variantId: v.id,
              error: "Missing inventory_item_id on variant response",
            });
            continue;
          }

          const sku = v.sku || null;
          const av = sku ? amrodBySku.get(sku) : null;

          const weightKgRaw =
            Number(
              av?.packagingAndDimension?.cartonWeight ??
                av?.productDimension?.weight ??
                v.weight ??
                0
            ) || 0;
          const weightKg = Math.max(0, weightKgRaw + 0.2);

          await updateInventoryItemMeasurement({
            inventoryItemId,
            weightKg,
            shippingPackageId: COURIER_GUY_SHIPPING_PACKAGE_ID || null,
          });

          if (LOCATION_IDS) {
            for (const LOCATION_ID of LOCATION_IDS) {
              if (!LOCATION_ID) continue;


            try {
              await setInventoryLevel(inventoryItemId, LOCATION_ID, 10)
            } catch (e) {
              console.log("location update failed")
              console.error(e)
            }

          }
          await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY_MS));
        }



        } catch (e) {
          logger.fail({
            amrodCode,
            productId,
            step: "inventory_measurement",
            variantId: v.id,
            error: String(e?.message || e),
          });
        }
      }

      // 4) Images (best effort)
      const colourMap = buildColourToImagesMap(product);
      const allImages = pickImageUrls(product);

      const colourToVariantIds = new Map();
      for (const v of variants) {
        const c = String(v.option1 || "").toLowerCase();
        if (!c) continue;
        const arr = colourToVariantIds.get(c) || [];
        arr.push(v.id);
        colourToVariantIds.set(c, arr);
      }

      for (const url of allImages) {
        try {
          if (DEBUG_IMAGES) {
            console.log(`🖼️ Uploading image: ${amrodCode} -> ${url}`);
          }

          let variant_ids = [];

          for (const [colour, urls] of colourMap.entries()) {
            if (urls.includes(url)) {
              variant_ids = colourToVariantIds.get(colour) || [];
              break;
            }
          }

          await createShopifyProductImage(
            productId,
            variant_ids.length ? { src: url, variant_ids } : { src: url }
          );
        } catch (e) {
          // Live log in GitHub Actions UI
          console.log(
            `::warning title=Image upload failed::${amrodCode} | ${url} | ${String(
              e?.message || e
            )}`
          );

          logger.fail({
            amrodCode,
            productId,
            step: "image",
            imageUrl: url,
            error: String(e?.message || e),
          });

          logImageFailure({
            amrod: product,
            error: e,
            extra: { productId, imageUrl: url, step: "createShopifyProductImage" },
          });
        }
      }

      logger.ok({
        amrodCode,
        productId,
        step: "complete",
        variants: variants.length,
        tags: categoryTags.length,
      });
    } catch (err) {
      logger.fail({
        amrodCode,
        step: "product",
        error: String(err?.message || err),
      });

      logProductFailure({
        amrod: product,
        stage: "syncAllProducts",
        error: err,
        extra: { amrodCode },
      });
    } finally {
      const finished = ++done;
      if (finished % 100 === 0 || finished === total) {
        const elapsedSec = (Date.now() - start) / 1000;
        const rate = finished / Math.max(elapsedSec, 1);
        const remainingSec = (total - finished) / Math.max(rate, 0.001);
        console.log(
          `📦 Progress: ${finished}/${total} | ${rate.toFixed(
            2
          )} prod/s | ETA ~ ${(remainingSec / 3600).toFixed(2)}h`
        );
      }
    }
  });
};
