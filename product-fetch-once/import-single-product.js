// import-single-product.js — shared create pipeline (one Amrod product → Shopify)
import { CATEGORY_SEO_SUFFIX, REQUEST_DELAY_MS } from "./config.js";
import {
  createShopifyProduct,
  updateShopifyVariant,
  createShopifyVariant,
  createShopifyProductImage,
  updateInventoryItemMeasurement,
  setInventoryLevel,
} from "./shopify.js";
import { mapAmrodToShopifyProduct } from "./mapper.js";
import { logImageFailure, logProductFailure } from "./logger.js";

const COURIER_GUY_SHIPPING_PACKAGE_ID =
  process.env.COURIER_GUY_SHIPPING_PACKAGE_ID || null;

const DEBUG_IMAGES =
  String(process.env.DEBUG_IMAGES || "").toLowerCase() === "true";

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

  const defaultImg =
    (amrod.images || []).find((i) => i.isDefault) || (amrod.images || [])[0];
  const defaultUrl = defaultImg?.urls?.[0]?.url;
  if (defaultUrl) urls.add(defaultUrl);

  if (mode === "default") return Array.from(urls);

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

function buildDesiredVariants(amrod) {
  const list = Array.isArray(amrod.variants) ? amrod.variants : [];
  if (!list.length) return [];

  const hasColour = list.some((v) => v.codeColourName || v.codeColour);
  const hasSize = list.some((v) => v.codeSizeName || v.codeSize);

  return list.map((v) => {
    const colour = String(v.codeColourName || v.codeColour || "").trim();
    const size = String(v.codeSizeName || v.codeSize || "").trim();

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

/**
 * Full import: create Shopify product + variants + inventory measurement + images.
 * Caller is responsible for deleting any existing product first when updating.
 */
export async function runSingleProductImportPipeline(product, logger) {
  const amrodCode = product.fullCode || product.simpleCode || "UNKNOWN_CODE";
  const LOCATION_IDS = process.env.SHOPIFY_LOCATION_IDS?.split(",");

  const categoryTags = buildCategoryTags(product.categories || []);

  const payload = mapAmrodToShopifyProduct(product, categoryTags);

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

  const shopifyProduct = await createShopifyProduct(payload, product);

  const productId = shopifyProduct.id;
  const defaultVariantId = shopifyProduct.variants?.[0]?.id;

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
            await setInventoryLevel(inventoryItemId, LOCATION_ID, 10);
          } catch (e) {
            console.log("location update failed");
            console.error(e);
          }
        }
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

  if (variants.length && REQUEST_DELAY_MS > 0) {
    await new Promise((resolve) => setTimeout(resolve, REQUEST_DELAY_MS));
  }

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
}
