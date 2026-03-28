// mapper.js

/**
 * Detect if any variant has colour info
 */
function hasAnyColour(amrod) {
  return (
    Array.isArray(amrod?.variants) &&
    amrod.variants.some((v) => v.codeColourName || v.codeColour)
  );
}

/**
 * Detect if any variant has size info
 */
function hasAnySize(amrod) {
  return (
    Array.isArray(amrod?.variants) &&
    amrod.variants.some((v) => v.codeSizeName || v.codeSize)
  );
}

/**
 * Build branding locations → custom.branding
 * ⚠️ VERY IMPORTANT: Do not change branding import structure.
 */
function buildBrandingLocations(amrod) {
  if (!Array.isArray(amrod?.brandings)) return [];

  const required = amrod.requiredBrandingPositions ?? [];

  return amrod.brandings.map((pos) => ({
    positionCode: pos.positionCode,
    positionName: pos.positionName,
    positionMultiplier: Number(pos.positionMultiplier ?? 1),
    required: required.includes(pos.positionCode),
    comment: pos.positionComment ?? null,
  }));
}

/**
 * Build branding options grouped by position → amrod.branding_options
 * ⚠️ VERY IMPORTANT: Do not change branding import structure.
 */
function buildBrandingOptions(amrod) {
  if (!Array.isArray(amrod?.brandings)) return {};

  const options = {};

  for (const pos of amrod.brandings) {
    options[pos.positionCode] = (pos.method ?? []).map((m) => ({
      brandingCode: m.brandingCode,
      brandingName: m.brandingName,
      brandingDepartment: m.brandingDepartment ?? null,
      brandingInclusiveMethod: !!m.brandingInclusiveMethod,
      displayIndex: m.displayIndex ?? null,
      maxPrintingSizeWidth: m.maxPrintingSizeWidth
        ? Number(m.maxPrintingSizeWidth)
        : null,
      maxPrintingSizeHeight: m.maxPrintingSizeHeight
        ? Number(m.maxPrintingSizeHeight)
        : null,
      numberOfColours: m.numberOfColours ?? null,
      brandingMultiplier: m.brandingMultiplier
        ? Number(m.brandingMultiplier)
        : 1,
      exclusions: m.exclusions ?? [],
    }));
  }

  return options;
}

/**
 * Extract category tags from categories[].path
 * Example: "display/outdoor products/skins/..." -> ["display", "outdoor products", "skins", ...]
 */
function buildCategoryTags(amrod) {
  if (!Array.isArray(amrod?.categories)) return [];

  const parts = [];
  for (const cat of amrod.categories) {
    if (!cat?.path) continue;
    const split = String(cat.path)
      .split("/")
      .map((s) => String(s || "").trim())
      .filter(Boolean);
    parts.push(...split);
  }

  // de-dupe, preserve original casing from Amrod path (Shopify tags are case-sensitive visually)
  return Array.from(new Set(parts));
}

/**
 * Stable, deterministic tags string:
 * - trimmed
 * - de-duped (case-insensitive)
 * - sorted (case-insensitive)
 * - joined by ", " (Shopify REST accepts comma-separated string)
 */
function normalizeTags(tags) {
  if (!Array.isArray(tags)) return "";

  const clean = tags.map((t) => String(t || "").trim()).filter(Boolean);

  // de-dupe case-insensitive but keep the first-seen original casing
  const seen = new Map(); // lower -> original
  for (const t of clean) {
    const key = t.toLowerCase();
    if (!seen.has(key)) seen.set(key, t);
  }

  const unique = Array.from(seen.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([, original]) => original);

  return unique.join(", ");
}

/**
 * Pick best (largest) image URL from Amrod image urls array
 */
function pickBestImageUrl(urls) {
  if (!Array.isArray(urls) || urls.length === 0) return null;
  const sorted = [...urls].sort(
    (a, b) => Number(b?.width ?? 0) - Number(a?.width ?? 0)
  );
  return sorted[0]?.url ?? null;
}

/**
 * Build Shopify images payload from amrod.images
 * If you truly import images elsewhere, you can just ignore `product.images` downstream.
 */
function buildImages(amrod) {
  if (!Array.isArray(amrod?.images)) return [];

  const srcs = [];
  for (const img of amrod.images) {
    const best = pickBestImageUrl(img?.urls);
    if (best) srcs.push(best);
  }

  // de-dupe
  const unique = Array.from(new Set(srcs));

  // Shopify REST expects [{src}]
  return unique.map((src) => ({ src }));
}

/**
 * Build variants array (Shopify REST create product payload)
 * - sku from variant.fullCode (falls back to product fullCode)
 * - weight stored on variant (Courier Guy typically reads Shopify variant weight)
 * - dimensions stored in product metafield (shipping.variant_dimensions) keyed by SKU
 *
 * ✅ Weight is ALWAYS written as grams ("g") in Shopify
 */
function buildVariants(amrod, useColour, useSize) {
  const list =
    Array.isArray(amrod?.variants) && amrod.variants.length
      ? amrod.variants
      : [
          {
            fullCode: amrod?.fullCode ?? amrod?.simpleCode ?? null,
            codeColourName: null,
            codeSizeName: null,
            productDimension: null,
            packagingAndDimension: null,
          },
        ];

  const variants = [];
  for (const v of list) {
    const sku = v?.fullCode ?? amrod?.fullCode ?? amrod?.simpleCode ?? "";

    // Endpoint returns a number; treat it as grams and always write unit "g"
    const weightG =
      v?.productDimension?.weight != null ? Number(v.productDimension.weight) : null;

    const variant = {
      sku,
      // If you don't track inventory quantities, typically you leave inventory_management null
      // inventory_management: null,
      requires_shipping: true,
    };

    if (Number.isFinite(weightG)) {
      variant.weight = weightG;
      variant.weight_unit = "g";
    }

    // Options (only if the product actually uses them)
    // If a value is missing but the option exists, we still need something deterministic
    if (useColour) {
      variant.option1 = v?.codeColourName || v?.codeColour || "Default";
    }
    if (useSize) {
      // option index depends on whether Colour is present
      if (useColour) variant.option2 = v?.codeSizeName || v?.codeSize || "Default";
      else variant.option1 = v?.codeSizeName || v?.codeSize || "Default";
    }

    variants.push(variant);
  }

  return variants;
}

/**
 * Build a CourierGuy-friendly dimensions map in a PRODUCT metafield
 * keyed by SKU, since REST product create can't reliably attach variant metafields inline.
 *
 * We store:
 * - weight_g (ALWAYS grams)
 * - length / width / height (raw numbers from Amrod)
 * - dimension_units: "unknown" (you can change later once you confirm Amrod units)
 */
function buildVariantDimensionsMap(amrod) {
  const map = {};

  const variants =
    Array.isArray(amrod?.variants) && amrod.variants.length ? amrod.variants : [];

  for (const v of variants) {
    const sku = v?.fullCode ?? amrod?.fullCode ?? amrod?.simpleCode ?? "";
    if (!sku) continue;

    const pd = v?.productDimension ?? {};
    const pack = v?.packagingAndDimension ?? {};

    // productDimension has length/width/weight in your example
    const length = pd.length != null ? Number(pd.length) : null;
    const width = pd.width != null ? Number(pd.width) : null;

    // height isn't in productDimension in your example; carton height exists in packagingAndDimension
    const height =
      pack.cartonSizeDimensionH != null ? Number(pack.cartonSizeDimensionH) : null;

    // Endpoint returns a number; treat it as grams
    const weight_g = pd.weight != null ? Number(pd.weight) : null;

    map[sku] = {
      weight_g: Number.isFinite(weight_g) ? weight_g : null,
      length: Number.isFinite(length) ? length : null,
      width: Number.isFinite(width) ? width : null,
      height: Number.isFinite(height) ? height : null,
      dimension_units: "unknown",
    };
  }

  return map;
}

/**
 * Main mapper
 */
export const mapAmrodToShopifyProduct = (amrod, tags = []) => {
  const useColour = hasAnyColour(amrod);
  const useSize = hasAnySize(amrod);

  const options = [];
  if (useColour) options.push({ name: "Color" });
  if (useSize) options.push({ name: "Size" });

  // ✅ Category path tags
  const categoryTags = buildCategoryTags(amrod);

  // ✅ Merge tags passed in + category tags + (optional) keywords/tags from API
  const mergedTags = [...(Array.isArray(tags) ? tags : []), ...categoryTags];

  const brandingLocations = buildBrandingLocations(amrod); // ✅ unchanged structure
  const brandingOptions = buildBrandingOptions(amrod); // ✅ unchanged structure

  // ✅ Variants (SKU comes from variant.fullCode)
  const variants = buildVariants(amrod, useColour, useSize);

  // ✅ Dimensions map for shipping apps (Courier Guy)
  const variantDimensionsMap = buildVariantDimensionsMap(amrod);

  return {
    product: {
      // ✅ product name + description
      title: amrod.productName,
      body_html: amrod.description,

      vendor: "Amrod",
      status: "active",

      // ✅ Ensure product is available on Online Store (and other channels that respect this)
      published_scope: "global",

      // 🚫 Disable VAT / tax for this product
      taxable: false,

      // ✅ Tags: includes category-path parts
      tags: normalizeTags(mergedTags),

      // ✅ Images (if you import elsewhere, you can ignore/override this downstream)
      images: buildImages(amrod),

      // ✅ Prevent default "Title" option
      ...(options.length ? { options } : {}),

      // ✅ Create variants
      variants,

      metafields: [
        // Keep: product full code stored (useful for reference)
        {
          namespace: "supplier",
          key: "amrod_full_code",
          value: amrod.fullCode,
          type: "single_line_text_field",
        },

        // ✅ Branding import unchanged
        {
          namespace: "custom",
          key: "branding",
          value: JSON.stringify(brandingLocations),
          type: "json",
        },
        {
          namespace: "amrod",
          key: "branding_options",
          value: JSON.stringify(brandingOptions),
          type: "json",
        },

        // ✅ Required branding positions captured explicitly (UI can read this directly if needed)
        {
          namespace: "amrod",
          key: "required_branding_positions",
          value: JSON.stringify(amrod.requiredBrandingPositions ?? []),
          type: "json",
        },

        // ✅ Shipping dimensions map for Courier Guy (keyed by SKU)
        {
          namespace: "shipping",
          key: "variant_dimensions",
          value: JSON.stringify(variantDimensionsMap),
          type: "json",
        },
      ],
    },
  };
};
