// config.js
export const AMROD_AUTH_ENDPOINT = "https://identity.amrod.co.za/VendorLogin";
export const AMROD_PRODUCTS_ENDPOINT = "https://vendorapi.amrod.co.za/api/v1/Products/GetProductsAndBranding";

export const AMROD_AUTH_DETAILS = {
  Username: process.env.AMROD_USERNAME,
  Password: process.env.AMROD_PASSWORD,
  CustomerCode: process.env.AMROD_CUSTOMER_CODE,
};

export const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
export const SHOPIFY_TOKEN = process.env.SHOPIFY_TOKEN;

// Shopify stable versions are quarterly; 2026-01 is valid in docs. :contentReference[oaicite:1]{index=1}
export const SHOPIFY_API_VERSION = "2026-01";

export const AMROD_TEST_LIMIT = process.env.AMROD_TEST_LIMIT
  ? Number(process.env.AMROD_TEST_LIMIT)
  : null;

export const REQUEST_DELAY_MS = 600;

// EXACT suffix you requested:
export const CATEGORY_SEO_SUFFIX =
  "unrivaled-branding-corporate-printing-products";

// Logs folder (optional override)
export const LOG_DIR = process.env.LOG_DIR || "logs";
