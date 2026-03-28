// tree-map-import/config.js

export const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
export const SHOPIFY_TOKEN = process.env.SHOPIFY_TOKEN;

// ✅ Needed by GraphQL client
export const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";

export const AMROD_AUTH_DETAILS = {
  Username: process.env.AMROD_USERNAME,
  Password: process.env.AMROD_PASSWORD,
  CustomerCode: process.env.AMROD_CUSTOMER_CODE,
};

export const AMROD_AUTH_ENDPOINT = "https://identity.amrod.co.za/VendorLogin";
export const AMROD_CATEGORIES_ENDPOINT =
  "https://vendorapi.amrod.co.za/api/v1/Categories/";

export const REQUEST_DELAY_MS = 600;
export const SHOPIFY_MENU_HANDLE = "main-menu";

// Optional log dir if you want later
export const LOG_DIR = "logs";
