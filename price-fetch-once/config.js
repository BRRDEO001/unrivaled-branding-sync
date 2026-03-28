// config.js
export const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
export const SHOPIFY_TOKEN = process.env.SHOPIFY_TOKEN;
export const SHOPIFY_API_VERSION = "2026-01";

export const SHOPIFY_GRAPHQL_ENDPOINT = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

export const AMROD_AUTH_ENDPOINT = "https://identity.amrod.co.za/VendorLogin";
export const AMROD_PRICES_ENDPOINT = "https://vendorapi.amrod.co.za/api/v1/Prices/";

export const AMROD_AUTH_DETAILS = {
  Username: process.env.AMROD_USERNAME,
  Password: process.env.AMROD_PASSWORD,
  CustomerCode: process.env.AMROD_CUSTOMER_CODE,
};
