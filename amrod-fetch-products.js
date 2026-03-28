#!/usr/bin/env node

/* =====================================================
   CONFIG
===================================================== */

// -------- AMROD --------
const AMROD_AUTH_ENDPOINT = 'https://identity.amrod.co.za/VendorLogin';
const AMROD_PRODUCTS_ENDPOINT = 'https://vendorapi.amrod.co.za/api/v1/Products/';

const AMROD_AUTH_DETAILS = {
  Username: process.env.AMROD_USERNAME,
  Password: process.env.AMROD_PASSWORD,
  CustomerCode: process.env.AMROD_CUSTOMER_CODE
};

// -------- SHOPIFY --------
const SHOPIFY_STORE = process.env.SHOPIFY_STORE; // your-store.myshopify.com
const SHOPIFY_TOKEN = process.env.SHOPIFY_TOKEN; // offline token from Postman/GitHub Secrets
const SHOPIFY_API_VERSION = '2024-01';

// -------- TEST / SAFETY --------
const AMROD_TEST_LIMIT = process.env.AMROD_TEST_LIMIT
  ? Number(process.env.AMROD_TEST_LIMIT)
  : null;

const REQUEST_DELAY_MS = 600;

/* =====================================================
   HELPERS
===================================================== */

const sleep = ms => new Promise(r => setTimeout(r, ms));

const shopifyFetch = async (endpoint, method = 'GET', body) => {
  if (!SHOPIFY_TOKEN) {
    throw new Error('SHOPIFY_TOKEN not set in environment variables');
  }

  const res = await fetch(
    `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/${endpoint}`,
    {
      method,
      headers: {
        'X-Shopify-Access-Token': SHOPIFY_TOKEN,
        'Content-Type': 'application/json'
      },
      body: body ? JSON.stringify(body) : undefined
    }
  );

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Shopify error ${res.status}: ${text}`);
  }

  return res.json();
};

/* =====================================================
   AMROD
===================================================== */

const fetchAmrodToken = async () => {
  console.log('🔐 Authenticating with Amrod...');

  const res = await fetch(AMROD_AUTH_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(AMROD_AUTH_DETAILS)
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Amrod Auth failed (${res.status}): ${text}`);
  }

  const data = await res.json();

  if (!data.token) {
    throw new Error('Token not found in Amrod auth response');
  }

  console.log('✅ Amrod token received');
  return data.token;
};

const fetchAmrodProducts = async (token) => {
  console.log('📦 Fetching Amrod products...');
  const res = await fetch(AMROD_PRODUCTS_ENDPOINT, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch products (${res.status})`);
  }

  const products = await res.json();
  console.log(`✅ Retrieved ${products.length} products`);
  return products;
};

/* =====================================================
   SHOPIFY COLLECTIONS
===================================================== */

const getOrCreateCollection = async (category) => {
  const search = await shopifyFetch(
    `custom_collections.json?title=${encodeURIComponent(category.name)}`
  );

  if (search.custom_collections.length > 0) {
    return search.custom_collections[0].id;
  }

  console.log(`📁 Creating collection: ${category.name}`);
  const created = await shopifyFetch(
    'custom_collections.json',
    'POST',
    { custom_collection: { title: category.name, image: category.image ? { src: category.image } : undefined } }
  );

  await sleep(REQUEST_DELAY_MS);
  return created.custom_collection.id;
};

const addProductToCollection = async (productId, collectionId) => {
  await shopifyFetch('collects.json', 'POST', {
    collect: { product_id: productId, collection_id: collectionId }
  });
  await sleep(REQUEST_DELAY_MS);
};

/* =====================================================
   SHOPIFY PRODUCT MAPPING
===================================================== */

const mapAmrodToShopifyProduct = (amrod) => ({
  product: {
    title: amrod.productName,
    body_html: amrod.description,
    vendor: 'Amrod',
    status: 'active',
    images: amrod.images.flatMap(img => img.urls.map(u => ({ src: u.url }))),
    variants: amrod.variants.map(v => ({
      sku: v.fullCode,
      inventory_management: 'shopify',
      weight: v.productDimension?.weight || 0,
      weight_unit: 'kg'
    })),
    metafields: [
      {
        namespace: 'supplier',
        key: 'amrod_full_code',
        value: amrod.fullCode,
        type: 'single_line_text_field'
      }
    ]
  }
});

const createShopifyProduct = async (amrodProduct) => {
  console.log(`🛍️ Creating product: ${amrodProduct.fullCode}`);
  const payload = mapAmrodToShopifyProduct(amrodProduct);
  const res = await shopifyFetch('products.json', 'POST', payload);
  await sleep(REQUEST_DELAY_MS);
  return res.product;
};

/* =====================================================
   MAIN SYNC
===================================================== */

const syncAllProducts = async () => {
  const token = await fetchAmrodToken();
  let products = await fetchAmrodProducts(token);

  if (AMROD_TEST_LIMIT && AMROD_TEST_LIMIT > 0) {
    products = products.slice(0, AMROD_TEST_LIMIT);
    console.log(`🧪 TEST MODE ENABLED — syncing ${products.length} products only`);
  }

  for (const product of products) {
    try {
      const shopifyProduct = await createShopifyProduct(product);

      for (const category of product.categories || []) {
        const collectionId = await getOrCreateCollection(category);
        await addProductToCollection(shopifyProduct.id, collectionId);
      }

      console.log(`✅ Synced ${product.fullCode}`);
    } catch (err) {
      console.error(`❌ Failed product ${product.fullCode}:`, err.message);
    }
  }
};

/* =====================================================
   ENTRY POINT
===================================================== */

(async () => {
  try {
    if (!SHOPIFY_TOKEN) throw new Error('SHOPIFY_TOKEN not set in environment variables');
    await syncAllProducts();
    console.log('🎉 Sync complete');
  } catch (err) {
    console.error('🔥 Sync failed:', err.message);
    console.error('Stack:', err.stack);
    console.log('📝 AMROD Auth Details:', {
      Username: AMROD_AUTH_DETAILS.Username,
      CustomerCode: AMROD_AUTH_DETAILS.CustomerCode,
      Password: '********'
    });
    process.exit(1);
  }
})();
