#!/usr/bin/env node
/**
 * If Categories/GetUpdated reports changes, run full Categories/ → menu + metafield sync.
 * Independent of product/price/stock daily job.
 */
import { fileURLToPath } from 'url';
import { fetchAmrodToken, fetchAmrodCategories, fetchUpdatedCategories } from './amrod.js';
import { upsertMenu, setShopJsonMetafield } from './shopify.js';
import { buildMenuParentsAndFullTree } from './categories-sync.js';

export async function runCategoriesIncrementalGateThenFullSync() {
  console.log('🔑 Fetching Amrod token...');
  const token = await fetchAmrodToken();

  console.log('📭 Categories GetUpdated...');
  const delta = await fetchUpdatedCategories(token);

  if (!Array.isArray(delta) || delta.length === 0) {
    console.log('✅ No category changes from Amrod (empty GetUpdated)');
    return;
  }

  console.log(`📬 GetUpdated: ${delta.length} row(s) — running full Categories sync`);

  const categories = await fetchAmrodCategories(token);

  console.log('🧱 Building FULL tree + ensuring collections exist...');
  const { menuItems, fullTree } = await buildMenuParentsAndFullTree(categories);

  console.log('🧭 Creating/updating Shopify menu (parents only)...');
  const menu = await upsertMenu({
    title: 'Main Menu',
    handle: 'main-menu',
    items: menuItems,
  });

  console.log('🧾 Saving full category tree to shop metafield...');
  await setShopJsonMetafield({
    namespace: 'amrod',
    key: 'category_tree',
    jsonValue: {
      generatedAt: new Date().toISOString(),
      menuHandle: 'main-menu',
      tree: fullTree,
    },
  });

  console.log(`✅ Menu upserted: ${menu.title} (${menu.handle})`);
}

if (fileURLToPath(import.meta.url) === process.argv[1]) {
  runCategoriesIncrementalGateThenFullSync().catch((err) => {
    console.error('🔥 Category incremental sync failed:');
    console.error(err);
    console.error(err?.message);
    process.exit(1);
  });
}
