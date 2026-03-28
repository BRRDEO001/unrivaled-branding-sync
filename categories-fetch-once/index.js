// index.js
import { fetchAmrodToken, fetchAmrodCategories } from "./amrod.js";
import { upsertMenu, setShopJsonMetafield } from "./shopify.js";
import { buildMenuParentsAndFullTree } from "./categories-sync.js";

(async () => {
  try {
    console.log("🔑 Fetching Amrod token...");
    const token = await fetchAmrodToken();

    console.log("📦 Fetching Amrod categories...");
    const categories = await fetchAmrodCategories(token);

    console.log("🧱 Building FULL tree + ensuring collections exist...");
    const { menuItems, fullTree } = await buildMenuParentsAndFullTree(categories);

    console.log("🧭 Creating/updating Shopify menu (parents only)...");
    const menu = await upsertMenu({
      title: "Main Menu",
      handle: "main-menu",
      items: menuItems,
    });

    console.log("🧾 Saving full category tree to shop metafield...");
    await setShopJsonMetafield({
      namespace: "amrod",
      key: "category_tree",
      jsonValue: {
        generatedAt: new Date().toISOString(),
        menuHandle: "main-menu",
        tree: fullTree,
      },
    });

    console.log(`✅ Menu upserted: ${menu.title} (${menu.handle})`);
    console.log("🎉 Category sync complete");
  } catch (err) {
    console.error("🔥 Category sync failed:");
    console.error(err);
    console.error(err?.message);
    console.error(err?.stack);
    process.exit(1);
  }
})();
