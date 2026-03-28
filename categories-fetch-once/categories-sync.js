// categories-sync.js
import { getOrCreateCollection } from "./collections.js";
import { sleep } from "./helpers.js";

/**
 * Creates/ensures collections for ALL categories, builds a FULL tree (unlimited depth),
 * AND returns a top-level-only menu array (parents only).
 *
 * Return shape:
 * {
 *   menuItems: [MenuItemInput...],  // ONLY top-level parents (no children)
 *   fullTree:  [TreeNode...],       // full nested tree for metafield storage
 * }
 */
export async function buildMenuParentsAndFullTree(categories, path = []) {
  const fullTree = [];

  for (const cat of categories) {
    const currentPath = [...path, cat.categoryName];
    console.log(`➡️ ${currentPath.join(" > ")}`);

    try {
      const collection = await getOrCreateCollection(cat);
      if (!collection?.id) {
        console.warn(`⚠️ Collection not created: ${cat.categoryName}`);
        continue;
      }

      const node = {
        title: collection.title,
        handle: collection.handle,
        collectionGid: collection.id,
        url: `/collections/${collection.handle}`,
        // include original Amrod id if you want to map back later
        amrodCategoryId: cat.id,
        children: [],
      };

      const children = Array.isArray(cat.children) ? cat.children : [];
      if (children.length > 0) {
        node.children = (
          await buildMenuParentsAndFullTree(children, currentPath)
        ).fullTree;
      }

      fullTree.push(node);
      await sleep(120);
    } catch (err) {
      console.error(
        `❌ Failed category ${cat.categoryName}:`,
        err instanceof Error ? err.message : JSON.stringify(err)
      );
    }
  }

  // menuItems = ONLY level-1 items, no nesting
  const menuItems = fullTree.map((n) => ({
    title: n.title,
    type: "COLLECTION",
    resourceId: n.collectionGid,
    url: n.url,
    items: [], // important: no nesting at all
  }));

  return { menuItems, fullTree };
}
