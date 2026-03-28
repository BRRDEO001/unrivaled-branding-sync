import { getOrCreateCollection } from './shopify.js';

export async function syncCategoriesRecursively(
  categories,
  parentCollectionId = null,
  path = []
) {
  for (const category of categories) {
    const currentPath = [...path, category.categoryName];
    console.log(`➡️ ${currentPath.join(' > ')}`);

    const collectionId = await getOrCreateCollection(
      category,
      parentCollectionId
    );

    if (category.children && category.children.length > 0) {
      await syncCategoriesRecursively(
        category.children,
        collectionId,
        currentPath
      );
    }
  }
}
