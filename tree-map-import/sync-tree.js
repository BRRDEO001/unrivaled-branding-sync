// tree-map-import/sync-tree.js
import { fetchAmrodToken, fetchAmrodCategories } from "./amrod-categories.js";
import { graphqlFetch } from "./shopify-graphql.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

// Deterministic handle from segments so each node is uniquely addressed.
// Example: ["corporate gifts","specials","bags"] => "corporate-gifts--specials--bags"
function buildHandleFromSegments(segs) {
  return segs.map(slugify).join("--").slice(0, 255);
}

function flattenCategories(nodes) {
  const out = [];
  const walk = (arr) => {
    for (const n of arr || []) {
      out.push(n);
      if (Array.isArray(n.children) && n.children.length) walk(n.children);
    }
  };
  walk(nodes);
  return out;
}

function getField(node, key) {
  const f = (node?.fields || []).find((x) => x.key === key);
  return f?.value ?? null;
}

async function findNodeByHandle(type, handle) {
  const query = `
    query FindByHandle($h: MetaobjectHandleInput!) {
      metaobjectByHandle(handle: $h) {
        id
        handle
        fields { key value }
      }
    }
  `;

  const data = await graphqlFetch(query, {
    h: { type, handle },
  });

  return data?.metaobjectByHandle ?? null;
}

async function upsertNode({
  type,
  handle,
  title,
  slug,
  pathValue,
  depth,
  parentId,
  sourceId,
  imageUrl,
  sortOrder,
}) {
  const existing = await findNodeByHandle(type, handle);

  const mutationCreate = `
    mutation CreateNode($metaobject: MetaobjectCreateInput!) {
      metaobjectCreate(metaobject: $metaobject) {
        metaobject { id handle }
        userErrors { field message }
      }
    }
  `;

  // ✅ Correct signature (needs explicit id argument)
  const mutationUpdate = `
    mutation UpdateNode($id: ID!, $metaobject: MetaobjectUpdateInput!) {
      metaobjectUpdate(id: $id, metaobject: $metaobject) {
        metaobject { id handle }
        userErrors { field message }
      }
    }
  `;

  // These keys must match your metaobject definition field keys
  const fields = [
    { key: "title", value: title },
    { key: "slug", value: slug },
    { key: "path", value: pathValue },
    { key: "depth", value: String(depth) },
  ];

  // parent is a metaobject reference field: value must be the parent's GID
  if (parentId) fields.push({ key: "parent", value: parentId });

  // Optional fields — only if you created them in Shopify
  if (sourceId !== undefined && sourceId !== null)
    fields.push({ key: "source_id", value: String(sourceId) });

  if (imageUrl) fields.push({ key: "image_url", value: String(imageUrl) });

  if (sortOrder !== undefined && sortOrder !== null)
    fields.push({ key: "sort_order", value: String(sortOrder) });

  // CREATE
  if (!existing?.id) {
    const data = await graphqlFetch(mutationCreate, {
      metaobject: { type, handle, fields },
    });
    return data.metaobjectCreate.metaobject;
  }

  // UPDATE only if important fields changed (keeps it lighter)
  const existingTitle = getField(existing, "title");
  const existingSlug = getField(existing, "slug");
  const existingPath = getField(existing, "path");
  const existingParent = getField(existing, "parent");

  const needsUpdate =
    existingTitle !== title ||
    existingSlug !== slug ||
    existingPath !== pathValue ||
    (parentId && existingParent !== parentId);

  if (!needsUpdate) return { id: existing.id, handle: existing.handle };

  const data = await graphqlFetch(mutationUpdate, {
    id: existing.id,
    metaobject: { fields },
  });

  return data.metaobjectUpdate.metaobject;
}

export async function syncCategoryTree() {
  const TYPE = "category_node";

  console.log("🔐 Fetching Amrod token...");
  const token = await fetchAmrodToken();

  console.log("📚 Fetching Amrod categories...");
  const categoriesResponse = await fetchAmrodCategories(token);

  const roots = Array.isArray(categoriesResponse)
    ? categoriesResponse
    : Array.isArray(categoriesResponse?.categories)
      ? categoriesResponse.categories
      : [];

  console.log(`✅ Got ${roots.length} categories`);

  const allCats = flattenCategories(roots);
  console.log(`🌲 Flattened total nodes: ${allCats.length}`);

  // Cache by handle => id (cuts repeated lookups within this run)
  const cache = new Map();

  let upserted = 0;
  let skippedNoPath = 0;

  for (const cat of allCats) {
    const rawPath = cat?.categoryPath ?? null;

    if (!rawPath) {
      skippedNoPath++;
      continue;
    }

    const segs = splitPathSegments(rawPath);
    if (!segs.length) {
      skippedNoPath++;
      continue;
    }

    let parentId = null;
    let cumulativeSegs = [];

    for (let depth = 0; depth < segs.length; depth++) {
      const seg = segs[depth];
      cumulativeSegs = [...cumulativeSegs, seg];

      const handle = buildHandleFromSegments(cumulativeSegs);
      const pathValue = cumulativeSegs.join("/"); // store original readable path

      // Use cached id if already created/seen in this run
      const cachedId = cache.get(handle);
      if (cachedId) {
        parentId = cachedId;
        continue;
      }

      const isLeaf = depth === segs.length - 1;

      const node = await upsertNode({
        type: TYPE,
        handle,
        title: seg,
        slug: slugify(seg),
        pathValue,
        depth,
        parentId,
        sourceId: isLeaf ? cat?.id : null,
        imageUrl: isLeaf ? (cat?.categoryImage ?? null) : null,
        sortOrder: isLeaf ? (cat?.order ?? null) : null,
      });

      cache.set(handle, node.id);
      parentId = node.id;
      upserted++;

      // Friendly pace; you can reduce once it’s stable
      await sleep(80);
    }
  }

  console.log(
    `🌳 Tree sync done. Upserted: ${upserted} | skippedNoPath: ${skippedNoPath}`
  );
}
