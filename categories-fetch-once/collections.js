// collections.js
import https from "https";
import { SHOPIFY_STORE, SHOPIFY_TOKEN, REQUEST_DELAY_MS } from "./config.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 🔒 Local constant (do NOT import from config.js)
const CATEGORY_SEO_SUFFIX =
  "unrivaled-branding-corporate-printing-products";

/* -------------------------
   GraphQL helper
-------------------------- */

const graphqlFetch = (query, variables = {}) => {
  const data = JSON.stringify({ query, variables });

  const options = {
    hostname: SHOPIFY_STORE,
    path: "/admin/api/2026-01/graphql.json",
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(data),
      "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    },
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        try {
          const json = JSON.parse(body);

          if (json.errors?.length) {
            return reject(
              new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`)
            );
          }

          const firstPayload = json.data && Object.values(json.data)[0];
          const userErrors = firstPayload?.userErrors;
          if (Array.isArray(userErrors) && userErrors.length) {
            return reject(
              new Error(`UserErrors: ${JSON.stringify(userErrors)}`)
            );
          }

          resolve(json.data);
        } catch (err) {
          reject(err);
        }
      });
    });

    req.on("error", reject);
    req.write(data);
    req.end();
  });
};

/* -------------------------
   Utilities
-------------------------- */

const slugify = (input) =>
  String(input || "")
    .toLowerCase()
    .trim()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

const splitPathSegments = (p) =>
  String(p || "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .replace(/\/{2,}/g, "/")
    .split("/")
    .map((s) => s.trim())
    .filter(Boolean);

/* -------------------------
   Collection helpers
-------------------------- */

const findCollectionByTitle = async (title) => {
  const query = `
    query FindCollection($q: String!) {
      collections(first: 1, query: $q) {
        nodes {
          id
          title
          handle
        }
      }
    }
  `;
  const data = await graphqlFetch(query, { q: `title:"${title}"` });
  return data.collections.nodes[0] ?? null;
};

const publishToOnlineStore = async (publishableId) => {
  const getPubQuery = `
    query GetPublication {
      publications(first: 1) {
        nodes { id name }
      }
    }
  `;

  let publicationId = null;

  try {
    const pubData = await graphqlFetch(getPubQuery);
    publicationId = pubData.publications.nodes[0]?.id ?? null;
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes("Access denied")) {
      console.warn(
        "⚠️ Skipping publishing: token missing publication scopes."
      );
      return;
    }
    throw err;
  }

  if (!publicationId) return;

  const mutation = `
    mutation Publish($id: ID!, $pubId: ID!) {
      publishablePublish(id: $id, input: { publicationId: $pubId }) {
        userErrors { field message }
      }
    }
  `;

  try {
    await graphqlFetch(mutation, {
      id: publishableId,
      pubId: publicationId,
    });
  } catch (err) {
    console.warn("⚠️ Publish skipped:", err?.message || err);
  }
};

const buildRuleSet = (tagNeedles) => {
  const clean = [...new Set(tagNeedles)]
    .filter(Boolean)
    .map((t) => String(t).trim())
    .filter((t) => t.length);

  if (!clean.length) {
    throw new Error("No valid tag rules provided");
  }

  return {
    appliedDisjunctively: true,
    rules: clean.map((needle) => ({
      column: "TAG",
      relation: "EQUALS",
      condition: needle,
    })),
  };
};

const createSmartCollection = async ({ title, tagNeedles, imageSrc }) => {
  const mutation = `
    mutation CreateCollection($input: CollectionInput!) {
      collectionCreate(input: $input) {
        collection { id title handle }
        userErrors { field message }
      }
    }
  `;

  const baseInput = {
    title,
    ruleSet: buildRuleSet(tagNeedles),
  };

  if (imageSrc) {
    try {
      const data = await graphqlFetch(mutation, {
        input: { ...baseInput, image: { src: imageSrc } },
      });
      return data.collectionCreate.collection;
    } catch (err) {
      console.warn(
        `⚠️ Image rejected for "${title}". Retrying without image.`
      );
    }
  }

  const data = await graphqlFetch(mutation, { input: baseInput });
  return data.collectionCreate.collection;
};

const updateCollectionRuleSet = async ({ id, title, tagNeedles }) => {
  const mutation = `
    mutation UpdateCollection($input: CollectionInput!) {
      collectionUpdate(input: $input) {
        collection { id title handle }
        userErrors { field message }
      }
    }
  `;

  const data = await graphqlFetch(mutation, {
    input: { id, title, ruleSet: buildRuleSet(tagNeedles) },
  });

  return data.collectionUpdate.collection;
};

/* -------------------------
   MAIN ENTRY
-------------------------- */

/**
 * Products are tagged with:
 * - each PATH SEGMENT independently
 * - plus SEO tag from category name
 *
 * Collection matches:
 * - leaf path segment
 * - OR SEO tag
 */
export const getOrCreateCollection = async (category) => {
  const title = category.categoryName ?? category.name ?? "Untitled";

  const rawPath = category.categoryPath ?? category.path ?? "";
  const segments = splitPathSegments(rawPath);
  const leafSegment = segments.at(-1) ?? null;

  const seoTag = title
    ? `${slugify(title)}-${CATEGORY_SEO_SUFFIX}`.slice(0, 255)
    : null;

  const tagNeedles = [leafSegment, seoTag].filter(Boolean);

  const imageRaw = category.categoryImage ?? category.image;
  const imageSrc =
    typeof imageRaw === "string" &&
    /\.(jpe?g|png|gif|webp)$/i.test(imageRaw)
      ? imageRaw
      : null;

  const existing = await findCollectionByTitle(title);

  if (existing?.id) {
    console.log(`✅ Found collection: ${title}. Updating rules…`);

    const updated = await updateCollectionRuleSet({
      id: existing.id,
      title,
      tagNeedles,
    });

    await publishToOnlineStore(updated.id);
    await sleep(REQUEST_DELAY_MS);
    return updated;
  }

  console.log(`🆕 Creating smart collection: ${title}`);
  const created = await createSmartCollection({
    title,
    tagNeedles,
    imageSrc,
  });

  await publishToOnlineStore(created.id);
  await sleep(REQUEST_DELAY_MS);
  return created;
};
