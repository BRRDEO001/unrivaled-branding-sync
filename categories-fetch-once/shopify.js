// shopify.js
import https from "https";
import { SHOPIFY_STORE, SHOPIFY_TOKEN, REQUEST_DELAY_MS } from "./config.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

          // Surface userErrors as real errors if present on first payload.
          const firstPayload = json.data && Object.values(json.data)[0];
          const userErrors = firstPayload?.userErrors;
          if (Array.isArray(userErrors) && userErrors.length) {
            return reject(new Error(`UserErrors: ${JSON.stringify(userErrors)}`));
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

/**
 * MENU UPSERT
 */
export const getMenuByHandle = async (handle) => {
  const query = `
    query GetMenu($query: String!) {
      menus(first: 1, query: $query) {
        nodes { id handle title isDefault }
      }
    }
  `;
  const data = await graphqlFetch(query, { query: `handle:${handle}` });
  return data.menus.nodes[0] ?? null;
};

const toMenuItemInputTree = (items = []) =>
  items.map((item) => ({
    title: item.title,
    type: item.type,
    resourceId: item.resourceId ?? null,
    url: item.url ?? null,
    tags: item.tags ?? null,
    items: toMenuItemInputTree(item.items ?? []),
  }));

export const upsertMenu = async ({ title, handle, items }) => {
  if (!Array.isArray(items)) {
    throw new Error("upsertMenu: `items` must be an array (use [] for empty).");
  }

  const existing = await getMenuByHandle(handle);
  const itemTree = toMenuItemInputTree(items);

  if (!existing) {
    const mutation = `
      mutation CreateMenu($title: String!, $handle: String!, $items: [MenuItemCreateInput!]!) {
        menuCreate(title: $title, handle: $handle, items: $items) {
          menu { id handle title }
          userErrors { field message }
        }
      }
    `;
    const data = await graphqlFetch(mutation, { title, handle, items: itemTree });
    await sleep(REQUEST_DELAY_MS);
    return data.menuCreate.menu;
  }

  const mutation = `
    mutation UpdateMenu($id: ID!, $title: String!, $handle: String, $items: [MenuItemUpdateInput!]!) {
      menuUpdate(id: $id, title: $title, handle: $handle, items: $items) {
        menu { id handle title }
        userErrors { field message }
      }
    }
  `;

  const variables = {
    id: existing.id,
    title,
    handle: existing.isDefault ? null : handle,
    items: itemTree,
  };

  const data = await graphqlFetch(mutation, variables);
  await sleep(REQUEST_DELAY_MS);
  return data.menuUpdate.menu;
};

/**
 * SHOP METAFIELD UPSERT (store your full tree JSON here)
 * Uses metafieldsSet. :contentReference[oaicite:2]{index=2}
 */

export const getShopId = async () => {
  const query = `
    query {
      shop { id }
    }
  `;
  const data = await graphqlFetch(query);
  return data.shop.id;
};

export const setShopJsonMetafield = async ({
  namespace,
  key,
  jsonValue,
}) => {
  const ownerId = await getShopId();

  const mutation = `
    mutation SetShopMetafield($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key type }
        userErrors { field message }
      }
    }
  `;

  // MetafieldsSetInput.value is always a string; for type "json" pass stringified JSON. :contentReference[oaicite:3]{index=3}
  const variables = {
    metafields: [
      {
        ownerId,
        namespace,
        key,
        type: "json",
        value: JSON.stringify(jsonValue),
      },
    ],
  };

  const data = await graphqlFetch(mutation, variables);
  await sleep(REQUEST_DELAY_MS);
  return data.metafieldsSet.metafields[0];
};
