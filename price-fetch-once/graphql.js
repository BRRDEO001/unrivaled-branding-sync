// graphql.js
import fs from "fs";
import { SHOPIFY_TOKEN, SHOPIFY_GRAPHQL_ENDPOINT } from "./config.js";

export async function shopifyGraphql(query, variables) {
  if (!SHOPIFY_GRAPHQL_ENDPOINT || !SHOPIFY_TOKEN) {
    throw new Error("Missing Shopify GraphQL credentials (SHOPIFY_STORE/SHOPIFY_TOKEN)");
  }

  const res = await fetch(SHOPIFY_GRAPHQL_ENDPOINT, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": SHOPIFY_TOKEN,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  const json = await res.json().catch(() => null);

  if (!res.ok) {
    throw new Error(`Shopify GraphQL HTTP ${res.status}: ${JSON.stringify(json)}`);
  }

  if (json?.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(json.errors)}`);
  }

  return json.data;
}

/**
 * Reserve a staged upload target for BULK_MUTATION_VARIABLES (JSONL).
 * Shopify requires: resource=BULK_MUTATION_VARIABLES, mimeType="text/jsonl", httpMethod=POST.
 */
export async function createBulkVarsStagedUploadTarget({ filename = "bulk_op_vars" } = {}) {
  const mutation = `
    mutation {
      stagedUploadsCreate(input: [{
        resource: BULK_MUTATION_VARIABLES,
        filename: "${filename}",
        mimeType: "text/jsonl",
        httpMethod: POST
      }]) {
        userErrors { field message }
        stagedTargets {
          url
          resourceUrl
          parameters { name value }
        }
      }
    }
  `;

  const data = await shopifyGraphql(mutation);

  const payload = data?.stagedUploadsCreate;
  const errs = payload?.userErrors || [];
  if (errs.length) throw new Error(`stagedUploadsCreate userErrors: ${JSON.stringify(errs)}`);

  const target = payload?.stagedTargets?.[0];
  if (!target?.url || !Array.isArray(target?.parameters)) {
    throw new Error(`stagedUploadsCreate returned unexpected target: ${JSON.stringify(target)}`);
  }

  // For bulk ops, the "stagedUploadPath" is the parameter named "key"
  const keyParam = target.parameters.find((p) => p.name === "key")?.value;
  if (!keyParam) throw new Error(`Missing "key" parameter in stagedTargets: ${JSON.stringify(target.parameters)}`);

  return {
    url: target.url,
    parameters: target.parameters,
    stagedUploadPath: keyParam,
  };
}

/**
 * Upload JSONL to the staged target using multipart form data.
 * IMPORTANT: the "file" field must be the LAST field (Shopify docs).
 */
export async function uploadJsonlToStagedTarget({ url, parameters, jsonlPath }) {
  if (!fs.existsSync(jsonlPath)) throw new Error(`JSONL file not found: ${jsonlPath}`);

  const form = new FormData();

  // Add all parameters first
  for (const p of parameters) {
    form.append(p.name, p.value);
  }

  // Then add file LAST
  const buf = fs.readFileSync(jsonlPath);
  const blob = new Blob([buf], { type: "text/jsonl" });
  form.append("file", blob, jsonlPath.split("/").pop() || "bulk.jsonl");

  const res = await fetch(url, { method: "POST", body: form });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Staged upload failed: HTTP ${res.status} ${text}`);
  }
}

/**
 * Start a bulk mutation run.
 */
export async function runBulkMutation({ mutationString, stagedUploadPath, clientIdentifier = "bulk-run" }) {
  const mutation = `
    mutation RunBulk($mutation: String!, $path: String!, $clientId: String) {
      bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $path, clientIdentifier: $clientId) {
        bulkOperation { id status url }
        userErrors { field message }
      }
    }
  `;

  const data = await shopifyGraphql(mutation, {
    mutation: mutationString,
    path: stagedUploadPath,
    clientId: clientIdentifier,
  });

  const payload = data?.bulkOperationRunMutation;
  const errs = payload?.userErrors || [];
  if (errs.length) throw new Error(`bulkOperationRunMutation userErrors: ${JSON.stringify(errs)}`);

  const op = payload?.bulkOperation;
  if (!op?.id) throw new Error(`bulkOperationRunMutation returned no operation: ${JSON.stringify(payload)}`);

  return op; // { id, status, url }
}

/**
 * Poll bulkOperation(id:) (recommended for API 2026-01+).
 */
export async function pollBulkOperation({ id, intervalMs = 4000, timeoutMs = 60 * 60 * 1000 }) {
  const query = `
    query GetBulkOp($id: ID!) {
      bulkOperation(id: $id) {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
        partialDataUrl
      }
    }
  `;

  const start = Date.now();

  while (true) {
    const data = await shopifyGraphql(query, { id });
    const op = data?.bulkOperation;

    if (!op) throw new Error(`bulkOperation query returned nothing for id=${id}`);

    const status = op.status;
    if (status === "COMPLETED") return op;

    if (status === "FAILED" || status === "CANCELED") {
      throw new Error(
        `Bulk op ${status}: ${op.errorCode || "UNKNOWN"} (partialDataUrl=${op.partialDataUrl || "none"})`
      );
    }

    if (Date.now() - start > timeoutMs) {
      throw new Error(`Timed out polling bulk operation after ${Math.round(timeoutMs / 1000)}s (last status=${status})`);
    }

    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
