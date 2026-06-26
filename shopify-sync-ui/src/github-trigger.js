const WORKFLOW_INPUTS = {
  dry_run: "false",
  delete_existing: "true",
  apply_prices: "true",
  concurrency: "4",
};

export async function triggerSyncWorkflow(env, { productName = "", fullCode = "" } = {}) {
  const repo = env.GITHUB_REPO;
  const workflowFile = env.GITHUB_WORKFLOW_FILE || "amrod-shopify-sync-by-name.yml";
  const token = env.GITHUB_PAT;

  if (!repo || !token) {
    throw new Error("Server misconfigured: GITHUB_REPO and GITHUB_PAT are required");
  }

  const name = String(productName || "").trim();
  const code = String(fullCode || "").trim();
  if (!code && !name) throw new Error("Product code or name is required");

  const url = `https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/dispatches`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "X-GitHub-Api-Version": "2026-03-10",
      "User-Agent": "amrod-sync-ui-worker",
    },
    body: JSON.stringify({
      ref: env.GITHUB_REF || "main",
      inputs: {
        ...WORKFLOW_INPUTS,
        product_name: name,
        product_code: code,
      },
    }),
  });

  if (res.status === 204 || res.status === 200) {
    let actionsUrl = `https://github.com/${repo}/actions`;

    if (res.status === 200) {
      try {
        const data = await res.json();
        if (data?.html_url) actionsUrl = data.html_url;
      } catch {
        actionsUrl = await findLatestRunUrl(env);
      }
    } else {
      actionsUrl = await findLatestRunUrl(env);
    }

    const label = code || name;
    return {
      ok: true,
      message: `Sync started for ${code ? `code ${code}` : `"${name}"`}.`,
      actionsUrl,
      fullCode: code || null,
      productName: name || null,
      label,
    };
  }

  const text = await res.text();
  throw new Error(`GitHub API ${res.status}: ${text || res.statusText}`);
}

function normalizeProductSelection(raw) {
  if (typeof raw === "string") {
    return { fullCode: "", productName: raw.trim() };
  }
  return {
    fullCode: String(raw?.fullCode || raw?.code || "").trim(),
    productName: String(raw?.productName || raw?.name || "").trim(),
  };
}

export async function triggerSyncWorkflows(env, products) {
  const list = Array.isArray(products) ? products : [products];
  const seen = new Set();
  const selections = [];

  for (const raw of list) {
    const item = normalizeProductSelection(raw);
    const key = item.fullCode || item.productName;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    selections.push(item);
  }

  if (!selections.length) {
    throw new Error("Select at least one product to sync");
  }

  const repo = env.GITHUB_REPO;
  const runs = [];

  for (const item of selections) {
    try {
      const result = await triggerSyncWorkflow(env, item);
      runs.push({
        fullCode: item.fullCode || null,
        productName: item.productName || null,
        ok: true,
        actionsUrl: result.actionsUrl,
      });
    } catch (e) {
      runs.push({
        fullCode: item.fullCode || null,
        productName: item.productName || null,
        ok: false,
        error: String(e?.message || e),
      });
    }
  }

  const started = runs.filter((r) => r.ok);
  if (!started.length) {
    throw new Error(runs[0]?.error || "All sync requests failed");
  }

  const failed = runs.filter((r) => !r.ok);
  let message = `Started ${started.length} sync job(s).`;
  if (failed.length) {
    message += ` ${failed.length} failed to start.`;
  }

  return {
    ok: true,
    message,
    startedCount: started.length,
    failedCount: failed.length,
    runs,
    actionsUrl: `https://github.com/${repo}/actions`,
  };
}

async function findLatestRunUrl(env) {
  const repo = env.GITHUB_REPO;
  const workflowFile = env.GITHUB_WORKFLOW_FILE || "amrod-shopify-sync-by-name.yml";

  try {
    const q = new URL(`https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/runs`);
    q.searchParams.set("per_page", "1");

    const res = await fetch(q, {
      headers: {
        Authorization: `Bearer ${env.GITHUB_PAT}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2026-03-10",
      },
    });

    if (!res.ok) return `https://github.com/${repo}/actions`;

    const data = await res.json();
    const run = data?.workflow_runs?.[0];
    return run?.html_url || `https://github.com/${repo}/actions`;
  } catch {
    return `https://github.com/${repo}/actions`;
  }
}
