const WORKFLOW_INPUTS = {
  dry_run: "false",
  delete_existing: "true",
  apply_prices: "true",
  concurrency: "4",
};

export async function triggerSyncWorkflow(env, productName) {
  const repo = env.GITHUB_REPO;
  const workflowFile = env.GITHUB_WORKFLOW_FILE || "amrod-shopify-sync-by-name.yml";
  const token = env.GITHUB_PAT;

  if (!repo || !token) {
    throw new Error("Server misconfigured: GITHUB_REPO and GITHUB_PAT are required");
  }

  const name = String(productName || "").trim();
  if (!name) throw new Error("Product name is required");

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

    return {
      ok: true,
      message: `Sync started for "${name}".`,
      actionsUrl,
    };
  }

  const text = await res.text();
  throw new Error(`GitHub API ${res.status}: ${text || res.statusText}`);
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
