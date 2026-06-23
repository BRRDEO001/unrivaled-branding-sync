# Amrod Sync — Shopify Admin UI (free)

Give your team a **search box inside Shopify Admin** to sync Amrod products — no GitHub login required.

## How it works

```
Shopify Admin (Apps → Amrod Sync)
        ↓
Cloudflare Worker (free tier) — this UI + API
        ↓
GitHub Actions — runs `amrod-shopify-sync-by-name.yml` in the background
        ↓
Shopify store updated
```

GitHub still runs the heavy sync (Amrod fetch + import). Clients only see a simple form.

**Cost:** $0 on typical usage
- Cloudflare Workers free tier (100k requests/day)
- GitHub Actions free minutes (private repos have a monthly limit)
- Shopify Custom App — free for your own store

---

## One-time setup (≈30 minutes)

### 1. GitHub — personal access token

1. GitHub → **Settings → Developer settings → Personal access tokens → Fine-grained**
2. Repository access: this repo only
3. Permissions: **Actions → Read and write**
4. Copy the token

### 2. Deploy the UI (Cloudflare)

```bash
cd shopify-sync-ui
npm install
npx wrangler login

# Secrets (not committed)
npx wrangler secret put GITHUB_PAT          # paste GitHub token
npx wrangler secret put SHOPIFY_API_SECRET  # from Shopify app (step 3)

# Optional: staff password if not opening from embedded app
npx wrangler secret put SYNC_API_KEY

# Deploy
npm run deploy
```

Note the URL, e.g. `https://amrod-sync-ui.<your-subdomain>.workers.dev`

Edit `wrangler.toml` if needed:
- `GITHUB_REPO` — your `owner/repo`
- `ALLOWED_SHOP` — optional, e.g. `your-store.myshopify.com`

Public var (not secret):
```bash
npx wrangler secret put SHOPIFY_API_KEY   # actually use: wrangler vars or [vars] in toml
```

Add to `wrangler.toml`:
```toml
[vars]
SHOPIFY_API_KEY = "your_app_client_id"
```

Redeploy after changing vars.

### 3. Shopify — Custom app

1. **Shopify Admin → Settings → Apps and sales channels → Develop apps**
2. **Create an app** → name it e.g. `Amrod Sync`
3. **Configuration**
   - **App URL:** `https://amrod-sync-ui.<subdomain>.workers.dev`
   - **Allowed redirection URL(s):** same URL (and `/auth/callback` if Shopify asks)
   - Enable **Embed app in Shopify admin**
4. **API credentials** — copy **Client ID** → `SHOPIFY_API_KEY` in wrangler
5. Copy **Client secret** → `SHOPIFY_API_SECRET` wrangler secret
6. **Install app** on the store

Your team opens **Apps → Amrod Sync** in Shopify Admin.

### 4. Scopes (minimal)

For this UI-only trigger, the app does **not** need product write scopes — it only calls your worker, which triggers GitHub. You can leave Admin API scopes empty or minimal.

---

## Using the app

1. Open **Apps → Amrod Sync** in Shopify Admin
2. Paste the **exact Amrod product name** (case-sensitive)
3. Click **Sync to Shopify**
4. Wait a few minutes — all Amrod rows with that exact name are imported

Optional: run **Dry run** from GitHub if you want to preview matches first (staff-only).

---

## Security

- Embedded app requests include a **Shopify session token** (JWT) verified by the worker
- Optional `ALLOWED_SHOP` restricts which store can trigger syncs
- Optional `SYNC_API_KEY` for staff testing outside embedded mode
- `GITHUB_PAT` never leaves the server — clients never see it

---

## Local development

```bash
cd shopify-sync-ui
npm run dev
```

Open `http://localhost:8787` — use staff API key if App Bridge is not available.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Unauthorized | Open from Shopify Apps menu, or enter staff API key |
| GitHub 404 | Check `GITHUB_REPO` and `GITHUB_WORKFLOW_FILE` in wrangler.toml |
| No products synced | Name must match Amrod **exactly** (case-sensitive) |
| Workflow not starting | PAT needs **Actions: Read and write** on the repo |

---

## Alternative (no Cloudflare)

If you prefer not to deploy a worker, staff can bookmark a **GitHub Actions** link with pre-filled inputs — but that still requires GitHub access. The worker + embedded app is the client-friendly option.
