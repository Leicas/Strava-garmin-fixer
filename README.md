# StravaFit

Auto-merge Fitbit GPS + HR with Garmin Edge cadence/speed/temperature streams,
then replace the original Strava activity with the merged file.

See `PLAN.md` for the full architecture; phases 0–6 are all shipped.

## Run locally (host Python via uv)

```sh
uv sync
cp .env.example .env          # then fill in the OAuth creds
uv run python -m app.cli strava login
uv run python -m app.cli fitbit login
uv run uvicorn app.main:app --reload --port 8000
```

Then open <http://localhost:8000/> and <http://localhost:8000/healthz>.

## Security checklist before exposing to the internet

This service drives your Strava activities and stores OAuth refresh tokens.
Before putting it behind a public hostname:

1. **Set `DASHBOARD_USER` and `DASHBOARD_PASSWORD`** in `.env`. The dashboard
   uses HTTP Basic auth on every route except `/healthz`, `/webhook/strava`,
   and the OAuth callbacks (which can't be authed by their senders). Pick a
   strong, unique password.
2. **Set `STRAVA_OWNER_ID`** to your Strava athlete id once you know it. The
   webhook handler drops events from any other owner — the only meaningful
   filter we have, since Strava POSTs are not signed.
3. **Set a strong `VERIFY_TOKEN`** (used during the Strava push-subscription
   handshake — any random ~32-char string is fine).
4. **Front the service with a tunnel that terminates HTTPS**: Cloudflare
   Tunnel, Tailscale Funnel, or a VPS reverse proxy. The container only binds
   `127.0.0.1:8000`, so the tunnel is the only ingress.
5. **Recommended (free)**: layer Cloudflare Access on top of the tunnel —
   gives you SSO + IP-allowlist + rate-limiting at the edge. The Basic auth
   inside the app is then defense-in-depth, not the only line.
6. **Never set `DASHBOARD_AUTH_DISABLED=1`** on a public host.

State-changing dashboard endpoints (`/settings/toggle`, `/activity/*/merge`)
require the `HX-Request` header that HTMX always sends. This blocks classic
CSRF (a cross-origin form submit can't forge that header) even if your browser
has cached Basic credentials.

What this does NOT cover:
- Brute-force against weak passwords. Pick a strong one.
- Compromised browser session. Out of scope.
- DoS / rate limiting. Use the tunnel layer.

## Run in Docker (recommended for server deployment)

```sh
cp .env.example .env          # fill in OAuth creds, VERIFY_TOKEN, PUBLIC_BASE_URL
docker compose build
docker compose up -d
```

The dashboard listens on `http://localhost:8000` (loopback only — front it with
a tunnel or reverse proxy for production).

### OAuth — entirely through the dashboard

No shell access required. From <http://localhost:8000/settings>:

1. Click **Connect Strava** → redirected to Strava → consent → bounced back to `/settings`.
2. Click **Connect Fitbit** → same flow.

Tokens persist in `./data/state.db` (mounted volume), so you only do this once
unless you revoke access.

### Register the redirect URIs

The provider has to know where to redirect back to. Register these once:

| Provider | Where | Setting | Value |
|---|---|---|---|
| Strava | <https://www.strava.com/settings/api> | Authorization Callback Domain | the host of `PUBLIC_BASE_URL` (no scheme, no port, no path — e.g. `stravafit.example.com` or `localhost`) |
| Fitbit | <https://dev.fitbit.com/apps/new> | Redirect URI | `<PUBLIC_BASE_URL>/auth/fitbit/callback` (full URL, exact match). App type: **Personal** |

`PUBLIC_BASE_URL` in your `.env` must match what the user's browser can reach
the service at — for pure-localhost dev that's `http://localhost:8000`; for a
deployment behind a tunnel, the public HTTPS hostname.

### Webhook subscription (production only)

Once `PUBLIC_BASE_URL` resolves over HTTPS:

```sh
docker compose exec stravafit uv run python -m scripts.bootstrap_subscription ensure
```

Idempotent — creates the Strava push subscription only if none currently points
at your callback URL.

### Logs

```sh
docker compose logs -f stravafit
```

Output is structured JSON via `structlog`.

### CLI commands (optional, also available in-container)

The `stravafit` CLI is shipped in the image for inspection / debugging:

```sh
docker compose exec stravafit uv run python -m app.cli strava list
docker compose exec stravafit uv run python -m app.cli fitbit find-near 2026-05-02T08:00:00Z
docker compose exec stravafit uv run python -m app.cli run-merge <strava_id> --dry-run
```

`strava login` / `fitbit login` still work for host-based development (they
spin up local HTTP listeners on 8001 / 8002), but the dashboard flow is the
recommended path for any server deployment.

## Deploy via Komodo

`compose.komodo.yaml` is the variant that uses Komodo's `[[SECRET]]` template
syntax (resolved server-side at deploy time — plain `docker compose` cannot
expand that, which is why it's split from the local file).

1. Build & push the image to a registry (or set `build: .` in the Komodo file
   and let Periphery build locally).
2. Define `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, `FITBIT_CLIENT_ID`,
   `FITBIT_CLIENT_SECRET`, `VERIFY_TOKEN` as Komodo Secrets.
3. Set `PUBLIC_BASE_URL` as a Komodo Variable pointing at your tunnel hostname.
4. Create a Stack pointing at `compose.komodo.yaml`.
5. Run Cloudflare Tunnel separately, mapping
   `stravafit.<your-domain>` → `http://127.0.0.1:8000`.
6. After first deploy, run `bootstrap_subscription ensure` once via
   `komodo` exec or by `docker compose exec` on the box.

## Layout

```
app/
  main.py            FastAPI app + lifespan
  config.py          pydantic-settings
  db.py              aiosqlite helpers
  schema.sql         idempotent DDL
  tokens.py          shared TokenPair store
  jobs.py            jobs + processed_activities CRUD
  worker.py          merge job orchestrator
  webhook.py         Strava push-subscription handler
  merge.py           pure deterministic streams+TCX → FIT
  logging.py         structlog config
  cli.py             stravafit CLI entrypoint
  strava/{auth,client}.py   OAuth + API client
  fitbit/{auth,client}.py   OAuth (PKCE) + API client
  dashboard/router.py       HTMX-friendly routes
  templates/...             Jinja2 + Tailwind/DaisyUI/HTMX (CDN)
scripts/
  bootstrap_subscription.py  idempotent subscription manager
tests/
  test_merge.py     determinism + sanity-check tests (5/5 passing)
```
