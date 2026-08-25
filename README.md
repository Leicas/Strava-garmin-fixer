# StravaFit

Auto-merge Fitbit GPS + HR with Garmin Edge cadence/speed/temperature streams,
then replace the original activity with the merged file.

See `PLAN.md` for the full architecture; phases 0–6 are all shipped.

## ⚠ Strava API paywall (June 2026) → Garmin Connect is now the primary target

Strava's Developer Program requires a paid Strava subscription for API access
since 2026-06-30. Without one the app is flagged **"Application Status:
Inactive"** and every API call returns a bare `403 Forbidden` — which is why
the Strava pages of this dashboard broke. Two ways out:

- Pay for Strava and reactivate the app at <https://www.strava.com/settings/api>
  — the original Strava flow below then works again unchanged.
- **Use the Garmin Connect pivot (default):** the Edge already syncs to Garmin
  Connect, so StravaFit now downloads the original FIT from Garmin, merges in
  Google Health GPS+HR, and **replaces the activity on Garmin**. Garmin's
  native Strava sync pushes the merged activity to Strava for free.

### Garmin setup

1. Set `GARMIN_EMAIL` / `GARMIN_PASSWORD` in `.env` (unofficial API — your own
   account credentials via `python-garminconnect`, no dev program).
2. If the account has MFA, run `stravafit garmin login` once interactively to
   seed the token cache (`data/garmin_tokens/`, lasts ~a year). Headless
   logins resume from the cache afterwards.
3. Open **/garmin** in the dashboard: list activities, then per row —
   **Dry run** (merge only), **Semi** (merge, pause for you to delete the
   original on Garmin Connect, then upload), **Auto** (delete + upload).
4. Optional poller (replaces the Strava webhook as the automatic trigger):
   set `GARMIN_POLL_MINUTES=15` (`GARMIN_POLL_MODE=auto|semi_auto|dry_run`).
   It honors the same *auto merge* toggle on /settings.
5. CLI equivalents: `stravafit garmin list | fetch-fit | run-merge | delete-activity`.

**Strava cleanup caveat:** with Garmin→Strava sync on, the *original* reaches
Strava seconds after the ride — before any merge can run — and without API
access it can't be deleted programmatically. After each replace, delete the
original on strava.com by hand; the merged version syncs over from Garmin
automatically. Job logs remind you.

### Feeding Dreeve

StravaFit can act as [Dreeve](https://dreeve.app)'s Garmin connector, delivering
the *enriched* activity instead of the raw one: set `DREEVE_EXPORT_ENABLED=1`
and point the compose variable `DREEVE_WATCH_DIR` at Dreeve's watch folder
(when co-located). Every handled activity produces exactly one file there —
the merged FIT when a Google Health match exists, the original FIT otherwise
("Passthrough" badge). The same files are served over the authenticated
`/export/` API (list / fetch / DELETE-to-ack) if the two apps ever live on
different hosts.

Dreeve skips imports whose (sport type, start time) already exist, so the
stock `dreeve-garmin-connector` download loop must be disabled — if it
delivers the raw original first, the merged version is skipped forever.
Activities it already imported can only be upgraded by deleting them in
Dreeve's admin and letting StravaFit re-deliver.

To reuse an existing Garmin session instead of a fresh credential login
(Garmin rate-limits repeat logins), POST the connector's `garmin_tokens.json`
to `/settings/garmin-tokens` (Basic auth, `Content-Type: application/json`).

## Run locally (host Python via uv)

```sh
uv sync
cp .env.example .env          # then fill in the OAuth creds
uv run python -m app.cli strava login
uv run uvicorn app.main:app --reload --port 8000
# Then click "Connect Google" on /settings to finish OAuth via the dashboard.
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

## Workflow note: Strava's API delete is unreliable

Strava documents `DELETE /activities/{id}` as a supported endpoint with
`activity:write` scope, but it has been inconsistent in practice. Combined
with Strava's content-based duplicate detection (which rejects uploads whose
start time matches an existing activity), this means the fully-automatic
"delete original → upload merged" path can fail.

**Recommended workflow:** manual upload via the dashboard.

1. From the activity detail page → click **Find Google Health match** → pick the right one → **Download merged FIT**.
2. Upload the FIT to Strava via the web UI.
3. Delete the original on Strava via the web UI.
4. Back on the original activity's detail page in StravaFit, click **Mark as merged** so the webhook handler skips it next time.

**Probe whether DELETE works for your token:** before you trust auto-replace,
try `stravafit strava delete-activity <test_id>` on a throwaway test ride.
If it returns 204 (or 404 for a nonexistent id, meaning permission was OK),
the auto path is safe. If 401/403, stick to manual.

**Safety net for auto-replace:** when you do click "Replace on Strava (auto)",
the worker writes the merged FIT to `data/recovery/strava-{id}-{ts}.fit`
*before* deleting the original. If anything fails between delete and upload,
the recovery file is downloadable from `/jobs/{id}/recovery.fit` so you can
finish the upload by hand.

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
2. Click **Connect Google** → same flow against Google Health.

Tokens persist in `./data/state.db` (mounted volume), so you only do this once
unless you revoke access.

### Register the redirect URIs

The provider has to know where to redirect back to. Register these once:

| Provider | Where | Setting | Value |
|---|---|---|---|
| Strava | <https://www.strava.com/settings/api> | Authorization Callback Domain | the host of `PUBLIC_BASE_URL` (no scheme, no port, no path — e.g. `stravafit.example.com` or `localhost`) |
| Google Health | <https://console.cloud.google.com/> | Authorized redirect URI | `<PUBLIC_BASE_URL>/auth/google/callback` (full URL, exact match) |

**Google Health setup:**
1. In Google Cloud Console, create a project and enable the **Health API** (`health.googleapis.com`).
2. Configure the OAuth consent screen — for personal use leave it in "Testing" mode and add yourself as a test user.
3. Create OAuth 2.0 credentials → application type **Web application** → add the redirect URI above.
4. Copy the client ID + client secret into `.env` as `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`.

The Google Health API replaces the deprecated Fitbit Web API. Same TCX export
endpoint shape (`:exportExerciseTcx` returns `{"tcxData": "..."}`), so the
merge function works unchanged. Required scopes are
`googlehealth.activity_and_fitness.readonly`,
`googlehealth.location.readonly`, and
`googlehealth.health_metrics_and_measurements.readonly`.

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
docker compose exec stravafit uv run python -m app.cli google find-near 2026-05-02T08:00:00Z
docker compose exec stravafit uv run python -m app.cli run-merge <strava_id> --dry-run
```

`strava login` still works for host-based dev (local listener on `:8001`).
Google Health connection only goes through the dashboard — no CLI variant —
since Google's OAuth requires a registered web client.

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
  db.py              aiosqlite helpers + on-startup column migration
  schema.sql         idempotent DDL
  tokens.py          shared TokenPair store
  jobs.py            jobs + processed_activities CRUD
  worker.py          merge job orchestrator
  webhook.py         Strava push-subscription handler
  merge.py           pure deterministic streams+TCX → FIT
  logging.py         structlog config
  cli.py             stravafit CLI entrypoint
  security.py        BasicAuthMiddleware + require_htmx CSRF guard
  auth_router.py     /auth/{strava,google}/{start,callback}
  strava/{auth,client}.py         OAuth + API client
  garmin/client.py                Garmin Connect client (unofficial API, curl_cffi)
  garmin/fitparse.py              FIT bytes -> merge-shaped streams (pure)
  garmin/worker.py                Garmin-sourced merge pipeline (replace on Garmin)
  garmin/poller.py                background poller (Garmin has no personal webhooks)
  google_health/{auth,client}.py  OAuth + API client (replaces Fitbit)
  dashboard/router.py             HTMX-friendly routes
  templates/...                   Jinja2 + Tailwind/DaisyUI/HTMX (CDN)
scripts/
  bootstrap_subscription.py  idempotent subscription manager
tests/
  test_merge.py     determinism + sanity-check tests (5/5 passing)
```
