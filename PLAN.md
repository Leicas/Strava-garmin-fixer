# Plan: Auto-merge Fitbit GPS+HR with Garmin Edge cadence/speed for Strava

## Goal
When the Garmin Edge 130 uploads a ride to Strava with missing/sparse GPS, automatically pull the matching Fitbit activity, merge GPS + HR from Fitbit with cadence + speed + temperature from Garmin, and replace the Strava activity with the merged version.

## Why webhook (and not the Fitbit app TCX export)
The mobile app's "Export as TCX" menu is unreliable and the Fitbit web dashboard was decommissioned. The Web API endpoint `GET /1/user/-/activities/{logId}.tcx` is stable and gives us GPS + HR programmatically. That's what this service uses.

## Language: Python (not Rust — here's why)

The deciding factor is FIT file *writing*, not server performance.

| Capability | Python | Rust |
|---|---|---|
| FIT read | `fitparse`, `fit-tool` — both solid | `fitparser` — actively maintained, solid |
| **FIT write** | **`fit-tool` — works, used in production** | **`fitparser` explicitly says no write support; `garminfit` is WIP and stale; no maintained Rust encoder exists** |
| HTTP server | FastAPI — fine | Axum — fine |
| OAuth, HTTP client, XML parse | All fine | All fine |
| Server-rendered HTMX dashboard | Jinja2 + FastAPI — trivial | Askama/Maud + Axum — also trivial |

For a Strava merge service, FIT writing is the whole point. Rust would force one of three bad options: write your own FIT encoder against Garmin's spec (significant project, the spec is dense and binary), settle for TCX-only output (loses the temperature stream permanently), or shell out to a Python helper from Rust (defeats the purpose of choosing Rust).

Python performance is a non-issue here. The hot path is "one webhook every few hours, do some I/O, write a ~50KB file." There's nothing to optimize.

If you want a Rust project, this isn't the right one. Save it for something CPU-bound or where you actually benefit from the type system (e.g., the haptic device firmware-adjacent stuff at work).

## High-level architecture

```
┌─────────────┐  webhook   ┌──────────────┐   HTTPS    ┌─────────────────┐    HTTPS    ┌──────────────┐
│   Strava    │───────────>│  CF Tunnel   │───────────>│   FastAPI app   │<───────────>│  Fitbit API  │
└─────────────┘            │  (or other)  │            │  ┌───────────┐  │             └──────────────┘
       ▲                   └──────────────┘            │  │ Webhook   │  │
       │                          ▲                    │  │ handler   │  │
       │                          │                    │  ├───────────┤  │
       │                          │  browser           │  │ Dashboard │  │
       │                          │  (HTMX)            │  │ (HTMX UI) │  │
       │                          │                    │  ├───────────┤  │
       │              upload      │                    │  │  SQLite   │  │
       └──────────────────────────┴────────────────────┤  │ tokens +  │  │
                          merged                       │  │  state    │  │
                                                       │  └───────────┘  │
                                                       └─────────────────┘
```

Single Python service: webhook receiver + dashboard share the same FastAPI app, the same DB, the same auth context. Deployed as a Komodo Stack on the local machine. Public reachability for the Strava webhook via Cloudflare Tunnel (or equivalent — see deployment notes).

## Components

### 1. OAuth flows (one-time setup)

**Strava** (`strava_auth.py`)
- Register app at https://www.strava.com/settings/api
- Scopes: `activity:read_all,activity:write`
- `/auth/strava/callback` exchanges code for refresh + access tokens
- Store tokens in SQLite

**Fitbit** (`fitbit_auth.py`)
- Register at https://dev.fitbit.com/apps/new
- **App type: Personal** — avoids review for a single-user app
- Scopes: `activity` (and `location` if needed for GPS — confirm against current API docs)
- `/auth/fitbit/callback` handles redirect
- Fitbit access tokens last 8h; refresh tokens rotate on each refresh — store both, always persist the new pair after refresh

Both flows can be CLI-driven for one user — local HTTP listener, browser opens, paste redirect, done. No need for a public auth UI in v1.

### 2. Strava webhook receiver

**Subscription setup (one-time bootstrap script)**
```bash
curl -X POST https://www.strava.com/api/v3/push_subscriptions \
  -F client_id=$STRAVA_CLIENT_ID \
  -F client_secret=$STRAVA_CLIENT_SECRET \
  -F callback_url=https://stravafit.<your-domain>/webhook/strava \
  -F verify_token=$VERIFY_TOKEN
```
Make this idempotent — list existing subscriptions first, only create if absent.

**`GET /webhook/strava`** — Strava verification handshake. Echo `hub.challenge` if `hub.verify_token` matches.

**`POST /webhook/strava`** — event payload:
```json
{"object_type":"activity","object_id":12345,"aspect_type":"create","owner_id":...}
```

Filter:
- `object_type == "activity"` AND `aspect_type == "create"`
- Fetch activity → check `type in ["Ride","VirtualRide"]`
- Check `device_name` matches Edge 130 (or whatever the string actually is — log it on first run, confirm)
- Check GPS is missing/sparse (see "Edge cases" #1 for the actual rule)

If it matches, enqueue background work. `FastAPI BackgroundTasks` is fine for v1 — swap for RQ/Celery only if you need retries or persistence across restarts.

### 3. Merge worker

For a given Strava activity ID:

**a) Pull Strava streams** (what the Edge has):
```
GET /activities/{id}/streams?keys=time,latlng,distance,altitude,velocity_smooth,heartrate,cadence,watts,temp,grade_smooth&key_by_type=true
```
Returns dict-of-streams. Time is seconds since start.

**b) Find the matching Fitbit activity**:
```
GET /1/user/-/activities/list.json?afterDate={ride_date}&sort=asc&limit=20
```
Match by start time within ±2 min window AND duration overlap > 80%.

**c) Pull Fitbit TCX**:
```
GET /1/user/-/activities/{logId}.tcx?includePartialTCX=true
```
Parse trackpoints — each has `<Time>`, `<Position>`, `<HeartRateBpm>`.

**d) Time-align the two streams** — this is where the time will go:
- `t_offset = fitbit_first_trackpoint_time - strava_start_time`
- Resample Fitbit lat/lon/HR onto Strava's time axis (linear interp for lat/lon, nearest-neighbor for HR is fine)
- Sanity check: distance computed from interpolated GPS should be within ~5–10% of Strava's distance stream. If it's wildly off, abort and log — something's misaligned.

**e) Build merged FIT** using [`fit-tool`](https://pypi.org/project/fit-tool/):
- One `record` per second with: `timestamp`, `position_lat`, `position_long`, `heart_rate` (Fitbit), `cadence`, `speed`, `distance`, `temperature` (Strava/Garmin)
- Add `session` and `lap` messages with totals
- File header + CRC

Fallback if FIT writing is painful: emit TCX. Strava accepts it. You lose the temperature stream — acceptable v1 tradeoff.

**f) Replace on Strava**:
1. `DELETE /activities/{strava_id}`
2. `POST /uploads` with merged file, `data_type=fit` (or `tcx`)
3. Poll `GET /uploads/{upload_id}` until `activity_id` is populated (usually a few seconds)
4. `PUT /activities/{new_id}` to set name and append `[merged-by-stravafit]` to description — this is the loop-prevention marker

### 4. Loop prevention
The replacement upload triggers a new webhook event. Two checks, applied together:
- Description contains `[merged-by-stravafit]` → skip
- Strava activity ID exists in `processed_activities` table → skip

### 5. Dashboard

Server-rendered HTMX UI sharing the same FastAPI app. No JS build step, no SPA, no separate auth context. Tailwind via CDN is fine for v1; switch to a build step only if it bothers you.

**Stack**
- Templates: Jinja2 (`fastapi.templating.Jinja2Templates`)
- Interactivity: [HTMX](https://htmx.org/) — `hx-get`, `hx-post`, `hx-swap` cover ~everything
- Styling: Tailwind + [DaisyUI](https://daisyui.com/) (CDN); pre-built components save hours
- Charts (for stream previews): [uPlot](https://github.com/leeoniya/uPlot) — tiny, fast, no React needed
- Auth: HTTP Basic over the tunnel, or Cloudflare Access policy on the tunnel hostname (preferred — zero code)

**Pages**

| Route | What it does |
|---|---|
| `/` | Activity list. Last 50 Strava activities. Status badge per row: `Untouched`, `Candidate`, `Merged`, `Skipped (no match)`, `Error`. Click → detail page. |
| `/activity/{strava_id}` | Detail. Streams preview (uPlot: lat/lon, HR, cadence, speed). Shows matched Fitbit log if any. Buttons: **Find Fitbit match**, **Preview merge**, **Run merge**, **Delete + re-merge**. |
| `/preview/{strava_id}/{fitbit_log_id}` | Renders the merged FIT in-memory and shows an overlay map + stream charts WITHOUT uploading. Confirms alignment looks sane before committing. |
| `/manual` | Form for "merge by IDs" — paste any Strava ID + Fitbit log ID, run pipeline. Useful for ride days where the auto-detection rule didn't fire. |
| `/fitbit/recent` | Last 30 Fitbit activities. Click → details + manual "find Strava match by time" button. |
| `/jobs` | Recent merge jobs with status, timing, error tracebacks. Filter by result. |
| `/settings` | Token expiry timestamps, "refresh now" buttons. Webhook subscription status. Auto-merge toggle (kill switch). |
| `/healthz` | Plain JSON, for monitoring. |

**Critical UX behaviors**

- **Preview before commit, always.** Every merge action goes through `/preview` first. The merge logic is deterministic, so preview and final-merge produce identical bytes. The dashboard always offers "Preview" before "Run" — never one-click destructive ops.
- **Dry-run mode.** A toggle in settings: when on, "Run merge" stops after producing the merged file and offers it as a download. No Strava delete/upload happens. Good for first weeks of operation.
- **Auto-merge kill switch.** A toggle that, when off, makes the webhook handler create rows in `processed_activities` with status `pending_manual_review` instead of merging. You then approve from the dashboard. Lets you watch behavior for a while before fully trusting it.
- **Map overlay.** On the preview page, render the original Edge GPS (sparse/broken) and the Fitbit GPS on the same Leaflet map in different colors. This is the single most useful sanity check.

### 6. SQLite schema

```sql
CREATE TABLE tokens (
  service TEXT PRIMARY KEY,            -- 'strava' or 'fitbit'
  access_token TEXT NOT NULL,
  refresh_token TEXT NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE TABLE processed_activities (
  strava_id INTEGER PRIMARY KEY,
  fitbit_log_id INTEGER,
  merged_at INTEGER NOT NULL,
  result TEXT NOT NULL,                -- 'success' | 'skipped:no_match' | 'pending_manual_review' | 'error:...'
  notes TEXT
);

CREATE TABLE jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  strava_id INTEGER NOT NULL,
  fitbit_log_id INTEGER,
  trigger TEXT NOT NULL,                -- 'webhook' | 'manual' | 'preview'
  status TEXT NOT NULL,                 -- 'queued' | 'running' | 'success' | 'error'
  dry_run INTEGER NOT NULL DEFAULT 0,
  started_at INTEGER,
  finished_at INTEGER,
  error TEXT,
  log TEXT
);

CREATE TABLE settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
```

## Phases for Claude Code

### Phase 0 — Project skeleton (DONE)
- FastAPI app, SQLite via `aiosqlite`, `pydantic-settings` for env
- `Dockerfile`, `compose.yaml` ready for Komodo
- `/healthz` endpoint
- Templates dir with a base layout (Tailwind + DaisyUI + HTMX from CDN), one placeholder `/` page
- `pyproject.toml` with `uv` lockfile

### Phase 1 — Strava OAuth + read
- CLI: initial OAuth dance via local listener on port 8001
- `StravaClient` class with auto-refresh on 401
- CLI: `python -m app.cli strava fetch-activity <id>` prints stream summary
- Dashboard: `/` lists last 50 Strava activities

### Phase 2 — Fitbit OAuth + read
- Same shape as Phase 1
- CLI: `python -m app.cli fitbit find-near <iso_datetime>`
- CLI: `python -m app.cli fitbit fetch-tcx <log_id>` saves TCX to disk
- Dashboard: `/fitbit/recent` + activity detail "Find Fitbit match"

### Phase 3 — Merge logic (file-based, no Strava-write yet)
- Pure function: `merge(strava_streams: dict, fitbit_tcx: bytes) -> bytes`
- Unit tests using fixtures from a real ride
- CLI: `python -m app.cli merge --strava-streams s.json --fitbit-tcx r.tcx --out merged.fit`
- Dashboard: `/preview/{strava_id}/{fitbit_log_id}` — Leaflet map overlay + uPlot charts. No upload.

### Phase 4 — Strava write
- Delete + upload + poll-for-processing
- `dry_run` flag on the worker
- Dashboard: "Run merge" button, `/jobs` page

### Phase 5 — Webhook
- Idempotent bootstrap script for subscription
- Verification handshake
- Event handler that filters and enqueues
- Dashboard: webhook status + test button in `/settings`

### Phase 6 — End-to-end with safety rails
- Wire merge worker into webhook handler
- Loop-prevention checks
- Auto-merge kill switch
- `structlog` JSON output
- Run with auto-merge OFF for 2–3 rides before enabling

## Edge cases (track these, don't necessarily block v1)

1. **Partial GPS, not zero GPS** — detection rule: ">30% of records have null latlng" OR "max distance from any single point < 200m".
2. **Fitbit start/stop differs from Edge** — time-align by overlap.
3. **GPS dropouts in Fitbit** (tunnels) — interpolate, don't discard.
4. **Activity type mismatch** — Fitbit "Outdoor Bike" vs Strava "Ride". Match by time, not type label.
5. **Determinism** — same inputs must produce byte-identical FIT. Lock timestamps, no random UUIDs.
6. **Token expiry mid-job** — refresh defensively before each call.
7. **Strava upload processing can fail** — handle the error response from `GET /uploads/{id}`. Don't delete the original until the new upload's `activity_id` is confirmed.
8. **Rate limits** — Fitbit: 150 calls/hour/user. Strava: 200/15min, 2000/day.

## Libraries

**Backend**: `fastapi`, `uvicorn[standard]`, `httpx`, `pydantic`, `pydantic-settings`,
`aiosqlite`, `fit-tool`, `lxml`, `structlog`, `pytest`, `respx`.

**Dashboard**: `jinja2`, HTMX/Tailwind/DaisyUI/Leaflet/uPlot via CDN.
