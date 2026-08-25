from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from lxml import etree

from app import jobs as jobs_mod
from app import tokens as token_store
from app.config import settings
from app.db import connect
from app.garmin.client import GarminClient, GarminNotConfigured
from app.google_health.client import (
    GoogleHealthClient,
    GoogleNotConfigured,
    activity_type_label,
)
from app.merge import MergeError, merge_streams_to_fit
from app.security import require_htmx
from app.strava.client import StravaClient, StravaNotConfigured

log = structlog.get_logger()

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
# Expose helpers to templates so we don't have to pre-enrich every dict.
templates.env.globals["activity_type_label"] = activity_type_label

router = APIRouter()


# ---------- helpers ----------------------------------------------------------

def _format_duration(seconds: int | None) -> str:
    if not seconds:
        return "—"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"


def _format_date(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return iso


def _format_unix(ts: int | None) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _badge_for(result: str | None) -> dict[str, str]:
    if result is None:
        return {"label": "Untouched", "css": "badge-ghost"}
    if result.startswith("success"):
        return {"label": "Merged", "css": "badge-success"}
    if result == "merged_manually":
        return {"label": "Merged (manual)", "css": "badge-success badge-outline"}
    if result.startswith("passthrough"):
        return {"label": "Passthrough", "css": "badge-info badge-outline"}
    if result == "pending_manual_review":
        return {"label": "Review", "css": "badge-warning"}
    if result.startswith("skipped"):
        return {"label": "Skipped", "css": "badge-neutral"}
    if result.startswith("error"):
        return {"label": "Error", "css": "badge-error"}
    return {"label": result, "css": "badge-ghost"}


def _job_status_badge(status: str | None) -> dict[str, str]:
    return {
        "queued":          {"label": "queued",          "css": "badge-ghost"},
        "running":         {"label": "running",         "css": "badge-info"},
        "awaiting_delete": {"label": "awaiting delete", "css": "badge-warning"},
        "success":         {"label": "success",         "css": "badge-success"},
        "error":           {"label": "error",           "css": "badge-error"},
    }.get(status or "", {"label": status or "?", "css": "badge-ghost"})


def _parse_strava_start(iso: str | None) -> datetime:
    if not iso:
        raise ValueError("missing start_date")
    s = iso.replace("Z", "+00:00") if iso.endswith("Z") else iso
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def _processed_lookup(source: str = "strava") -> dict[int, dict[str, Any]]:
    async with connect() as db:
        rows = await (await db.execute(
            "SELECT strava_id, external_id, result, notes FROM processed_activities "
            "WHERE source = ?",
            (source,),
        )).fetchall()
    return {int(r["strava_id"]): dict(r) for r in rows}


def _shape_activity(raw: dict[str, Any], processed: dict[int, dict[str, Any]]) -> dict[str, Any]:
    sid = int(raw["id"])
    proc = processed.get(sid)
    # Cheap GPS-presence signal: Strava summary payload includes start_latlng
    # / end_latlng. Empty list (or missing) means no GPS recorded — i.e., the
    # activity was indoors or recorded on a non-GPS head unit like a wheel-
    # sensor Edge. We use this purely as a list-view hint; the preview will
    # do the real merge.
    start_ll = raw.get("start_latlng") or []
    has_gps = bool(start_ll) and start_ll != [0, 0]
    return {
        "id": sid,
        "name": raw.get("name") or "(unnamed)",
        "type": raw.get("type") or "",
        "device_name": raw.get("device_name") or "",
        "distance_km": (raw.get("distance") or 0) / 1000.0,
        "moving_time": _format_duration(raw.get("moving_time")),
        "start": _format_date(raw.get("start_date_local")),
        "badge": _badge_for(proc["result"] if proc else None),
        "has_gps": has_gps,
    }


# Settings table helpers ------------------------------------------------------

async def _setting_get(key: str, default: str) -> str:
    async with connect() as db:
        row = await (await db.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        )).fetchone()
    return row["value"] if row else default


async def _setting_set(key: str, value: str) -> None:
    async with connect() as db:
        await db.execute(
            """
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await db.commit()


def _truthy(s: str) -> bool:
    return s.strip().lower() in ("1", "true", "yes", "on")


# TCX helpers (preview map) ---------------------------------------------------

TCX_NS = {"tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}


def _tcx_path(tcx_bytes: bytes) -> list[list[float]]:
    """Extract [[lat, lon], ...] from a TCX. Trackpoints lacking position
    are skipped (e.g., tunnels)."""
    try:
        root = etree.fromstring(tcx_bytes)
    except etree.XMLSyntaxError:
        return []
    out: list[list[float]] = []
    for tp in root.iterfind(".//tcd:Trackpoint", TCX_NS):
        pos = tp.find("tcd:Position", TCX_NS)
        if pos is None:
            continue
        lat_el = pos.find("tcd:LatitudeDegrees", TCX_NS)
        lon_el = pos.find("tcd:LongitudeDegrees", TCX_NS)
        if lat_el is None or lon_el is None:
            continue
        try:
            out.append([float(lat_el.text), float(lon_el.text)])
        except (TypeError, ValueError):
            continue
    return out


def _strava_latlng_path(streams: dict[str, Any]) -> list[list[float]]:
    raw = (streams.get("latlng") or {}).get("data") or []
    return [pt for pt in raw if pt and len(pt) == 2 and pt[0] is not None]


def _tcx_chart_series(tcx_bytes: bytes) -> dict[str, Any]:
    """Pull ([epoch_offsets_s], [hr], [lat], [lon], [alt]) arrays out of a
    TCX. ``epoch_offsets_s`` is seconds since the first trackpoint."""
    out: dict[str, Any] = {"t": [], "hr": [], "lat": [], "lon": [], "alt": []}
    try:
        root = etree.fromstring(tcx_bytes)
    except etree.XMLSyntaxError:
        return out
    base: datetime | None = None
    for tp in root.iterfind(".//tcd:Trackpoint", TCX_NS):
        time_el = tp.find("tcd:Time", TCX_NS)
        if time_el is None or not time_el.text:
            continue
        try:
            ts = datetime.fromisoformat(time_el.text.replace("Z", "+00:00"))
        except ValueError:
            continue
        if base is None:
            base = ts
        offset = (ts - base).total_seconds()

        hr_el = tp.find("tcd:HeartRateBpm/tcd:Value", TCX_NS)
        hr = int(hr_el.text) if (hr_el is not None and hr_el.text) else None

        pos = tp.find("tcd:Position", TCX_NS)
        lat = lon = None
        if pos is not None:
            lat_el = pos.find("tcd:LatitudeDegrees", TCX_NS)
            lon_el = pos.find("tcd:LongitudeDegrees", TCX_NS)
            if lat_el is not None and lon_el is not None:
                try:
                    lat = float(lat_el.text)
                    lon = float(lon_el.text)
                except (TypeError, ValueError):
                    lat = lon = None

        alt_el = tp.find("tcd:AltitudeMeters", TCX_NS)
        alt = float(alt_el.text) if (alt_el is not None and alt_el.text) else None

        out["t"].append(offset)
        out["hr"].append(hr)
        out["lat"].append(lat)
        out["lon"].append(lon)
        out["alt"].append(alt)
    return out


def _strava_chart_series(streams: dict[str, Any]) -> dict[str, Any]:
    """Trim Strava streams to the keys we plot, with a shared time axis."""
    times = (streams.get("time") or {}).get("data") or []
    return {
        "t":          times,
        "heartrate":  (streams.get("heartrate") or {}).get("data") or [],
        "cadence":    (streams.get("cadence") or {}).get("data") or [],
        "speed":      (streams.get("velocity_smooth") or {}).get("data") or [],
        "temp":       (streams.get("temp") or {}).get("data") or [],
        "altitude":   (streams.get("altitude") or {}).get("data") or [],
    }


# ---------- index -----------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    ctx: dict[str, Any] = {"connected": False, "activities": [], "error": None}
    try:
        async with StravaClient.open() as client:
            raw = await client.list_recent_activities(limit=50)
        processed = await _processed_lookup()
        ctx["connected"] = True
        ctx["activities"] = [_shape_activity(a, processed) for a in raw]
    except StravaNotConfigured:
        pass
    except httpx.HTTPStatusError as e:
        log.warning("strava.list_failed", status=e.response.status_code)
        ctx["connected"] = True
        ctx["error"] = f"Strava API error: {e.response.status_code} {e.response.reason_phrase}"
    except httpx.HTTPError as e:
        log.warning("strava.list_failed", err=str(e))
        ctx["connected"] = True
        ctx["error"] = f"Network error talking to Strava: {e}"
    return templates.TemplateResponse(request, "index.html", ctx)


# ---------- activity detail --------------------------------------------------

@router.get("/activity/{strava_id}", response_class=HTMLResponse)
async def activity_detail(request: Request, strava_id: int) -> HTMLResponse:
    ctx: dict[str, Any] = {"strava_id": strava_id, "error": None, "activity": None,
                           "stream_summary": [], "badge": _badge_for(None),
                           "processed": None, "gps_path_json": "[]",
                           "gps_point_count": 0}
    try:
        async with StravaClient.open() as client:
            activity = await client.get_activity(strava_id)
            streams = await client.get_streams(strava_id)
    except StravaNotConfigured:
        ctx["error"] = "Strava not connected. Run `stravafit strava login` first."
        return templates.TemplateResponse(request, "activity_detail.html", ctx)
    except httpx.HTTPStatusError as e:
        ctx["error"] = f"Strava {e.response.status_code}: {e.response.text[:200]}"
        return templates.TemplateResponse(request, "activity_detail.html", ctx)

    ctx["activity"] = activity
    ctx["activity_started_at"] = _format_date(activity.get("start_date_local"))
    ctx["activity_distance_km"] = (activity.get("distance") or 0) / 1000.0
    ctx["activity_moving"] = _format_duration(activity.get("moving_time"))
    ctx["start_iso"] = activity.get("start_date") or ""

    # GPS path for the detail map. Empty when Strava has no latlng stream
    # (indoor / non-GPS head unit) — the template branches on this.
    strava_path = _strava_latlng_path(streams or {})
    ctx["gps_path_json"] = json.dumps(strava_path)
    ctx["gps_point_count"] = len(strava_path)

    ctx["stream_summary"] = sorted(
        (
            {
                "key": k,
                "n": len((v or {}).get("data") or []),
                "type": (v or {}).get("type"),
                "resolution": (v or {}).get("resolution"),
            }
            for k, v in (streams or {}).items()
        ),
        key=lambda d: d["key"],
    )

    proc = (await _processed_lookup()).get(strava_id)
    ctx["badge"] = _badge_for(proc["result"] if proc else None)
    ctx["processed"] = proc

    return templates.TemplateResponse(request, "activity_detail.html", ctx)


@router.get("/activity/{strava_id}/_google_matches", response_class=HTMLResponse)
async def activity_google_matches(request: Request, strava_id: int) -> HTMLResponse:
    """HTMX fragment: list candidate Google Health exercises near the activity start."""
    matches: list[dict[str, Any]] = []
    error: str | None = None
    try:
        async with StravaClient.open() as sc:
            activity = await sc.get_activity(strava_id)
        start_dt = _parse_strava_start(activity.get("start_date"))
        async with GoogleHealthClient.open() as gc:
            matches = await gc.find_near(start_dt, window_minutes=120)
    except StravaNotConfigured:
        error = "Strava not connected."
    except GoogleNotConfigured:
        error = "Google Health not connected. Connect from Settings."
    except Exception as e:  # noqa: BLE001 - surface in UI
        error = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(
        request,
        "partials/google_matches.html",
        {"strava_id": strava_id, "matches": matches, "error": error},
    )


@router.post("/activity/{strava_id}/mark-merged", response_class=HTMLResponse,
             dependencies=[Depends(require_htmx)])
async def activity_mark_merged(
    request: Request,
    strava_id: int,
    external_id: str | None = Form(None),
    note: str | None = Form(None),
) -> HTMLResponse:
    """Record that the user manually completed a merge for this activity (e.g.
    they downloaded the merged FIT and uploaded it via Strava UI). Future
    webhook events for this strava_id will be skipped by is_already_processed."""
    await jobs_mod.record_processed(
        strava_id,
        external_id=external_id,
        result="merged_manually",
        notes=(note or "marked merged via dashboard"),
    )
    log.info("dashboard.mark_merged", strava_id=strava_id, external_id=external_id)
    return HTMLResponse(
        '<div class="alert alert-success text-sm">'
        '<span>Marked as merged. Future webhook events for this activity will skip auto-merge.</span>'
        '</div>'
    )


@router.post("/activity/{strava_id}/merge", response_class=HTMLResponse,
             dependencies=[Depends(require_htmx)])
async def activity_merge(
    request: Request,
    strava_id: int,
    bg: BackgroundTasks,
    external_id: str | None = Form(None),
    mode: str = Form(jobs_mod.MODE_AUTO),
) -> HTMLResponse:
    from app.worker import run_merge_job

    if mode not in (jobs_mod.MODE_DRY_RUN, jobs_mod.MODE_AUTO, jobs_mod.MODE_SEMI_AUTO):
        raise HTTPException(400, f"invalid mode {mode!r}")

    job_id = await jobs_mod.enqueue(
        strava_id,
        external_id=external_id,
        trigger="manual",
        mode=mode,
    )
    bg.add_task(run_merge_job, job_id)
    job = await jobs_mod.get(job_id) or {
        "id": job_id, "status": "queued", "strava_id": strava_id,
        "started_at": None, "finished_at": None, "trigger": "manual",
        "dry_run": 1 if mode == jobs_mod.MODE_DRY_RUN else 0,
        "mode": mode, "error": None,
        "external_id": external_id, "log": "", "recovery_path": None,
    }
    return templates.TemplateResponse(
        request, "partials/job_row.html",
        {"job": _shape_job(job)},
    )


# ---------- preview ---------------------------------------------------------

@router.get("/preview/{strava_id}/{external_id}", response_class=HTMLResponse)
async def preview(request: Request, strava_id: int, external_id: str) -> HTMLResponse:
    ctx: dict[str, Any] = {
        "strava_id": strava_id,
        "external_id": external_id,
        "error": None,
        "warnings": [],
        "stats": None,
        "edge_path_json": "[]",
        "source_path_json": "[]",
        "activity_name": "",
    }
    try:
        async with StravaClient.open() as sc:
            activity = await sc.get_activity(strava_id)
            streams = await sc.get_streams(strava_id)
        ctx["activity_name"] = activity.get("name") or ""
        ctx["activity_started_at"] = _format_date(activity.get("start_date_local"))
        start_dt = _parse_strava_start(activity.get("start_date"))

        async with GoogleHealthClient.open() as gc:
            tcx = await gc.get_exercise_tcx(external_id)

        edge_path = _strava_latlng_path(streams)
        source_path = _tcx_path(tcx)

        merge_error: str | None = None
        result_summary: dict[str, Any] | None = None
        try:
            result = merge_streams_to_fit(
                strava_streams=streams,
                strava_start_time=start_dt,
                fitbit_tcx=tcx,
                activity_name=activity.get("name") or "Merged ride",
            )
            result_summary = {
                "bytes": len(result.fit_bytes),
                "records": result.record_count,
                "distance_m": result.distance_meters,
                "gps_source": result.gps_source,
                "warnings": [{"code": w.code, "message": w.message} for w in result.warnings],
            }
        except MergeError as e:
            merge_error = str(e)

        ctx["edge_path_json"] = json.dumps(edge_path)
        ctx["source_path_json"] = json.dumps(source_path)
        ctx["edge_point_count"] = len(edge_path)
        ctx["source_point_count"] = len(source_path)
        ctx["stats"] = result_summary
        ctx["merge_error"] = merge_error
        ctx["strava_series_json"] = json.dumps(_strava_chart_series(streams))
        ctx["source_series_json"] = json.dumps(_tcx_chart_series(tcx))

    except StravaNotConfigured:
        ctx["error"] = "Strava not connected."
    except GoogleNotConfigured:
        ctx["error"] = "Google Health not connected."
    except Exception as e:  # noqa: BLE001 - surface in UI
        ctx["error"] = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(request, "preview.html", ctx)


@router.get("/preview/{strava_id}/{external_id}/download.fit")
async def preview_download(strava_id: int, external_id: str) -> Response:
    """Run the merge inline and return the FIT bytes as a download. The
    safest workflow — no Strava write, user uploads manually."""
    try:
        async with StravaClient.open() as sc:
            activity = await sc.get_activity(strava_id)
            streams = await sc.get_streams(strava_id)
        start_dt = _parse_strava_start(activity.get("start_date"))
        async with GoogleHealthClient.open() as gc:
            tcx = await gc.get_exercise_tcx(external_id)
        result = merge_streams_to_fit(
            strava_streams=streams,
            strava_start_time=start_dt,
            fitbit_tcx=tcx,
            activity_name=activity.get("name") or "Merged ride",
        )
    except StravaNotConfigured:
        raise HTTPException(412, "Strava not connected")
    except GoogleNotConfigured:
        raise HTTPException(412, "Google Health not connected")
    except MergeError as e:
        raise HTTPException(422, f"merge failed: {e}")

    filename = f"strava-{strava_id}-merged.fit"
    return Response(
        content=result.fit_bytes,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------- manual ----------------------------------------------------------

@router.get("/manual", response_class=HTMLResponse)
async def manual_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "manual.html", {})


@router.post("/manual", response_class=HTMLResponse)
async def manual_submit(
    request: Request,
    strava_id: int = Form(...),
    external_id: str = Form(...),
) -> RedirectResponse:
    return RedirectResponse(url=f"/preview/{strava_id}/{external_id}", status_code=303)


# ---------- google health recent --------------------------------------------

@router.get("/google/recent", response_class=HTMLResponse)
async def google_recent(request: Request, days: int = 30) -> HTMLResponse:
    """List recent Google Health exercises. ``days`` query param controls the
    look-back window (default 30, sane bounds 1..365)."""
    days = max(1, min(int(days), 365))
    ctx: dict[str, Any] = {"connected": False, "activities": [], "error": None,
                           "days": days}
    try:
        async with GoogleHealthClient.open() as client:
            after = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
            # No explicit page_size: let the client pull full pages (100 each)
            # across all available pages within the window.
            activities = await client.list_exercises(after_date=after)
        ctx["connected"] = True

        def _start_key(a: dict[str, Any]) -> str:
            # Handle both {"exercise": {...}} and {"value": {"exercise": {...}}}.
            val = a.get("value") if isinstance(a.get("value"), dict) else None
            ex = (val or {}).get("exercise") if val else a.get("exercise")
            return ((ex or {}).get("interval") or {}).get("startTime") or ""

        activities.sort(key=_start_key, reverse=True)
        ctx["activities"] = activities
    except GoogleNotConfigured:
        pass
    except httpx.HTTPError as e:
        ctx["connected"] = True
        ctx["error"] = f"Google Health API error: {e}"
    return templates.TemplateResponse(request, "google_recent.html", ctx)


@router.get("/google/exercise/{external_id}", response_class=HTMLResponse)
async def google_exercise_detail(request: Request, external_id: str) -> HTMLResponse:
    """Map + HR chart for a single Google Health exercise, parsed from its TCX."""
    ctx: dict[str, Any] = {
        "external_id": external_id,
        "error": None,
        "path_json": "[]",
        "series_json": json.dumps({"t": [], "hr": [], "alt": []}),
        "point_count": 0,
        "started_iso": None,
        "duration_s": None,
    }
    try:
        async with GoogleHealthClient.open() as gc:
            tcx = await gc.get_exercise_tcx(external_id)
        series = _tcx_chart_series(tcx)
        path = [[lat, lon] for lat, lon in zip(series["lat"], series["lon"])
                if lat is not None and lon is not None]

        # Read first <Time> for display.
        try:
            root = etree.fromstring(tcx)
            first = root.find(".//tcd:Trackpoint/tcd:Time", TCX_NS)
            ctx["started_iso"] = first.text if first is not None else None
        except etree.XMLSyntaxError:
            pass

        ctx["path_json"] = json.dumps(path)
        # uPlot can't plot null y-values; replace with NaN-equivalent for JSON.
        ctx["series_json"] = json.dumps({
            "t": series["t"],
            "hr": [v for v in series["hr"]],
            "alt": [v for v in series["alt"]],
        })
        ctx["point_count"] = len(path)
        if series["t"]:
            ctx["duration_s"] = int(series["t"][-1])

    except GoogleNotConfigured:
        ctx["error"] = "Google Health not connected. Connect from Settings."
    except Exception as e:  # noqa: BLE001 - surface in UI
        ctx["error"] = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(request, "google_exercise.html", ctx)


async def _google_exercise_start(external_id: str) -> datetime:
    """Start time of a Google Health exercise, read from its TCX (there is no
    cheap direct-GET for a single data point)."""
    async with GoogleHealthClient.open() as gc:
        tcx_bytes = await gc.get_exercise_tcx(external_id)
    try:
        root = etree.fromstring(tcx_bytes)
    except etree.XMLSyntaxError:
        raise RuntimeError("could not parse TCX")
    first = root.find(".//tcd:Trackpoint/tcd:Time", TCX_NS)
    if first is None or not first.text:
        raise RuntimeError("TCX has no trackpoints with a Time element")
    return datetime.fromisoformat(first.text.replace("Z", "+00:00"))


@router.get("/google/{external_id}/_strava_matches", response_class=HTMLResponse)
async def google_strava_matches(
    request: Request, external_id: str
) -> HTMLResponse:
    """HTMX fragment: list Strava activities near a Google Health exercise's start time."""
    matches: list[dict[str, Any]] = []
    error: str | None = None
    try:
        when = await _google_exercise_start(external_id)
        async with StravaClient.open() as sc:
            matches = await sc.find_near(when, window_minutes=120)
    except StravaNotConfigured:
        error = "Strava not connected."
    except GoogleNotConfigured:
        error = "Google Health not connected."
    except Exception as e:  # noqa: BLE001 - surface in UI
        error = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(
        request,
        "partials/strava_matches.html",
        {"external_id": external_id, "matches": matches, "error": error},
    )


@router.get("/google/{external_id}/_garmin_matches", response_class=HTMLResponse)
async def google_garmin_matches(
    request: Request, external_id: str
) -> HTMLResponse:
    """HTMX fragment: list Garmin activities near a Google Health exercise's
    start time, with merge actions pinned to this exercise's data point."""
    matches: list[dict[str, Any]] = []
    error: str | None = None
    try:
        when = await _google_exercise_start(external_id)
        async with GarminClient.open() as gc:
            raw = await gc.find_near(when, window_minutes=120)
        for m in raw:
            matches.append({
                "id": int(m["activityId"]),
                "name": m.get("activityName") or "(unnamed)",
                "start": (m.get("startTimeLocal") or m.get("startTimeGMT") or "")[:19],
                "type": ((m.get("activityType") or {}).get("typeKey") or "").replace("_", " "),
            })
    except GarminNotConfigured:
        error = "Garmin not connected. Seed tokens via /settings/garmin-tokens or `stravafit garmin login`."
    except GoogleNotConfigured:
        error = "Google Health not connected."
    except Exception as e:  # noqa: BLE001 - unofficial API; surface in UI
        error = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(
        request,
        "partials/garmin_matches.html",
        {"external_id": external_id, "matches": matches, "error": error},
    )


# ---------- garmin -----------------------------------------------------------

def _shape_garmin_activity(
    raw: dict[str, Any], processed: dict[int, dict[str, Any]]
) -> dict[str, Any]:
    gid = int(raw["activityId"])
    proc = processed.get(gid)
    type_key = (raw.get("activityType") or {}).get("typeKey") or ""
    start_local = (raw.get("startTimeLocal") or raw.get("startTimeGMT") or "")[:16]
    duration_s = raw.get("duration")
    return {
        "id": gid,
        "name": raw.get("activityName") or "(unnamed)",
        "type": type_key.replace("_", " "),
        "distance_km": (raw.get("distance") or 0) / 1000.0,
        "duration": _format_duration(int(duration_s) if duration_s else None),
        "start": start_local,
        "badge": _badge_for(proc["result"] if proc else None),
        "has_gps": bool(raw.get("hasPolyline")),
    }


@router.get("/garmin", response_class=HTMLResponse)
async def garmin_index(request: Request) -> HTMLResponse:
    ctx: dict[str, Any] = {
        "connected": False,
        "activities": [],
        "error": None,
        "jobs": [],
    }
    try:
        async with GarminClient.open() as gc:
            raw = await gc.list_recent_activities(limit=50)
        processed = await _processed_lookup(source="garmin")
        ctx["connected"] = True
        ctx["activities"] = [_shape_garmin_activity(a, processed) for a in raw]
    except GarminNotConfigured as e:
        ctx["error"] = str(e)
    except Exception as e:  # noqa: BLE001 - unofficial API; surface anything in the UI
        log.warning("garmin.list_failed", err=str(e))
        ctx["connected"] = True
        ctx["error"] = f"Garmin Connect error: {e}"
    # Show recent garmin jobs on the same page so the HTMX rows have somewhere to land.
    rows = await jobs_mod.list_recent(50)
    ctx["jobs"] = [_shape_job(j) for j in rows if (j.get("source") or "strava") == "garmin"][:10]
    return templates.TemplateResponse(request, "garmin_recent.html", ctx)


@router.post("/garmin/activity/{garmin_id}/merge", response_class=HTMLResponse,
             dependencies=[Depends(require_htmx)])
async def garmin_activity_merge(
    request: Request,
    garmin_id: int,
    bg: BackgroundTasks,
    external_id: str | None = Form(None),
    mode: str = Form(jobs_mod.MODE_AUTO),
) -> HTMLResponse:
    from app.worker import run_merge_job

    if mode not in (jobs_mod.MODE_DRY_RUN, jobs_mod.MODE_AUTO, jobs_mod.MODE_SEMI_AUTO):
        raise HTTPException(400, f"invalid mode {mode!r}")

    job_id = await jobs_mod.enqueue(
        garmin_id,
        external_id=external_id,
        trigger="manual",
        mode=mode,
        source=jobs_mod.SOURCE_GARMIN,
    )
    bg.add_task(run_merge_job, job_id)
    job = await jobs_mod.get(job_id) or {
        "id": job_id, "status": "queued", "strava_id": garmin_id,
        "started_at": None, "finished_at": None, "trigger": "manual",
        "dry_run": 1 if mode == jobs_mod.MODE_DRY_RUN else 0,
        "mode": mode, "error": None, "source": "garmin",
        "external_id": external_id, "log": "", "recovery_path": None,
    }
    return templates.TemplateResponse(
        request, "partials/job_row.html",
        {"job": _shape_job(job)},
    )


# ---------- garmin preview ----------------------------------------------------

async def _garmin_fit_and_meta(garmin_id: int) -> tuple[Any, str, Any]:
    """(parsed FIT, activity name, start datetime) for a Garmin activity."""
    from app.garmin.fitparse import parse_fit_streams
    from app.garmin.client import parse_start_gmt

    async with GarminClient.open() as gc:
        summary = await gc.get_activity(garmin_id)
        fit_bytes = await gc.download_original_fit(garmin_id)
    parsed = parse_fit_streams(fit_bytes)
    name = ""
    start_dt = parsed.start_time
    if summary:
        name = summary.get("activityName") or ""
        start_dt = parse_start_gmt(summary) or start_dt
    return parsed, name, start_dt


@router.get("/garmin/preview/{garmin_id}/{external_id}", response_class=HTMLResponse)
async def garmin_preview(request: Request, garmin_id: int, external_id: str) -> HTMLResponse:
    """Same diff view as the Strava preview, sourced from the Garmin FIT:
    both GPS tracks on one map, both stream sets charted, and the merge
    result (or its error) — without touching anything."""
    ctx: dict[str, Any] = {
        "preview_source": "garmin",
        "strava_id": garmin_id,  # template variable name is legacy
        "external_id": external_id,
        "error": None,
        "warnings": [],
        "stats": None,
        "edge_path_json": "[]",
        "source_path_json": "[]",
        "activity_name": "",
    }
    try:
        parsed, name, start_dt = await _garmin_fit_and_meta(garmin_id)
        ctx["activity_name"] = name
        ctx["activity_started_at"] = start_dt.strftime("%Y-%m-%d %H:%M UTC")

        async with GoogleHealthClient.open() as gc:
            tcx = await gc.get_exercise_tcx(external_id)

        edge_path = _strava_latlng_path(parsed.streams)
        source_path = _tcx_path(tcx)

        merge_error: str | None = None
        result_summary: dict[str, Any] | None = None
        try:
            result = merge_streams_to_fit(
                strava_streams=parsed.streams,
                strava_start_time=start_dt,
                source_tcx=tcx,
                activity_name=name or "Merged ride",
            )
            result_summary = {
                "bytes": len(result.fit_bytes),
                "records": result.record_count,
                "distance_m": result.distance_meters,
                "gps_source": result.gps_source,
                "warnings": [{"code": w.code, "message": w.message} for w in result.warnings],
            }
        except MergeError as e:
            merge_error = str(e)

        ctx["edge_path_json"] = json.dumps(edge_path)
        ctx["source_path_json"] = json.dumps(source_path)
        ctx["edge_point_count"] = len(edge_path)
        ctx["source_point_count"] = len(source_path)
        ctx["stats"] = result_summary
        ctx["merge_error"] = merge_error
        ctx["strava_series_json"] = json.dumps(_strava_chart_series(parsed.streams))
        ctx["source_series_json"] = json.dumps(_tcx_chart_series(tcx))

    except GarminNotConfigured:
        ctx["error"] = "Garmin not connected."
    except GoogleNotConfigured:
        ctx["error"] = "Google Health not connected."
    except Exception as e:  # noqa: BLE001 - surface in UI
        ctx["error"] = f"{type(e).__name__}: {e}"

    return templates.TemplateResponse(request, "preview.html", ctx)


@router.get("/garmin/preview/{garmin_id}/{external_id}/download.fit")
async def garmin_preview_download(garmin_id: int, external_id: str) -> Response:
    """Run the merge inline (Garmin FIT + Google TCX) and return the FIT
    bytes as a download — no writes anywhere."""
    try:
        parsed, name, start_dt = await _garmin_fit_and_meta(garmin_id)
        async with GoogleHealthClient.open() as gc:
            tcx = await gc.get_exercise_tcx(external_id)
        result = merge_streams_to_fit(
            strava_streams=parsed.streams,
            strava_start_time=start_dt,
            source_tcx=tcx,
            activity_name=name or "Merged ride",
        )
    except GarminNotConfigured:
        raise HTTPException(412, "Garmin not connected")
    except GoogleNotConfigured:
        raise HTTPException(412, "Google Health not connected")
    except MergeError as e:
        raise HTTPException(422, f"merge failed: {e}")

    filename = f"garmin-{garmin_id}-merged.fit"
    return Response(
        content=result.fit_bytes,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------- dreeve export hand-off -------------------------------------------
#
# The Garmin worker drops definitive FITs into <data>/export/. A cron on the
# Dreeve host lists them (plain text), downloads each, moves it into Dreeve's
# watch folder, then DELETEs the export. All routes sit behind the global
# Basic-auth middleware. DELETE is not forgeable cross-origin by a browser
# form, so no HTMX/CSRF dependency is needed.

_EXPORT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,120}\.fit$")


def _export_file(name: str) -> Path:
    if not _EXPORT_NAME_RE.match(name):
        raise HTTPException(400, "invalid export name")
    return settings.dreeve_export_dir / name


@router.get("/export/", response_class=Response)
async def export_list() -> Response:
    d = settings.dreeve_export_dir
    names = sorted(p.name for p in d.glob("*.fit")) if d.is_dir() else []
    return Response("\n".join(names) + ("\n" if names else ""), media_type="text/plain")


@router.get("/export/{name}")
async def export_get(name: str) -> FileResponse:
    path = _export_file(name)
    if not path.is_file():
        raise HTTPException(404, "no such export")
    return FileResponse(path, media_type="application/octet-stream", filename=name)


@router.delete("/export/{name}")
async def export_ack(name: str) -> Response:
    path = _export_file(name)
    if not path.is_file():
        raise HTTPException(404, "no such export")
    path.unlink()
    log.info("dreeve.export_acked", name=name)
    return Response(status_code=204)


# ---------- garmin token seeding ----------------------------------------------

@router.post("/settings/garmin-tokens")
async def garmin_tokens_import(request: Request) -> dict[str, str]:
    """Seed the Garmin token cache with a garmin_tokens.json payload (the
    format python-garminconnect and the dreeve-garmin-connector both dump).
    Lets an already-authenticated session be reused instead of doing a fresh
    credential login (Garmin rate-limits accounts that log in repeatedly).

    JSON body only — a cross-origin browser form can't send application/json
    without a CORS preflight, so this needs no HTMX header. Basic auth applies.
    """
    if "application/json" not in (request.headers.get("content-type") or ""):
        raise HTTPException(415, "send the garmin_tokens.json content as application/json")
    raw = await request.body()
    try:
        json.loads(raw)
    except ValueError as exc:
        raise HTTPException(400, f"not valid JSON: {exc}") from exc
    tokens_dir = Path(settings.garmin_tokens_path).expanduser()
    tokens_dir.mkdir(parents=True, exist_ok=True)
    out = tokens_dir / "garmin_tokens.json"
    out.write_bytes(raw)
    log.info("garmin.tokens_imported", path=str(out), bytes=len(raw))
    return {"status": "ok", "path": str(out)}


# ---------- jobs -------------------------------------------------------------

def _shape_job(j: dict[str, Any]) -> dict[str, Any]:
    rp = j.get("recovery_path")
    has_recovery = bool(rp) and Path(rp).is_file() if rp else False
    return {
        "id": j["id"],
        "strava_id": j["strava_id"],
        "external_id": j.get("external_id"),
        "trigger": j["trigger"],
        "status": j["status"],
        "status_badge": _job_status_badge(j["status"]),
        "mode": j.get("mode") or ("dry_run" if j.get("dry_run") else "auto"),
        "dry_run": bool(j.get("dry_run")),
        "started": _format_unix(j.get("started_at")),
        "finished": _format_unix(j.get("finished_at")),
        "error": j.get("error"),
        "log": (j.get("log") or "").strip(),
        "has_recovery": has_recovery,
        "awaiting_delete": (j.get("status") == "awaiting_delete"),
        "source": j.get("source") or "strava",
    }


@router.get("/jobs", response_class=HTMLResponse)
async def jobs_page(request: Request) -> HTMLResponse:
    rows = await jobs_mod.list_recent(50)
    return templates.TemplateResponse(
        request, "jobs.html",
        {"jobs": [_shape_job(j) for j in rows]},
    )


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_row(request: Request, job_id: int) -> HTMLResponse:
    """HTMX polling target — returns one job row."""
    j = await jobs_mod.get(job_id)
    if j is None:
        raise HTTPException(404, "job not found")
    return templates.TemplateResponse(
        request, "partials/job_row.html",
        {"job": _shape_job(j)},
    )


@router.post("/jobs/{job_id}/finish-upload", response_class=HTMLResponse,
             dependencies=[Depends(require_htmx)])
async def job_finish_upload(
    request: Request,
    job_id: int,
    bg: BackgroundTasks,
) -> HTMLResponse:
    """User has (presumably) deleted the original on Strava UI; now upload
    the merged FIT from disk. Triggers worker.resume_upload as a background
    task and returns the updated job row so HTMX can poll it to completion."""
    from app.worker import resume_upload

    j = await jobs_mod.get(job_id)
    if j is None:
        raise HTTPException(404, "job not found")
    if j["status"] != "awaiting_delete":
        raise HTTPException(409, f"job is in status {j['status']!r}, not awaiting_delete")

    bg.add_task(resume_upload, job_id)
    # Optimistically render the row in 'running' state — the polling will
    # converge to the real state.
    j["status"] = "running"
    return templates.TemplateResponse(
        request, "partials/job_row.html",
        {"job": _shape_job(j)},
    )


@router.get("/jobs/{job_id}/recovery.fit")
async def job_recovery_download(job_id: int) -> FileResponse:
    """Serve the on-disk recovery FIT for a job whose Strava replace failed."""
    j = await jobs_mod.get(job_id)
    if j is None:
        raise HTTPException(404, "job not found")
    rp = j.get("recovery_path")
    if not rp:
        raise HTTPException(404, "no recovery file for this job")
    p = Path(rp)
    if not p.is_file():
        raise HTTPException(410, "recovery file is no longer on disk")
    return FileResponse(
        path=str(p),
        media_type="application/octet-stream",
        filename=f"strava-{j['strava_id']}-merged.fit",
    )


# ---------- settings --------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(
    request: Request,
    connected: str | None = Query(None),
    auth_error: str | None = Query(None),
) -> HTMLResponse:
    auto_merge = _truthy(await _setting_get("auto_merge_enabled", "true"))
    default_dry_run = _truthy(await _setting_get("default_dry_run", "false"))
    strava_tok = await token_store.load("strava")
    google_tok = await token_store.load("google")
    return templates.TemplateResponse(
        request, "settings.html",
        {
            "auto_merge_enabled": auto_merge,
            "default_dry_run": default_dry_run,
            "strava_expires_at": _format_unix(strava_tok.expires_at) if strava_tok else None,
            "google_expires_at": _format_unix(google_tok.expires_at) if google_tok else None,
            "strava_expired": (strava_tok.expired if strava_tok else None),
            "google_expired": (google_tok.expired if google_tok else None),
            "strava_connected": strava_tok is not None,
            "google_connected": google_tok is not None,
            "flash_connected": connected,
            "flash_error": auth_error,
        },
    )


@router.post("/settings/toggle", response_class=HTMLResponse,
             dependencies=[Depends(require_htmx)])
async def settings_toggle(
    request: Request,
    key: str = Form(...),
) -> HTMLResponse:
    if key not in ("auto_merge_enabled", "default_dry_run"):
        raise HTTPException(400, "unknown setting key")
    cur = _truthy(await _setting_get(key, "false"))
    new = not cur
    await _setting_set(key, "true" if new else "false")
    log.info("settings.toggle", key=key, value=new)
    return templates.TemplateResponse(
        request, "partials/setting_toggle.html",
        {"key": key, "value": new},
    )
