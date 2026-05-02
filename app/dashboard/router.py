from __future__ import annotations

import json
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
from app.db import connect
from app.google_health.client import GoogleHealthClient, GoogleNotConfigured
from app.merge import MergeError, merge_streams_to_fit
from app.security import require_htmx
from app.strava.client import StravaClient, StravaNotConfigured

log = structlog.get_logger()

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

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
    if result == "pending_manual_review":
        return {"label": "Review", "css": "badge-warning"}
    if result.startswith("skipped"):
        return {"label": "Skipped", "css": "badge-neutral"}
    if result.startswith("error"):
        return {"label": "Error", "css": "badge-error"}
    return {"label": result, "css": "badge-ghost"}


def _job_status_badge(status: str | None) -> dict[str, str]:
    return {
        "queued":  {"label": "queued",  "css": "badge-ghost"},
        "running": {"label": "running", "css": "badge-info"},
        "success": {"label": "success", "css": "badge-success"},
        "error":   {"label": "error",   "css": "badge-error"},
    }.get(status or "", {"label": status or "?", "css": "badge-ghost"})


def _parse_strava_start(iso: str | None) -> datetime:
    if not iso:
        raise ValueError("missing start_date")
    s = iso.replace("Z", "+00:00") if iso.endswith("Z") else iso
    dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def _processed_lookup() -> dict[int, dict[str, Any]]:
    async with connect() as db:
        rows = await (await db.execute(
            "SELECT strava_id, external_id, result, notes FROM processed_activities"
        )).fetchall()
    return {int(r["strava_id"]): dict(r) for r in rows}


def _shape_activity(raw: dict[str, Any], processed: dict[int, dict[str, Any]]) -> dict[str, Any]:
    sid = int(raw["id"])
    proc = processed.get(sid)
    return {
        "id": sid,
        "name": raw.get("name") or "(unnamed)",
        "type": raw.get("type") or "",
        "device_name": raw.get("device_name") or "",
        "distance_km": (raw.get("distance") or 0) / 1000.0,
        "moving_time": _format_duration(raw.get("moving_time")),
        "start": _format_date(raw.get("start_date_local")),
        "badge": _badge_for(proc["result"] if proc else None),
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
                           "processed": None}
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
    dry_run: bool = Form(False),
) -> HTMLResponse:
    from app.worker import run_merge_job

    job_id = await jobs_mod.enqueue(
        strava_id,
        external_id=external_id,
        trigger="manual",
        dry_run=dry_run,
    )
    bg.add_task(run_merge_job, job_id)
    job = await jobs_mod.get(job_id) or {"id": job_id, "status": "queued",
                                         "strava_id": strava_id, "started_at": None,
                                         "finished_at": None, "trigger": "manual",
                                         "dry_run": int(dry_run), "error": None,
                                         "external_id": external_id, "log": ""}
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
            activities = await client.list_exercises(after_date=after, page_size=25)
        ctx["connected"] = True
        activities.sort(
            key=lambda a: (
                ((a.get("exercise") or {}).get("interval") or {}).get("startTime") or ""
            ),
            reverse=True,
        )
        ctx["activities"] = activities
    except GoogleNotConfigured:
        pass
    except httpx.HTTPError as e:
        ctx["connected"] = True
        ctx["error"] = f"Google Health API error: {e}"
    return templates.TemplateResponse(request, "google_recent.html", ctx)


@router.get("/google/{external_id}/_strava_matches", response_class=HTMLResponse)
async def google_strava_matches(
    request: Request, external_id: str
) -> HTMLResponse:
    """HTMX fragment: list Strava activities near a Google Health exercise's start time."""
    matches: list[dict[str, Any]] = []
    error: str | None = None
    try:
        async with GoogleHealthClient.open() as gc:
            # Fetch the exercise to read its start time.
            tcx = None  # not actually needed
            # Cheaper: list_exercises filters by civil date, so fall back to
            # a direct GET of the data point would be ideal but we don't have
            # that endpoint. Use the start time from the TCX.
            tcx_bytes = await gc.get_exercise_tcx(external_id)
        # Parse first <Time> from the TCX as the exercise start.
        from lxml import etree
        try:
            root = etree.fromstring(tcx_bytes)
        except etree.XMLSyntaxError:
            raise RuntimeError("could not parse TCX")
        first = root.find(".//tcd:Trackpoint/tcd:Time", TCX_NS)
        if first is None or not first.text:
            raise RuntimeError("TCX has no trackpoints with a Time element")
        when = datetime.fromisoformat(first.text.replace("Z", "+00:00"))

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
        "dry_run": bool(j.get("dry_run")),
        "started": _format_unix(j.get("started_at")),
        "finished": _format_unix(j.get("finished_at")),
        "error": j.get("error"),
        "log": (j.get("log") or "").strip(),
        "has_recovery": has_recovery,
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
