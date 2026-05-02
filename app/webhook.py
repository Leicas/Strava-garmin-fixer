from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import structlog

from app.config import settings
from app.db import connect

log = structlog.get_logger()
router = APIRouter(prefix="/webhook", tags=["webhook"])


class StravaEvent(BaseModel):
    object_type: str
    object_id: int
    aspect_type: str
    owner_id: int
    subscription_id: int | None = None
    event_time: int | None = None
    updates: dict | None = None


@router.get("/strava")
async def verify(
    hub_mode: str = Query(alias="hub.mode"),
    hub_challenge: str = Query(alias="hub.challenge"),
    hub_verify_token: str = Query(alias="hub.verify_token"),
):
    """Strava subscription verification handshake.

    Strava sends ``hub.mode``, ``hub.challenge``, ``hub.verify_token`` as query
    params. Because dots are not valid in Python identifiers, FastAPI's
    ``Query(alias=...)`` is used to bind them to underscore-named locals.

    Likewise, the response key ``hub.challenge`` cannot be expressed as a kwarg,
    so we build the dict literal and return it via ``JSONResponse`` directly.
    """
    if hub_mode != "subscribe":
        raise HTTPException(status_code=400, detail="hub.mode must be 'subscribe'")
    if hub_verify_token != settings.verify_token:
        raise HTTPException(status_code=403, detail="hub.verify_token mismatch")
    log.info("webhook.verify_ok")
    return JSONResponse({"hub.challenge": hub_challenge})


@router.post("/strava")
async def receive_event(event: StravaEvent, bg: BackgroundTasks) -> dict[str, str]:
    """Strava event receiver. Must respond within ~2s, so we offload work.

    Strava POSTs are NOT signed. The strongest filter is the owner_id check
    below — when STRAVA_OWNER_ID is set, we drop events for any other athlete.
    """
    log.info(
        "webhook.event",
        obj=event.object_type,
        aspect=event.aspect_type,
        oid=event.object_id,
        owner=event.owner_id,
    )

    if (
        settings.strava_owner_id is not None
        and event.owner_id != settings.strava_owner_id
    ):
        log.warning(
            "webhook.owner_mismatch",
            received=event.owner_id,
            expected=settings.strava_owner_id,
        )
        return {"status": "ignored", "reason": "owner_id mismatch"}

    if event.object_type != "activity" or event.aspect_type != "create":
        return {
            "status": "ignored",
            "reason": f"object_type={event.object_type} aspect={event.aspect_type}",
        }

    bg.add_task(_dispatch, event.object_id)
    return {"status": "accepted"}


async def _setting_bool(key: str, default: bool) -> bool:
    async with connect() as db:
        row = await (
            await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        ).fetchone()
    if not row:
        return default
    return str(row["value"]).strip().lower() in ("1", "true", "yes", "on")


async def _dispatch(strava_id: int) -> None:
    """Background task. Filter, then either enqueue a merge job or record pending review."""
    # Imports done lazily so the module can be imported even before the worker
    # agent finishes wiring up jobs/strava/worker.
    from app import jobs
    from app.jobs import LOOP_MARKER
    from app.strava.client import StravaClient, StravaNotConfigured
    from app.worker import run_merge_job

    bound = log.bind(strava_id=strava_id)

    # Loop prevention via processed_activities table -- cheap.
    if await jobs.is_already_processed(strava_id):
        bound.info("webhook.skip", reason="already_processed")
        return

    # Fetch the activity to apply the rest of the filter rules.
    try:
        async with StravaClient.open() as sc:
            act = await sc.get_activity(strava_id)
    except StravaNotConfigured:
        bound.warning("webhook.skip", reason="strava_not_configured")
        return
    except Exception as exc:
        bound.error("webhook.fetch_failed", err=str(exc))
        return

    # Skip our own re-uploads (loop-prevention marker in description).
    if LOOP_MARKER in (act.get("description") or ""):
        bound.info("webhook.skip", reason="loop_marker_present")
        await jobs.record_processed(
            strava_id,
            fitbit_log_id=None,
            result="skipped:self_upload",
            notes="LOOP_MARKER detected in description",
        )
        return

    # Activity-type filter. Plan says Ride / VirtualRide.
    if act.get("type") not in {"Ride", "VirtualRide"}:
        bound.info("webhook.skip", reason="not_a_ride", type=act.get("type"))
        return

    # Auto-merge kill switch.
    auto = await _setting_bool("auto_merge_enabled", True)
    if not auto:
        bound.info("webhook.kill_switch_off", action="pending_manual_review")
        await jobs.record_processed(
            strava_id,
            fitbit_log_id=None,
            result="pending_manual_review",
            notes="auto_merge_enabled=false; review in dashboard",
        )
        return

    # Enqueue + run.
    job_id = await jobs.enqueue(strava_id, trigger="webhook", dry_run=False)
    bound.info("webhook.enqueued", job_id=job_id)
    try:
        await run_merge_job(job_id)
    except Exception as exc:
        bound.error("webhook.merge_failed", err=str(exc))
