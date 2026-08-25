"""Background poller: Garmin Connect has no webhooks for personal accounts,
so this replaces the Strava push subscription as the automatic trigger.

Every ``settings.garmin_poll_minutes`` it lists recent Garmin activities and
enqueues a merge job for each one that is new (within the lookback window),
not already processed, and not already in flight. Respects the same
``auto_merge_enabled`` dashboard toggle as the Strava webhook did.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import structlog

from app import jobs
from app.config import settings
from app.db import connect
from app.garmin.client import GarminClient, GarminNotConfigured, parse_start_gmt

log = structlog.get_logger()


async def _auto_merge_enabled() -> bool:
    async with connect() as db:
        row = await (await db.execute(
            "SELECT value FROM settings WHERE key = 'auto_merge_enabled'"
        )).fetchone()
    return (row["value"] if row else "true").strip().lower() in ("1", "true", "yes", "on")


async def poll_once() -> int:
    """One poll pass. Returns the number of jobs enqueued. Raises on
    Garmin/API errors — the loop catches and logs."""
    if not await _auto_merge_enabled():
        log.debug("garmin_poll.disabled_by_setting")
        return 0

    async with GarminClient.open() as gc:
        activities = await gc.list_recent_activities(limit=15)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=settings.garmin_poll_lookback_hours)
    # Grace period: leave fresh activities alone so the watch has time to
    # sync its HR/GPS to Google Health before we decide merged-vs-passthrough.
    ripe_before = now - timedelta(minutes=settings.garmin_poll_min_age_minutes)
    enqueued = 0
    for a in activities:
        gid_raw = a.get("activityId")
        if gid_raw is None:
            continue
        gid = int(gid_raw)
        started = parse_start_gmt(a)
        if started is None or started < cutoff or started > ripe_before:
            continue
        if await jobs.is_already_processed(gid, source=jobs.SOURCE_GARMIN):
            continue
        if await jobs.has_active_job(gid, source=jobs.SOURCE_GARMIN):
            continue
        job_id = await jobs.enqueue(
            gid,
            trigger="poller",
            mode=settings.garmin_poll_mode,
            source=jobs.SOURCE_GARMIN,
        )
        log.info("garmin_poll.enqueued", garmin_id=gid, job_id=job_id,
                 name=a.get("activityName"))
        enqueued += 1
        from app.worker import run_merge_job

        await run_merge_job(job_id)
    return enqueued


async def poll_loop() -> None:
    """Run forever. Started from the app lifespan when polling is enabled."""
    interval_s = max(60, settings.garmin_poll_minutes * 60)
    log.info("garmin_poll.start", interval_s=interval_s,
             mode=settings.garmin_poll_mode,
             lookback_hours=settings.garmin_poll_lookback_hours)
    await asyncio.sleep(5)  # let startup finish before the first pass
    while True:
        try:
            n = await poll_once()
            if n:
                log.info("garmin_poll.pass_done", enqueued=n)
        except GarminNotConfigured as exc:
            log.warning("garmin_poll.not_configured", err=str(exc))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - the loop must survive anything
            log.exception("garmin_poll.pass_failed")
        await asyncio.sleep(interval_s)
