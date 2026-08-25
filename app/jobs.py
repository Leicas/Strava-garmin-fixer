from __future__ import annotations

import time
from typing import Any

from app.db import connect

LOOP_MARKER: str = "[merged-by-stravafit]"

MODE_DRY_RUN = "dry_run"
MODE_AUTO = "auto"
MODE_SEMI_AUTO = "semi_auto"
_VALID_MODES = (MODE_DRY_RUN, MODE_AUTO, MODE_SEMI_AUTO)

SOURCE_STRAVA = "strava"
SOURCE_GARMIN = "garmin"
_VALID_SOURCES = (SOURCE_STRAVA, SOURCE_GARMIN)


async def enqueue(
    strava_id: int,
    *,
    external_id: str | None = None,
    trigger: str,
    mode: str = MODE_AUTO,
    source: str = SOURCE_STRAVA,
) -> int:
    """Insert a queued job and return its id.

    ``strava_id`` is the source activity id — for source='garmin' jobs it
    holds the Garmin activityId (the column name is legacy).
    """
    if mode not in _VALID_MODES:
        raise ValueError(f"invalid mode {mode!r}, want one of {_VALID_MODES}")
    if source not in _VALID_SOURCES:
        raise ValueError(f"invalid source {source!r}, want one of {_VALID_SOURCES}")
    dry_run_legacy = 1 if mode == MODE_DRY_RUN else 0
    async with connect() as db:
        cursor = await db.execute(
            """
            INSERT INTO jobs (strava_id, external_id, trigger, status, dry_run, mode, log, source)
            VALUES (?, ?, ?, 'queued', ?, ?, '', ?)
            """,
            (strava_id, external_id, trigger, dry_run_legacy, mode, source),
        )
        await db.commit()
        job_id = cursor.lastrowid
    assert job_id is not None
    return int(job_id)


async def set_status(job_id: int, status: str) -> None:
    """Set an arbitrary job status. Used for transitions like 'awaiting_delete'."""
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (status, job_id),
        )
        await db.commit()


async def mark_running(job_id: int) -> None:
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
            (int(time.time()), job_id),
        )
        await db.commit()


async def mark_success(job_id: int) -> None:
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET status = 'success', finished_at = ? WHERE id = ?",
            (int(time.time()), job_id),
        )
        await db.commit()


async def mark_error(job_id: int, err: str) -> None:
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET status = 'error', finished_at = ?, error = ? WHERE id = ?",
            (int(time.time()), err, job_id),
        )
        await db.commit()


async def append_log(job_id: int, line: str) -> None:
    """Append a newline-prefixed line to jobs.log column."""
    async with connect() as db:
        row = await (
            await db.execute("SELECT log FROM jobs WHERE id = ?", (job_id,))
        ).fetchone()
        if row is None:
            return
        existing = (row["log"] or "").rstrip()
        new_log = f"{existing}\n{line}" if existing else line
        await db.execute(
            "UPDATE jobs SET log = ? WHERE id = ?",
            (new_log, job_id),
        )
        await db.commit()


async def get(job_id: int) -> dict[str, Any] | None:
    async with connect() as db:
        row = await (
            await db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        ).fetchone()
    return dict(row) if row is not None else None


async def list_recent(limit: int = 50) -> list[dict[str, Any]]:
    async with connect() as db:
        rows = await (
            await db.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
        ).fetchall()
    return [dict(r) for r in rows]


async def record_processed(
    strava_id: int,
    *,
    external_id: str | None,
    result: str,
    notes: str | None = None,
    source: str = SOURCE_STRAVA,
) -> None:
    """Upsert into processed_activities. ``strava_id`` holds the Garmin
    activityId when source='garmin' (legacy column name; ids are disjoint
    ranges in practice)."""
    async with connect() as db:
        await db.execute(
            """
            INSERT INTO processed_activities (strava_id, external_id, merged_at, result, notes, source)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(strava_id) DO UPDATE SET
                external_id = excluded.external_id,
                merged_at   = excluded.merged_at,
                result      = excluded.result,
                notes       = excluded.notes,
                source      = excluded.source
            """,
            (strava_id, external_id, int(time.time()), result, notes, source),
        )
        await db.commit()


async def set_recovery_path(job_id: int, path: str | None) -> None:
    """Record (or clear) the on-disk merged-FIT recovery file for a job."""
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET recovery_path = ? WHERE id = ?",
            (path, job_id),
        )
        await db.commit()


async def is_already_processed(strava_id: int, source: str = SOURCE_STRAVA) -> bool:
    """True if the activity has been handled and the webhook or poller should
    skip it: ``success`` (full auto-replace), ``merged_manually`` (user
    uploaded the merged FIT themselves and clicked Mark as Merged), or
    ``passthrough:*`` (delivered as-is to Dreeve, no HR source to merge)."""
    async with connect() as db:
        row = await (
            await db.execute(
                "SELECT 1 FROM processed_activities WHERE strava_id = ? AND source = ? "
                "AND (result IN ('success', 'merged_manually') OR result LIKE 'passthrough%')",
                (strava_id, source),
            )
        ).fetchone()
    return row is not None


async def has_active_job(strava_id: int, source: str = SOURCE_STRAVA) -> bool:
    """True if a job for this activity is still in flight (queued, running,
    or paused awaiting the manual delete step). Used by the Garmin poller to
    avoid double-enqueueing on every tick."""
    async with connect() as db:
        row = await (
            await db.execute(
                "SELECT 1 FROM jobs WHERE strava_id = ? AND source = ? "
                "AND status IN ('queued', 'running', 'awaiting_delete')",
                (strava_id, source),
            )
        ).fetchone()
    return row is not None
