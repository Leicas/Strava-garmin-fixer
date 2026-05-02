from __future__ import annotations

import time
from typing import Any

from app.db import connect

LOOP_MARKER: str = "[merged-by-stravafit]"


async def enqueue(
    strava_id: int,
    *,
    external_id: str | None = None,
    trigger: str,
    dry_run: bool = False,
) -> int:
    """Insert a queued job and return its id."""
    async with connect() as db:
        cursor = await db.execute(
            """
            INSERT INTO jobs (strava_id, external_id, trigger, status, dry_run, log)
            VALUES (?, ?, ?, 'queued', ?, '')
            """,
            (strava_id, external_id, trigger, 1 if dry_run else 0),
        )
        await db.commit()
        job_id = cursor.lastrowid
    assert job_id is not None
    return int(job_id)


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
) -> None:
    """Upsert into processed_activities."""
    async with connect() as db:
        await db.execute(
            """
            INSERT INTO processed_activities (strava_id, external_id, merged_at, result, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(strava_id) DO UPDATE SET
                external_id = excluded.external_id,
                merged_at   = excluded.merged_at,
                result      = excluded.result,
                notes       = excluded.notes
            """,
            (strava_id, external_id, int(time.time()), result, notes),
        )
        await db.commit()


async def is_already_processed(strava_id: int) -> bool:
    async with connect() as db:
        row = await (
            await db.execute(
                "SELECT 1 FROM processed_activities WHERE strava_id = ? AND result = 'success'",
                (strava_id,),
            )
        ).fetchone()
    return row is not None
