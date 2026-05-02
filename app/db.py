from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from app.config import settings

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


async def init_db() -> None:
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    async with aiosqlite.connect(settings.database_path) as db:
        await db.executescript(schema)
        # Migration: pre-Google rename of fitbit_log_id -> external_id.
        # SQLite errors if the column doesn't exist, so swallow that case.
        for table in ("processed_activities", "jobs"):
            try:
                await db.execute(
                    f"ALTER TABLE {table} RENAME COLUMN fitbit_log_id TO external_id"
                )
            except aiosqlite.OperationalError:
                pass
        # Migration: add jobs.recovery_path for the on-disk safety net.
        try:
            await db.execute("ALTER TABLE jobs ADD COLUMN recovery_path TEXT")
        except aiosqlite.OperationalError:
            pass
        await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    async with aiosqlite.connect(settings.database_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys = ON")
        yield db
