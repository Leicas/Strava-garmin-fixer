from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

from app.db import connect

Service = Literal["strava", "google"]


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str
    expires_at: int  # unix seconds

    @property
    def expired(self) -> bool:
        return self.expires_at - 60 <= int(time.time())


async def load(service: Service) -> TokenPair | None:
    async with connect() as db:
        row = await (
            await db.execute(
                "SELECT access_token, refresh_token, expires_at FROM tokens WHERE service = ?",
                (service,),
            )
        ).fetchone()
    if row is None:
        return None
    return TokenPair(row["access_token"], row["refresh_token"], int(row["expires_at"]))


async def save(service: Service, tokens: TokenPair) -> None:
    async with connect() as db:
        await db.execute(
            """
            INSERT INTO tokens (service, access_token, refresh_token, expires_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(service) DO UPDATE SET
                access_token  = excluded.access_token,
                refresh_token = excluded.refresh_token,
                expires_at    = excluded.expires_at
            """,
            (service, tokens.access_token, tokens.refresh_token, tokens.expires_at),
        )
        await db.commit()
