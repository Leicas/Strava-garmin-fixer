from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

import httpx
import structlog

from app import tokens as token_store
from app.fitbit import auth

log = structlog.get_logger()

API_BASE = "https://api.fitbit.com"


class FitbitNotConfigured(RuntimeError):
    """Raised when no Fitbit tokens are stored. Caller should prompt for login."""


def _parse_fitbit_start(activity: dict[str, Any]) -> datetime | None:
    """Fitbit activities expose `startTime` as ISO-8601 with a timezone offset
    (e.g. '2025-04-12T07:32:00.000-04:00'). Older shapes may use
    `originalStartTime`. Return a tz-aware datetime, or None if unparseable."""
    raw = activity.get("startTime") or activity.get("originalStartTime")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


class FitbitClient:
    def __init__(self, http: httpx.AsyncClient, current: token_store.TokenPair) -> None:
        self._http = http
        self._tokens = current

    @classmethod
    @asynccontextmanager
    async def open(cls, timeout: float = 30.0) -> AsyncIterator["FitbitClient"]:
        current = await token_store.load("fitbit")
        if current is None:
            raise FitbitNotConfigured(
                "No Fitbit tokens stored. Run `stravafit fitbit login`."
            )
        async with httpx.AsyncClient(timeout=timeout) as http:
            yield cls(http, current)

    @property
    def tokens(self) -> token_store.TokenPair:
        return self._tokens

    async def _ensure_fresh(self) -> None:
        if self._tokens.expired:
            log.info("fitbit.refresh", reason="expiry")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("fitbit", self._tokens)

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        await self._ensure_fresh()
        url = f"{API_BASE}{path}"
        headers = kwargs.pop("headers", {}) | {
            "Authorization": f"Bearer {self._tokens.access_token}"
        }

        resp = await self._http.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401:
            log.info("fitbit.refresh", reason="401")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("fitbit", self._tokens)
            headers["Authorization"] = f"Bearer {self._tokens.access_token}"
            resp = await self._http.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    async def list_activities(
        self,
        *,
        after_date: str,
        limit: int = 20,
        sort: str = "asc",
    ) -> list[dict[str, Any]]:
        """GET /1/user/-/activities/list.json?afterDate=YYYY-MM-DD&sort=asc&limit=N.
        Fitbit wraps activities in {"activities": [...], "pagination": {...}};
        this returns the inner list."""
        # Fitbit requires `offset=0` to be sent alongside afterDate/sort/limit.
        r = await self._request(
            "GET",
            "/1/user/-/activities/list.json",
            params={
                "afterDate": after_date,
                "sort": sort,
                "limit": limit,
                "offset": 0,
            },
        )
        payload = r.json()
        return list(payload.get("activities", []))

    async def get_activity_tcx(
        self,
        log_id: int,
        *,
        include_partial: bool = True,
    ) -> bytes:
        """GET /1/user/-/activities/{logId}.tcx?includePartialTCX=true. Returns raw TCX bytes."""
        r = await self._request(
            "GET",
            f"/1/user/-/activities/{log_id}.tcx",
            params={"includePartialTCX": "true" if include_partial else "false"},
        )
        return r.content

    async def find_near(
        self,
        when: datetime,
        *,
        window_minutes: int = 120,
    ) -> list[dict[str, Any]]:
        """Find Fitbit activities whose start time is within +/- window_minutes
        of `when`. Sorted ascending by absolute time delta."""
        # Normalize `when` to a tz-aware datetime so subtraction is well-defined.
        if when.tzinfo is None:
            when_aware = when.replace(tzinfo=timezone.utc)
        else:
            when_aware = when

        after_date = (when_aware - timedelta(days=1)).date().isoformat()
        candidates = await self.list_activities(after_date=after_date, limit=20, sort="asc")

        window = timedelta(minutes=window_minutes)
        scored: list[tuple[timedelta, dict[str, Any]]] = []
        for activity in candidates:
            started = _parse_fitbit_start(activity)
            if started is None:
                continue
            delta = abs(started - when_aware)
            if delta <= window:
                scored.append((delta, activity))

        scored.sort(key=lambda pair: pair[0])
        return [activity for _, activity in scored]
