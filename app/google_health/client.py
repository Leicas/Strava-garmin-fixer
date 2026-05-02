"""Google Health API client (Exercise data type + TCX export).

Replaces FitbitClient. The TCX bytes returned by ``get_exercise_tcx`` are the
same shape the merge function already understands."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator

import httpx
import structlog

from app import tokens as token_store
from app.google_health import auth

log = structlog.get_logger()

API_BASE = "https://health.googleapis.com"

EXERCISE_PARENT = "users/me/dataTypes/exercise"


class GoogleNotConfigured(RuntimeError):
    """Raised when no Google tokens are stored. Caller should prompt for connect."""


def _parse_exercise_start(point: dict[str, Any]) -> datetime | None:
    """Best-effort parse of the start time of an exercise dataPoint.

    Google's documented shape isn't fully pinned down by the public reference;
    we try the most likely paths and return None if nothing parses."""
    candidates = [
        ("exercise", "interval", "startTime"),
        ("exercise", "interval", "start_time"),
        ("interval", "startTime"),
        ("startTime",),
    ]
    raw: str | None = None
    for path in candidates:
        cur: Any = point
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                cur = None
                break
            cur = cur[key]
        if isinstance(cur, str):
            raw = cur
            break
    if raw is None:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _data_point_id(point: dict[str, Any]) -> str | None:
    """Extract the trailing numeric id from `name = "users/me/.../dataPoints/<id>"`."""
    name = point.get("name") or ""
    return name.rsplit("/", 1)[-1] if "/dataPoints/" in name else (name or None)


class GoogleHealthClient:
    def __init__(self, http: httpx.AsyncClient, current: token_store.TokenPair) -> None:
        self._http = http
        self._tokens = current

    @classmethod
    @asynccontextmanager
    async def open(cls, timeout: float = 30.0) -> AsyncIterator["GoogleHealthClient"]:
        current = await token_store.load("google")
        if current is None:
            raise GoogleNotConfigured(
                "No Google Health tokens stored. Connect from the Settings page."
            )
        async with httpx.AsyncClient(timeout=timeout) as http:
            yield cls(http, current)

    @property
    def tokens(self) -> token_store.TokenPair:
        return self._tokens

    async def _ensure_fresh(self) -> None:
        if self._tokens.expired:
            log.info("google.refresh", reason="expiry")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("google", self._tokens)

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        await self._ensure_fresh()
        url = f"{API_BASE}{path}"
        headers = kwargs.pop("headers", {}) | {
            "Authorization": f"Bearer {self._tokens.access_token}"
        }

        resp = await self._http.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401:
            log.info("google.refresh", reason="401")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("google", self._tokens)
            headers["Authorization"] = f"Bearer {self._tokens.access_token}"
            resp = await self._http.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    async def list_exercises(
        self,
        *,
        after_date: str,
        before_date: str | None = None,
        page_size: int = 25,
    ) -> list[dict[str, Any]]:
        """List exercise dataPoints. ``after_date`` / ``before_date`` are
        ``YYYY-MM-DD`` strings (the API filter only supports civil dates)."""
        filter_parts = [f'exercise.interval.civil_start_time >= "{after_date}"']
        if before_date:
            filter_parts.append(f'exercise.interval.civil_start_time < "{before_date}"')
        params = {
            "filter": " AND ".join(filter_parts),
            "pageSize": page_size,
        }
        r = await self._request("GET", f"/v4/{EXERCISE_PARENT}/dataPoints", params=params)
        return list(r.json().get("dataPoints", []))

    async def get_exercise_tcx(
        self,
        data_point_id: str,
        *,
        partial: bool = True,
    ) -> bytes:
        """Fetch the TCX export of one exercise. Returns raw TCX bytes (the
        wire format is JSON ``{"tcxData": "<TCX XML>"}``; we unwrap and encode)."""
        # Allow callers to pass either the bare numeric id or the full resource name.
        if "/" in str(data_point_id):
            path = f"/v4/{data_point_id}:exportExerciseTcx"
        else:
            path = f"/v4/{EXERCISE_PARENT}/dataPoints/{data_point_id}:exportExerciseTcx"
        params = {"partialData": "true" if partial else "false"}
        r = await self._request("GET", path, params=params)
        payload = r.json()
        tcx = payload.get("tcxData") or ""
        return tcx.encode("utf-8")

    async def find_near(
        self,
        when: datetime,
        *,
        window_minutes: int = 120,
    ) -> list[dict[str, Any]]:
        """Return exercises whose start time is within +/- window_minutes of
        ``when``, sorted by absolute time delta."""
        if when.tzinfo is None:
            when_aware = when.replace(tzinfo=timezone.utc)
        else:
            when_aware = when

        # The civil-time filter doesn't support time-of-day; widen by a day on
        # each side then filter client-side using the parsed startTime.
        after = (when_aware - timedelta(days=1)).date().isoformat()
        before = (when_aware + timedelta(days=1)).date().isoformat()
        candidates = await self.list_exercises(after_date=after, before_date=before)

        window = timedelta(minutes=window_minutes)
        scored: list[tuple[timedelta, dict[str, Any]]] = []
        for ex in candidates:
            started = _parse_exercise_start(ex)
            if started is None:
                continue
            delta = abs(started - when_aware)
            if delta <= window:
                scored.append((delta, ex))

        scored.sort(key=lambda pair: pair[0])
        return [ex for _, ex in scored]
