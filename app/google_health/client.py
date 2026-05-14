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


# Google Fit / Health activity type enum.
# Source: https://developers.google.com/fit/rest/v1/reference/activity-types
# Only the values likely to show up for fitness sync are mapped — anything
# unknown falls through to "type <n>" in activity_type_label().
_ACTIVITY_TYPE_NAMES: dict[int, str] = {
    0: "In vehicle",
    1: "Biking",
    2: "On foot",
    3: "Still",
    4: "Unknown",
    5: "Tilting",
    7: "Walking",
    8: "Running",
    9: "Aerobics",
    10: "Badminton",
    11: "Baseball",
    12: "Basketball",
    13: "Biathlon",
    14: "Handbiking",
    15: "Mountain biking",
    16: "Road biking",
    17: "Spinning",
    18: "Stationary biking",
    19: "Utility biking",
    20: "Boxing",
    21: "Calisthenics",
    22: "Circuit training",
    23: "Cricket",
    24: "Dancing",
    25: "Elliptical",
    26: "Fencing",
    27: "Football (American)",
    28: "Football (Australian)",
    29: "Football (Soccer)",
    30: "Frisbee",
    31: "Gardening",
    32: "Golf",
    33: "Gymnastics",
    34: "Handball",
    35: "Hiking",
    36: "Hockey",
    37: "Horseback riding",
    38: "Housework",
    39: "Jumping rope",
    40: "Kayaking",
    41: "Kettlebell training",
    42: "Kickboxing",
    43: "Kitesurfing",
    44: "Martial arts",
    45: "Meditation",
    46: "Mixed martial arts",
    47: "P90X",
    48: "Paragliding",
    49: "Pilates",
    50: "Polo",
    51: "Racquetball",
    52: "Rock climbing",
    53: "Rowing",
    54: "Rowing machine",
    55: "Rugby",
    56: "Jogging",
    57: "Running on sand",
    58: "Running (treadmill)",
    59: "Sailing",
    60: "Scuba diving",
    61: "Skateboarding",
    62: "Skating",
    63: "Cross skating",
    64: "Inline skating",
    65: "Skiing",
    66: "Back-country skiing",
    67: "Cross-country skiing",
    68: "Downhill skiing",
    69: "Kite skiing",
    70: "Roller skiing",
    71: "Sledding",
    72: "Sleeping",
    73: "Snowboarding",
    74: "Snowmobile",
    75: "Snowshoeing",
    76: "Squash",
    77: "Stair climbing",
    78: "Stair-climbing machine",
    79: "Stand-up paddleboarding",
    80: "Strength training",
    81: "Surfing",
    82: "Swimming",
    83: "Swimming (open water)",
    84: "Swimming (pool)",
    85: "Table tennis",
    86: "Team sports",
    87: "Tennis",
    88: "Treadmill running",
    89: "Volleyball",
    90: "Volleyball (beach)",
    91: "Volleyball (indoor)",
    92: "Wakeboarding",
    93: "Walking (fitness)",
    94: "Nordic walking",
    95: "Walking (treadmill)",
    96: "Waterpolo",
    97: "Weightlifting",
    98: "Wheelchair",
    99: "Windsurfing",
    100: "Yoga",
    101: "Zumba",
    102: "Diving",
    103: "Ergometer",
    104: "Ice skating",
    105: "Indoor skating",
    106: "Curling",
    108: "Other (unclassified)",
    109: "Light sleep",
    110: "Deep sleep",
    111: "REM sleep",
    112: "Awake (during sleep)",
    113: "Crossfit",
    114: "HIIT",
    115: "Interval training",
    116: "Walking (stroller)",
    117: "Elevator",
    118: "Escalator",
    119: "Archery",
    120: "Softball",
}


def _exercise_root(point: dict[str, Any]) -> dict[str, Any]:
    """Return the canonical exercise sub-dict on a dataPoint, regardless of
    whether the API nests it under ``value`` or places it at the top level."""
    val = point.get("value")
    if isinstance(val, dict):
        ex = val.get("exercise")
        if isinstance(ex, dict):
            return ex
    ex = point.get("exercise")
    return ex if isinstance(ex, dict) else {}


def activity_type_label(point: dict[str, Any]) -> str:
    """Human label for an exercise dataPoint's activity type. Handles
    integer enums (Fit API) and string codes (newer responses) and falls back
    to ``"type <n>"`` when the value is unknown. Returns ``""`` when unset."""
    ex = _exercise_root(point)
    raw = ex.get("activityType")
    if raw is None or raw == "":
        return ""
    if isinstance(raw, str):
        return raw.replace("_", " ").title() if raw.isupper() else raw
    if isinstance(raw, int):
        return _ACTIVITY_TYPE_NAMES.get(raw, f"type {raw}")
    return str(raw)


def _parse_exercise_start(point: dict[str, Any]) -> datetime | None:
    """Best-effort parse of the start time of an exercise dataPoint.

    Google's documented shape isn't fully pinned down by the public reference;
    we try the most likely paths and return None if nothing parses."""
    candidates = [
        ("value", "exercise", "interval", "startTime"),
        ("value", "exercise", "interval", "start_time"),
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
        page_size: int = 100,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        """List exercise dataPoints across all pages within the date window.

        ``after_date`` / ``before_date`` are ``YYYY-MM-DD`` strings (the API
        filter only supports civil dates). ``page_size`` controls per-page
        size (max 100 per Google docs); ``max_pages`` is a hard cap so a
        broken/looped ``nextPageToken`` cannot run forever — at 100/page
        that's still 2000 results, plenty for any UI listing."""
        filter_parts = [f'exercise.interval.civil_start_time >= "{after_date}"']
        if before_date:
            filter_parts.append(f'exercise.interval.civil_start_time < "{before_date}"')
        filter_expr = " AND ".join(filter_parts)

        all_points: list[dict[str, Any]] = []
        page_token: str | None = None
        for _ in range(max_pages):
            params: dict[str, Any] = {
                "filter": filter_expr,
                "pageSize": page_size,
            }
            if page_token:
                params["pageToken"] = page_token
            r = await self._request(
                "GET", f"/v4/{EXERCISE_PARENT}/dataPoints", params=params,
            )
            body = r.json()
            all_points.extend(body.get("dataPoints", []) or [])
            page_token = body.get("nextPageToken") or None
            if not page_token:
                break
        return all_points

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
