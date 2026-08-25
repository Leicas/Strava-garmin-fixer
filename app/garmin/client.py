"""Garmin Connect client.

Garmin Connect has no free official API for individuals (and Strava's
paywalled Standard tier is what this replaces), so this wraps the
community `garminconnect` library, which logs in with the account's own
credentials via curl_cffi TLS impersonation.

The library is synchronous — every call is pushed through
``asyncio.to_thread`` so the FastAPI event loop never blocks.

Auth model:
  - Tokens are cached on disk at ``settings.garmin_tokens_path`` and
    resumed automatically (they last ~1 year).
  - A fresh credential login happens only when the cache is missing or
    rejected, using GARMIN_EMAIL / GARMIN_PASSWORD from the env.
  - If Garmin demands an MFA code, headless (server) logins fail with
    GarminNotConfigured telling the user to run
    ``stravafit garmin login`` once interactively.
"""

from __future__ import annotations

import asyncio
import io
import time
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import structlog

from app.config import settings

log = structlog.get_logger()


class GarminNotConfigured(RuntimeError):
    """Raised when we can't authenticate. Caller should prompt for login."""


def _mfa_unavailable() -> str:
    raise GarminNotConfigured(
        "Garmin requires an MFA code but this is a headless login. "
        "Run `stravafit garmin login` interactively once to seed the token cache."
    )


def _login_sync(mfa_prompt: Callable[[], str] | None = None) -> Any:
    from garminconnect import Garmin  # heavy import, keep lazy

    tokenstore = str(Path(settings.garmin_tokens_path).expanduser())

    # 1) Resume from the on-disk token cache.
    try:
        g = Garmin()
        g.login(tokenstore)
        return g
    except Exception as exc:  # noqa: BLE001 - any failure falls through to fresh login
        log.info("garmin.token_resume_failed", err=str(exc)[:200])

    # 2) Fresh credential login.
    if not (settings.garmin_email and settings.garmin_password):
        raise GarminNotConfigured(
            "No valid Garmin token cache and GARMIN_EMAIL/GARMIN_PASSWORD are not set. "
            "Run `stravafit garmin login`."
        )
    prompt = mfa_prompt or _mfa_unavailable
    try:
        g = Garmin(
            email=settings.garmin_email,
            password=settings.garmin_password,
            prompt_mfa=prompt,
        )
    except TypeError:
        # Older library versions without prompt_mfa kwarg.
        g = Garmin(settings.garmin_email, settings.garmin_password)
    g.login()
    Path(tokenstore).mkdir(parents=True, exist_ok=True)
    g.garth.dump(tokenstore)
    log.info("garmin.credential_login_ok", tokenstore=tokenstore)
    return g


def _parse_gmt(raw: str | None) -> datetime | None:
    """Garmin's startTimeGMT is 'YYYY-MM-DD HH:MM:SS' in UTC, no tz suffix."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace(" ", "T")).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


class GarminClient:
    def __init__(self, garmin: Any) -> None:
        self._g = garmin

    @classmethod
    @asynccontextmanager
    async def open(
        cls, mfa_prompt: Callable[[], str] | None = None
    ) -> AsyncIterator["GarminClient"]:
        g = await asyncio.to_thread(_login_sync, mfa_prompt)
        yield cls(g)

    async def list_recent_activities(
        self, limit: int = 50, start: int = 0
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._g.get_activities, start, limit)

    async def get_activity(self, activity_id: int) -> dict[str, Any] | None:
        """Best-effort activity summary — API surface varies across library versions."""
        for name in ("get_activity", "get_activity_evaluation", "get_activity_details"):
            fn = getattr(self._g, name, None)
            if fn is None:
                continue
            try:
                out = await asyncio.to_thread(fn, activity_id)
                if isinstance(out, dict):
                    return out
            except Exception as exc:  # noqa: BLE001 - try the next accessor
                log.info("garmin.get_activity_failed", via=name, err=str(exc)[:200])
        return None

    async def find_near(
        self, when: datetime, *, window_minutes: int = 120, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Activities whose startTimeGMT is within +/- window of ``when``,
        sorted by absolute time delta."""
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        window = timedelta(minutes=window_minutes)
        acts = await self.list_recent_activities(limit=limit)
        scored: list[tuple[timedelta, dict[str, Any]]] = []
        for a in acts:
            started = _parse_gmt(a.get("startTimeGMT"))
            if started is None:
                continue
            delta = abs(started - when)
            if delta <= window:
                scored.append((delta, a))
        scored.sort(key=lambda p: p[0])
        return [a for _, a in scored]

    async def download_original_fit(self, activity_id: int) -> bytes:
        """Download the activity's ORIGINAL file (a zip holding the .fit) and
        return the raw FIT bytes."""
        dl_fmt = getattr(getattr(self._g, "ActivityDownloadFormat", None), "ORIGINAL", None)
        if dl_fmt is not None:
            blob = await asyncio.to_thread(
                self._g.download_activity, activity_id, dl_fmt
            )
        else:
            blob = await asyncio.to_thread(self._g.download_activity, activity_id)
        if not isinstance(blob, (bytes, bytearray)):
            raise RuntimeError(f"unexpected download payload type {type(blob)!r}")
        data = bytes(blob)
        if data[:2] == b"PK":  # zip envelope
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                names = [n for n in zf.namelist() if n.lower().endswith(".fit")]
                if not names:
                    raise RuntimeError(
                        f"ORIGINAL zip for {activity_id} has no .fit ({zf.namelist()})"
                    )
                return zf.read(names[0])
        return data

    async def upload_fit(self, fit_bytes: bytes, *, stem: str = "merged") -> dict[str, Any]:
        """Upload a FIT file. Returns the parsed 202 response body (shape:
        detailedImportResult; import itself is asynchronous — confirm with
        wait_for_activity).

        Raises RuntimeError unless Garmin answers 202 Accepted. In particular
        Garmin answers **204 No Content** for a FIT its importer won't parse
        (e.g. one missing the activity message) — that is a rejection, not a
        success, even though the library's upload_activity() reports it as an
        empty dict. This bit us on 2026-08-25: two activities were deleted
        after "successful" uploads that never imported.
        """
        import tempfile

        def _post() -> dict[str, Any]:
            with tempfile.TemporaryDirectory() as td:
                path = Path(td) / f"{stem}.fit"
                path.write_bytes(fit_bytes)
                with path.open("rb") as fh:
                    resp = self._g.client.post(
                        "connectapi",
                        self._g.garmin_connect_upload,
                        files={"file": (path.name, fh)},
                    )
            status = getattr(resp, "status_code", None)
            try:
                body = resp.json()
            except Exception:  # noqa: BLE001 - non-JSON body
                body = {}
            if status != 202:
                raise RuntimeError(
                    f"Garmin upload returned HTTP {status!r} (expected 202) — "
                    f"the FIT was NOT imported. body={str(body)[:200]}"
                )
            return body if isinstance(body, dict) else {}

        return await asyncio.to_thread(_post)

    async def wait_for_activity(
        self,
        start_time: datetime,
        *,
        exclude_ids: frozenset[int] | set[int] = frozenset(),
        timeout_s: float = 180.0,
        poll_s: float = 6.0,
        tolerance_s: float = 90.0,
    ) -> int:
        """Poll the activity list until an activity starting within
        ``tolerance_s`` of ``start_time`` (and not in ``exclude_ids``)
        appears; return its activityId. Raises TimeoutError otherwise.

        This is the only reliable confirmation that an async upload actually
        imported — the upload-status endpoint is not consistently available."""
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        deadline = time.monotonic() + timeout_s
        while True:
            for a in await self.list_recent_activities(limit=10):
                gid = a.get("activityId")
                st = _parse_gmt(a.get("startTimeGMT"))
                if gid is None or st is None or int(gid) in exclude_ids:
                    continue
                if abs((st - start_time).total_seconds()) <= tolerance_s:
                    return int(gid)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"uploaded activity near {start_time.isoformat()} did not "
                    f"appear on Garmin within {timeout_s:.0f}s"
                )
            await asyncio.sleep(poll_s)

    @staticmethod
    def uploaded_activity_id(upload_resp: dict[str, Any]) -> int | None:
        """Pull the new activityId out of an upload response, if present."""
        detail = upload_resp.get("detailedImportResult") or upload_resp
        successes = detail.get("successes") or []
        if successes and isinstance(successes[0], dict):
            iid = successes[0].get("internalId")
            if iid is not None:
                return int(iid)
        return None

    @staticmethod
    def upload_failures(upload_resp: dict[str, Any]) -> list[str]:
        detail = upload_resp.get("detailedImportResult") or upload_resp
        out: list[str] = []
        for f in detail.get("failures") or []:
            msgs = f.get("messages") or []
            for m in msgs:
                out.append(str(m.get("content") or m))
            if not msgs:
                out.append(str(f))
        return out

    async def delete_activity(self, activity_id: int) -> None:
        await asyncio.to_thread(self._g.delete_activity, activity_id)

    async def set_activity_name(self, activity_id: int, name: str) -> None:
        fn = getattr(self._g, "set_activity_name", None)
        if fn is None:
            raise RuntimeError("library lacks set_activity_name")
        await asyncio.to_thread(fn, activity_id, name)


def parse_start_gmt(activity: dict[str, Any]) -> datetime | None:
    """Public helper: start time (UTC) from a Garmin activity summary dict."""
    return _parse_gmt(activity.get("startTimeGMT"))
