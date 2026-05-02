from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
import structlog

from app import tokens as token_store
from app.strava import auth

log = structlog.get_logger()

API_BASE = "https://www.strava.com/api/v3"


class StravaUploadError(RuntimeError):
    def __init__(
        self,
        msg: str,
        *,
        upload_id: int | None = None,
        error_text: str | None = None,
    ) -> None:
        super().__init__(msg)
        self.upload_id = upload_id
        self.error_text = error_text

DEFAULT_STREAM_KEYS = (
    "time",
    "latlng",
    "distance",
    "altitude",
    "velocity_smooth",
    "heartrate",
    "cadence",
    "watts",
    "temp",
    "grade_smooth",
)


class StravaNotConfigured(RuntimeError):
    """Raised when no Strava tokens are stored. Caller should prompt for login."""


class StravaClient:
    def __init__(self, http: httpx.AsyncClient, current: token_store.TokenPair) -> None:
        self._http = http
        self._tokens = current

    @classmethod
    @asynccontextmanager
    async def open(cls, timeout: float = 30.0) -> AsyncIterator["StravaClient"]:
        current = await token_store.load("strava")
        if current is None:
            raise StravaNotConfigured("No Strava tokens stored. Run `stravafit strava login`.")
        async with httpx.AsyncClient(timeout=timeout) as http:
            yield cls(http, current)

    @property
    def tokens(self) -> token_store.TokenPair:
        return self._tokens

    async def _ensure_fresh(self) -> None:
        if self._tokens.expired:
            log.info("strava.refresh", reason="expiry")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("strava", self._tokens)

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        await self._ensure_fresh()
        url = f"{API_BASE}{path}"
        headers = kwargs.pop("headers", {}) | {"Authorization": f"Bearer {self._tokens.access_token}"}

        resp = await self._http.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401:
            log.info("strava.refresh", reason="401")
            self._tokens = await auth.refresh(self._tokens)
            await token_store.save("strava", self._tokens)
            headers["Authorization"] = f"Bearer {self._tokens.access_token}"
            resp = await self._http.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    async def list_recent_activities(self, limit: int = 50, page: int = 1) -> list[dict[str, Any]]:
        r = await self._request(
            "GET",
            "/athlete/activities",
            params={"per_page": limit, "page": page},
        )
        return r.json()

    async def get_activity(self, activity_id: int) -> dict[str, Any]:
        r = await self._request("GET", f"/activities/{activity_id}")
        return r.json()

    async def get_streams(
        self,
        activity_id: int,
        keys: tuple[str, ...] = DEFAULT_STREAM_KEYS,
    ) -> dict[str, Any]:
        r = await self._request(
            "GET",
            f"/activities/{activity_id}/streams",
            params={"keys": ",".join(keys), "key_by_type": "true"},
        )
        return r.json()

    async def delete_activity(self, activity_id: int) -> None:
        """DELETE /activities/{id}. 204 expected."""
        await self._request("DELETE", f"/activities/{activity_id}")

    async def upload(
        self,
        file_bytes: bytes,
        *,
        data_type: str = "fit",
        name: str | None = None,
        description: str | None = None,
        external_id: str | None = None,
        activity_type: str | None = None,
        trainer: bool | None = None,
        commute: bool | None = None,
    ) -> dict[str, Any]:
        """POST /uploads (multipart/form-data). Returns the JSON envelope.

        Note: At submission time activity_id is null — must poll get_upload.
        """
        ext = "fit"
        if data_type in ("fit", "tcx", "gpx", "fit.gz", "tcx.gz", "gpx.gz"):
            ext = data_type
        filename = f"merged.{ext}"

        data: dict[str, Any] = {"data_type": data_type}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if external_id is not None:
            data["external_id"] = external_id
        if activity_type is not None:
            data["activity_type"] = activity_type
        if trainer is not None:
            data["trainer"] = "1" if trainer else "0"
        if commute is not None:
            data["commute"] = "1" if commute else "0"

        files = {"file": (filename, file_bytes, "application/octet-stream")}
        r = await self._request("POST", "/uploads", data=data, files=files)
        return r.json()

    async def get_upload(self, upload_id: int) -> dict[str, Any]:
        """GET /uploads/{id}. Same shape as upload()."""
        r = await self._request("GET", f"/uploads/{upload_id}")
        return r.json()

    async def wait_for_upload(
        self,
        upload_id: int,
        *,
        timeout_s: float = 60.0,
        poll_s: float = 2.0,
    ) -> dict[str, Any]:
        """Poll get_upload until activity_id non-null OR error non-null OR timeout.

        Raises asyncio.TimeoutError on timeout.
        Raises StravaUploadError if error field is set.
        """
        deadline = time.monotonic() + timeout_s
        last: dict[str, Any] = {}
        while True:
            last = await self.get_upload(upload_id)
            error_text = last.get("error")
            if error_text:
                raise StravaUploadError(
                    f"Strava upload {upload_id} failed: {error_text}",
                    upload_id=upload_id,
                    error_text=str(error_text),
                )
            if last.get("activity_id") is not None:
                return last
            if time.monotonic() >= deadline:
                raise asyncio.TimeoutError(
                    f"Strava upload {upload_id} did not finish within {timeout_s}s; last status={last.get('status')!r}"
                )
            await asyncio.sleep(poll_s)

    async def update_activity(
        self,
        activity_id: int,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """PUT /activities/{id}. Send only the keys that are non-None as form data."""
        data: dict[str, Any] = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        r = await self._request("PUT", f"/activities/{activity_id}", data=data)
        return r.json()
