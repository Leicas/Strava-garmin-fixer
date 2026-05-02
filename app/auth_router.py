"""Web OAuth flow. The dashboard redirects users through the standard
authorization-code dance — no CLI access required.

Both providers redirect back to `{PUBLIC_BASE_URL}/auth/<service>/callback`,
so the user must register that URL with each provider before connecting.

State is held in a process-local dict with a 10-minute TTL. Single-user, single
process — no need for cookies or a session backend."""

from __future__ import annotations

import secrets
import time
from typing import Any

import httpx
import structlog
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse

from app import tokens as token_store
from app.config import settings
from app.google_health import auth as google_auth
from app.strava import auth as strava_auth

log = structlog.get_logger()
router = APIRouter()

# state -> {"service": "strava"|"google", "code_verifier": str|None, "expires_at": int}
_PENDING: dict[str, dict[str, Any]] = {}
_TTL_SECONDS = 600  # 10 min


def _gc() -> None:
    now = int(time.time())
    for k in list(_PENDING.keys()):
        if _PENDING[k]["expires_at"] < now:
            _PENDING.pop(k, None)


def _new_state(service: str, *, code_verifier: str | None = None) -> str:
    state = secrets.token_urlsafe(24)
    _gc()
    _PENDING[state] = {
        "service": service,
        "code_verifier": code_verifier,
        "expires_at": int(time.time()) + _TTL_SECONDS,
    }
    return state


def _consume(state: str | None, expected_service: str) -> dict[str, Any] | None:
    if not state:
        return None
    pending = _PENDING.pop(state, None)
    if pending is None or pending["service"] != expected_service:
        return None
    if pending["expires_at"] < int(time.time()):
        return None
    return pending


# ---------- Strava ----------------------------------------------------------

@router.get("/auth/strava/start")
async def strava_start() -> RedirectResponse:
    if not settings.strava_client_id or not settings.strava_client_secret:
        raise HTTPException(500, "STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET not set in env")
    state = _new_state("strava")
    url = strava_auth.build_authorize_url(settings.strava_redirect_uri, state)
    log.info("auth.strava.start")
    return RedirectResponse(url, status_code=302)


_REQUIRED_STRAVA_SCOPES = ("activity:read_all", "activity:write")


@router.get("/auth/strava/callback")
async def strava_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    scope: str | None = Query(None),
) -> RedirectResponse:
    if error:
        log.warning("auth.strava.error", error=error)
        return RedirectResponse(f"/settings?auth_error=strava:{error}", status_code=303)
    if not code:
        return RedirectResponse("/settings?auth_error=strava:no_code", status_code=303)

    # Strava lets users uncheck individual scopes on the consent screen, so
    # `scope` may be a subset of what we asked for. Reject if a required scope
    # is missing — silently saving a read-only token and failing on the next
    # delete/upload is much more confusing. (Side-effect-free, so it runs
    # before the state check.)
    granted = set((scope or "").split(","))
    missing = [s for s in _REQUIRED_STRAVA_SCOPES if s not in granted]
    if missing:
        log.warning("auth.strava.missing_scope", missing=missing, granted=sorted(granted))
        return RedirectResponse(
            f"/settings?auth_error=strava:missing_scope_{'+'.join(missing)}",
            status_code=303,
        )

    if _consume(state, "strava") is None:
        return RedirectResponse("/settings?auth_error=strava:bad_state", status_code=303)

    try:
        pair = await strava_auth.exchange_code(code)
    except httpx.HTTPStatusError as e:
        log.warning("auth.strava.exchange_failed", status=e.response.status_code)
        return RedirectResponse(
            f"/settings?auth_error=strava:exchange_{e.response.status_code}",
            status_code=303,
        )

    await token_store.save("strava", pair)
    log.info("auth.strava.connected", expires_at=pair.expires_at, scope=scope)
    return RedirectResponse("/settings?connected=strava", status_code=303)


# ---------- Google Health ---------------------------------------------------

@router.get("/auth/google/start")
async def google_start() -> RedirectResponse:
    if not settings.google_client_id or not settings.google_client_secret:
        raise HTTPException(500, "GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET not set in env")
    state = _new_state("google")
    url = google_auth.build_authorize_url(settings.google_redirect_uri, state)
    log.info("auth.google.start")
    return RedirectResponse(url, status_code=302)


@router.get("/auth/google/callback")
async def google_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
) -> RedirectResponse:
    if error:
        log.warning("auth.google.error", error=error)
        return RedirectResponse(f"/settings?auth_error=google:{error}", status_code=303)
    if not code:
        return RedirectResponse("/settings?auth_error=google:no_code", status_code=303)
    if _consume(state, "google") is None:
        return RedirectResponse("/settings?auth_error=google:bad_state", status_code=303)

    try:
        pair = await google_auth.exchange_code(
            code, redirect_uri=settings.google_redirect_uri,
        )
    except httpx.HTTPStatusError as e:
        log.warning("auth.google.exchange_failed", status=e.response.status_code,
                    body=e.response.text[:200])
        return RedirectResponse(
            f"/settings?auth_error=google:exchange_{e.response.status_code}",
            status_code=303,
        )
    except RuntimeError as e:
        # exchange_code raises if Google didn't return a refresh_token
        return RedirectResponse(f"/settings?auth_error=google:{e}", status_code=303)

    await token_store.save("google", pair)
    log.info("auth.google.connected", expires_at=pair.expires_at)
    return RedirectResponse("/settings?connected=google", status_code=303)
