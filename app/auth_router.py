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
from app.fitbit import auth as fitbit_auth
from app.strava import auth as strava_auth

log = structlog.get_logger()
router = APIRouter()

# state -> {"service": "strava"|"fitbit", "code_verifier": str|None, "expires_at": int}
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
    log.info("auth.strava.connected", expires_at=pair.expires_at)
    return RedirectResponse("/settings?connected=strava", status_code=303)


# ---------- Fitbit ----------------------------------------------------------

@router.get("/auth/fitbit/start")
async def fitbit_start() -> RedirectResponse:
    if not settings.fitbit_client_id or not settings.fitbit_client_secret:
        raise HTTPException(500, "FITBIT_CLIENT_ID / FITBIT_CLIENT_SECRET not set in env")
    code_verifier, code_challenge = fitbit_auth._make_pkce_pair()
    state = _new_state("fitbit", code_verifier=code_verifier)
    url = fitbit_auth.build_authorize_url(
        settings.fitbit_redirect_uri,
        state,
        code_challenge,
    )
    log.info("auth.fitbit.start")
    return RedirectResponse(url, status_code=302)


@router.get("/auth/fitbit/callback")
async def fitbit_callback(
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
) -> RedirectResponse:
    if error:
        log.warning("auth.fitbit.error", error=error)
        return RedirectResponse(f"/settings?auth_error=fitbit:{error}", status_code=303)
    if not code:
        return RedirectResponse("/settings?auth_error=fitbit:no_code", status_code=303)
    pending = _consume(state, "fitbit")
    if pending is None:
        return RedirectResponse("/settings?auth_error=fitbit:bad_state", status_code=303)
    code_verifier = pending["code_verifier"]
    if not code_verifier:
        return RedirectResponse("/settings?auth_error=fitbit:no_verifier", status_code=303)

    try:
        pair = await fitbit_auth.exchange_code(
            code, code_verifier, redirect_uri=settings.fitbit_redirect_uri,
        )
    except httpx.HTTPStatusError as e:
        log.warning("auth.fitbit.exchange_failed", status=e.response.status_code,
                    body=e.response.text[:200])
        return RedirectResponse(
            f"/settings?auth_error=fitbit:exchange_{e.response.status_code}",
            status_code=303,
        )

    await token_store.save("fitbit", pair)
    log.info("auth.fitbit.connected", expires_at=pair.expires_at)
    return RedirectResponse("/settings?connected=fitbit", status_code=303)
