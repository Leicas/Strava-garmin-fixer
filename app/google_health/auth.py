"""Google Health OAuth (replaces the deprecated Fitbit Web API auth).

Standard Google OAuth 2.0 web flow with a confidential client (we have
``GOOGLE_CLIENT_SECRET``), so PKCE is unnecessary. The first authorization
returns a refresh token only when ``access_type=offline`` AND ``prompt=consent``
are both present — otherwise subsequent re-auths return only an access token.
"""

from __future__ import annotations

import time
from urllib.parse import urlencode

import httpx

from app import tokens as token_store
from app.config import settings

AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"

# Read-only scopes covering exercise sessions, GPS, and biometrics. The TCX
# export endpoint specifically requires BOTH activity_and_fitness AND location.
DEFAULT_SCOPES = " ".join([
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.location.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
])


def build_authorize_url(redirect_uri: str, state: str, scopes: str = DEFAULT_SCOPES) -> str:
    qs = urlencode(
        {
            "client_id": settings.google_client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": scopes,
            "state": state,
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
        }
    )
    return f"{AUTHORIZE_URL}?{qs}"


def _expires_at(payload: dict) -> int:
    return int(time.time()) + int(payload["expires_in"])


async def exchange_code(code: str, *, redirect_uri: str) -> token_store.TokenPair:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            data={
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            },
        )
        r.raise_for_status()
        payload = r.json()
    if "refresh_token" not in payload:
        raise RuntimeError(
            "Google did not return a refresh_token. Revoke the existing grant at "
            "https://myaccount.google.com/permissions and re-run with prompt=consent."
        )
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=_expires_at(payload),
    )


async def refresh(current: token_store.TokenPair) -> token_store.TokenPair:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            data={
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "refresh_token": current.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        r.raise_for_status()
        payload = r.json()
    # Google does not normally rotate refresh tokens; reuse the existing one
    # if the response omits it.
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload.get("refresh_token", current.refresh_token),
        expires_at=_expires_at(payload),
    )
