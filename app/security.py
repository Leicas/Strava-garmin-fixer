"""Security primitives for the dashboard.

- `BasicAuthMiddleware`: HTTP Basic auth on every dashboard route. Excludes a
  small allow-list of unauthenticated endpoints (health, webhook receiver,
  OAuth callbacks) — those have their own validation paths.
- `require_htmx`: a FastAPI dependency for state-changing POSTs. Browsers do
  not include the `HX-Request` header on cross-origin form submissions, so this
  is a cheap CSRF defense even if Basic credentials are cached in the browser.

Threat model this covers:
- Drive-by attacker hitting the dashboard URL: 401, no further leakage.
- CSRF against state-changing endpoints (toggle kill switch, trigger merge):
  blocked unless the request also presents valid Basic credentials AND the
  HX-Request header — a non-trivial combination to forge cross-origin.

What it does NOT cover:
- Brute-force against weak passwords. Pick a strong one.
- A determined attacker on a compromised browser session. Out of scope.
- DoS / rate limiting. Front this with a tunnel that has those features
  (Cloudflare Tunnel + Access is the recommended deployment).
"""

from __future__ import annotations

import base64
import binascii
import secrets

import structlog
from fastapi import HTTPException, Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from starlette.types import ASGIApp

from app.config import settings

log = structlog.get_logger()


# Endpoints that intentionally don't require Basic auth.
# - /healthz: monitoring
# - /webhook/strava: the provider can't authenticate; uses verify_token
#   (handshake) and owner_id (events) instead.
# - /auth/strava/callback, /auth/google/callback: providers can't authenticate;
#   security is enforced via the OAuth `state` parameter (see app/auth_router.py).
_PUBLIC_PATHS: frozenset[str] = frozenset({
    "/healthz",
    "/webhook/strava",
    "/auth/strava/callback",
    "/auth/google/callback",
})


def _is_public(path: str) -> bool:
    return path in _PUBLIC_PATHS


class BasicAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if _is_public(path):
            return await call_next(request)

        if settings.dashboard_auth_disabled:
            # Surface this loudly in the logs so it's not silently insecure.
            log.warning("auth.disabled_serving_unauthenticated", path=path)
            return await call_next(request)

        if not settings.dashboard_user or not settings.dashboard_password:
            return Response(
                "Server misconfigured: DASHBOARD_USER and DASHBOARD_PASSWORD must be set, "
                "or DASHBOARD_AUTH_DISABLED=1 for local dev.",
                status_code=500,
                media_type="text/plain",
            )

        header = request.headers.get("authorization", "")
        if not header.lower().startswith("basic "):
            return _challenge()

        try:
            decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return _challenge()

        user, sep, password = decoded.partition(":")
        if not sep:
            return _challenge()

        # constant-time compare against both fields
        if not (
            secrets.compare_digest(user, settings.dashboard_user)
            and secrets.compare_digest(password, settings.dashboard_password)
        ):
            log.info("auth.basic.failed", user=user, path=path)
            return _challenge()

        return await call_next(request)


def _challenge() -> Response:
    return Response(
        "Unauthorized",
        status_code=status.HTTP_401_UNAUTHORIZED,
        headers={"WWW-Authenticate": 'Basic realm="StravaFit"'},
        media_type="text/plain",
    )


def require_htmx(request: Request) -> None:
    """Dependency: reject the request unless `HX-Request: true` is present.

    HTMX sets this header on every request. Cross-origin <form>/<a> POSTs from
    a different page do not, so this blocks classic CSRF even if the browser
    has cached Basic credentials for our origin."""
    if request.headers.get("hx-request", "").lower() != "true":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="HX-Request header required for this endpoint",
        )
