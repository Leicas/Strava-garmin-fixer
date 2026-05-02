from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from app import tokens as token_store
from app.config import settings

AUTHORIZE_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"
DEFAULT_SCOPES = "activity location"  # space-separated for Fitbit
LISTENER_PORT = 8002


def _basic_auth_header() -> str:
    raw = f"{settings.fitbit_client_id}:{settings.fitbit_client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _make_pkce_pair() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) for PKCE S256."""
    # token_urlsafe(64) returns ~86 chars; trim to 128 max (RFC 7636).
    verifier = secrets.token_urlsafe(64)[:128]
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def build_authorize_url(
    redirect_uri: str,
    state: str,
    code_challenge: str,
    scopes: str = DEFAULT_SCOPES,
) -> str:
    qs = urlencode(
        {
            "client_id": settings.fitbit_client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{AUTHORIZE_URL}?{qs}"


def _expires_at_from_payload(payload: dict) -> int:
    return int(time.time()) + int(payload["expires_in"])


async def exchange_code(
    code: str,
    code_verifier: str,
    *,
    redirect_uri: str | None = None,
) -> token_store.TokenPair:
    """Exchange a Fitbit auth code. Fitbit requires `redirect_uri` to match the
    one used in the authorize step. Defaults to the CLI-listener URL when
    omitted; the web OAuth router passes the dashboard callback URL instead."""
    if redirect_uri is None:
        redirect_uri = f"http://localhost:{LISTENER_PORT}/exchange_token"
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "client_id": settings.fitbit_client_id,
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": redirect_uri,
            },
        )
        r.raise_for_status()
        payload = r.json()
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=_expires_at_from_payload(payload),
    )


async def refresh(current: token_store.TokenPair) -> token_store.TokenPair:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": current.refresh_token,
            },
        )
        r.raise_for_status()
        payload = r.json()
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=_expires_at_from_payload(payload),
    )


def run_local_oauth_flow(scopes: str = DEFAULT_SCOPES, timeout: int = 300) -> tuple[str, str]:
    """Open the user's browser, capture the authorization code via a stdlib
    listener on localhost:LISTENER_PORT, and return (code, code_verifier).
    The verifier must be passed back to exchange_code(). Token exchange itself
    is the caller's job (it's async)."""

    if not settings.fitbit_client_id or not settings.fitbit_client_secret:
        raise RuntimeError("FITBIT_CLIENT_ID / FITBIT_CLIENT_SECRET not set in env")

    state = secrets.token_urlsafe(16)
    code_verifier, code_challenge = _make_pkce_pair()
    redirect_uri = f"http://localhost:{LISTENER_PORT}/exchange_token"
    auth_url = build_authorize_url(redirect_uri, state, code_challenge, scopes)
    received: dict[str, str | None] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            qs = parse_qs(urlparse(self.path).query)
            received["code"] = qs.get("code", [None])[0]
            received["state"] = qs.get("state", [None])[0]
            received["error"] = qs.get("error", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = (
                b"<!doctype html><meta charset=utf-8>"
                b"<h1>Authorized.</h1>"
                b"<p>You can close this tab and return to the terminal.</p>"
            )
            self.wfile.write(body)

        def log_message(self, *_args, **_kwargs) -> None:
            pass

    in_container = os.environ.get("IN_CONTAINER") == "1"
    bind_host = "" if in_container else "localhost"
    server = HTTPServer((bind_host, LISTENER_PORT), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        if in_container:
            print(f"Container mode: open this URL in your host browser:\n  {auth_url}\n")
        else:
            print("Opening browser for Fitbit authorization...")
            print(f"If it doesn't open, visit:\n  {auth_url}\n")
            webbrowser.open(auth_url)

        deadline = time.time() + timeout
        while "code" not in received and time.time() < deadline:
            time.sleep(0.1)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if received.get("error"):
        raise RuntimeError(f"Fitbit OAuth error: {received['error']}")
    if received.get("state") != state:
        raise RuntimeError("Fitbit OAuth state mismatch (possible CSRF)")
    if not received.get("code"):
        raise RuntimeError("Fitbit OAuth flow timed out without receiving code")
    return received["code"], code_verifier  # type: ignore[return-value]
