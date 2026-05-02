from __future__ import annotations

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

AUTHORIZE_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
DEFAULT_SCOPES = "activity:read_all,activity:write"
LISTENER_PORT = 8001


def build_authorize_url(redirect_uri: str, state: str, scopes: str = DEFAULT_SCOPES) -> str:
    qs = urlencode(
        {
            "client_id": settings.strava_client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "approval_prompt": "auto",
            "scope": scopes,
            "state": state,
        }
    )
    return f"{AUTHORIZE_URL}?{qs}"


async def exchange_code(code: str) -> token_store.TokenPair:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            data={
                "client_id": settings.strava_client_id,
                "client_secret": settings.strava_client_secret,
                "code": code,
                "grant_type": "authorization_code",
            },
        )
        r.raise_for_status()
        payload = r.json()
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=int(payload["expires_at"]),
    )


async def refresh(current: token_store.TokenPair) -> token_store.TokenPair:
    async with httpx.AsyncClient(timeout=30) as http:
        r = await http.post(
            TOKEN_URL,
            data={
                "client_id": settings.strava_client_id,
                "client_secret": settings.strava_client_secret,
                "refresh_token": current.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        r.raise_for_status()
        payload = r.json()
    return token_store.TokenPair(
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=int(payload["expires_at"]),
    )


def run_local_oauth_flow(scopes: str = DEFAULT_SCOPES, timeout: int = 300) -> str:
    """Open the user's browser, capture the authorization code via a stdlib
    listener on localhost:LISTENER_PORT, return the code. Token exchange is
    the caller's job (it's async)."""

    if not settings.strava_client_id or not settings.strava_client_secret:
        raise RuntimeError("STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET not set in env")

    state = secrets.token_urlsafe(16)
    redirect_uri = f"http://localhost:{LISTENER_PORT}/exchange_token"
    auth_url = build_authorize_url(redirect_uri, state, scopes)
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
            print(f"Opening browser for Strava authorization...")
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
        raise RuntimeError(f"Strava OAuth error: {received['error']}")
    if received.get("state") != state:
        raise RuntimeError("Strava OAuth state mismatch (possible CSRF)")
    if not received.get("code"):
        raise RuntimeError("Strava OAuth flow timed out without receiving code")
    return received["code"]  # type: ignore[return-value]
