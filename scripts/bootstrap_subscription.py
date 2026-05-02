"""Idempotent: list existing subscriptions; create only if absent.

Usage:
  uv run python -m scripts.bootstrap_subscription list
  uv run python -m scripts.bootstrap_subscription create
  uv run python -m scripts.bootstrap_subscription delete <id>
  uv run python -m scripts.bootstrap_subscription ensure   # create iff none point at our callback

Requires STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, VERIFY_TOKEN, PUBLIC_BASE_URL
in env (.env loaded via app.config).
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import httpx

from app.config import settings

API_BASE = "https://www.strava.com/api/v3/push_subscriptions"


def _callback_url() -> str:
    return f"{settings.public_base_url.rstrip('/')}/webhook/strava"


def _creds() -> dict[str, str]:
    return {
        "client_id": settings.strava_client_id,
        "client_secret": settings.strava_client_secret,
    }


def _check_creds() -> None:
    missing = []
    if not settings.strava_client_id:
        missing.append("STRAVA_CLIENT_ID")
    if not settings.strava_client_secret:
        missing.append("STRAVA_CLIENT_SECRET")
    if not settings.verify_token or settings.verify_token == "change-me":
        missing.append("VERIFY_TOKEN")
    if not settings.public_base_url or settings.public_base_url.startswith("http://localhost"):
        # Strava requires a publicly reachable URL. Warn but don't block list/delete.
        print(
            f"warning: PUBLIC_BASE_URL is '{settings.public_base_url}'. "
            "Strava requires a public HTTPS URL for create/ensure.",
            file=sys.stderr,
        )
    if missing:
        print(
            "error: missing required env vars: " + ", ".join(missing),
            file=sys.stderr,
        )
        sys.exit(2)


def _raise_for_strava(resp: httpx.Response) -> None:
    """Strava returns useful JSON detail on errors -- print body, then raise."""
    if resp.is_success:
        return
    try:
        body: Any = resp.json()
    except Exception:
        body = resp.text
    print(
        f"error: HTTP {resp.status_code} from {resp.request.method} {resp.request.url}",
        file=sys.stderr,
    )
    print(json.dumps(body, indent=2) if not isinstance(body, str) else body, file=sys.stderr)
    sys.exit(1)


def cmd_list() -> list[dict[str, Any]]:
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(API_BASE, params=_creds())
    _raise_for_strava(resp)
    data = resp.json()
    print(json.dumps(data, indent=2))
    return data if isinstance(data, list) else []


def cmd_create() -> dict[str, Any]:
    payload = {
        **_creds(),
        "callback_url": _callback_url(),
        "verify_token": settings.verify_token,
    }
    print(f"POST {API_BASE} callback_url={payload['callback_url']}")
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(API_BASE, data=payload)
    _raise_for_strava(resp)
    data = resp.json()
    print(json.dumps(data, indent=2))
    return data


def cmd_delete(sub_id: int) -> None:
    url = f"{API_BASE}/{sub_id}"
    with httpx.Client(timeout=30.0) as client:
        resp = client.delete(url, params=_creds())
    _raise_for_strava(resp)
    print(f"deleted subscription id={sub_id}")


def cmd_ensure() -> None:
    """List existing subs; if any matches our callback_url, do nothing. Else create."""
    target = _callback_url()
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(API_BASE, params=_creds())
    _raise_for_strava(resp)
    subs = resp.json() if isinstance(resp.json(), list) else []
    for sub in subs:
        if sub.get("callback_url") == target:
            print(f"already exists, id={sub.get('id')} callback_url={target}")
            return
    print(f"no existing subscription for {target} -- creating")
    cmd_create()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scripts.bootstrap_subscription",
        description="Idempotent Strava push-subscription manager.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="List push subscriptions for these credentials.")
    sub.add_parser("create", help="Create a subscription pointing at our callback URL.")
    p_del = sub.add_parser("delete", help="Delete a subscription by id.")
    p_del.add_argument("id", type=int, help="Subscription id to delete.")
    sub.add_parser(
        "ensure",
        help="Create a subscription only if none currently points at our callback URL.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    _check_creds()

    if args.cmd == "list":
        cmd_list()
    elif args.cmd == "create":
        cmd_create()
    elif args.cmd == "delete":
        cmd_delete(args.id)
    elif args.cmd == "ensure":
        cmd_ensure()
    else:  # argparse should prevent this
        parser.error(f"unknown command: {args.cmd}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
