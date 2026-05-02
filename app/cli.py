"""CLI entrypoint. Run with `python -m app.cli ...` or `stravafit ...`."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from app import jobs as jobs_mod
from app import tokens as token_store
from app.db import init_db
from app.fitbit import auth as fitbit_auth
from app.fitbit.client import FitbitClient, FitbitNotConfigured
from app.merge import MergeError, merge_streams_to_fit
from app.strava import auth as strava_auth
from app.strava.client import StravaClient, StravaNotConfigured


# ---------- Strava commands ----------

async def _strava_login() -> int:
    await init_db()
    code = strava_auth.run_local_oauth_flow()
    pair = await strava_auth.exchange_code(code)
    await token_store.save("strava", pair)
    expires = datetime.fromtimestamp(pair.expires_at, tz=timezone.utc).isoformat()
    print(f"Strava authorized. Access token expires at {expires}.")
    return 0


async def _strava_list(limit: int) -> int:
    try:
        async with StravaClient.open() as client:
            activities = await client.list_recent_activities(limit=limit)
    except StravaNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    for a in activities:
        print(
            f"{a['start_date_local']:25}  {a['id']:>12}  {a['type']:14}  "
            f"{a.get('distance', 0) / 1000:6.2f} km  {a.get('name', '')[:50]}"
        )
    return 0


async def _strava_fetch_activity(activity_id: int) -> int:
    try:
        async with StravaClient.open() as client:
            activity = await client.get_activity(activity_id)
            streams = await client.get_streams(activity_id)
    except StravaNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2

    print("=== Activity ===")
    print(json.dumps(
        {k: activity.get(k) for k in (
            "id", "name", "type", "device_name", "distance", "moving_time",
            "start_date", "start_date_local", "average_speed", "max_speed",
            "average_heartrate", "average_cadence", "average_temp",
        )},
        indent=2,
    ))
    print("\n=== Streams summary ===")
    for key, payload in streams.items():
        data = payload.get("data") or []
        print(f"  {key:18} n={len(data):>6}  type={payload.get('type')}  resolution={payload.get('resolution')}")
    return 0


async def _strava_streams(activity_id: int, out: str | None) -> int:
    try:
        async with StravaClient.open() as client:
            streams = await client.get_streams(activity_id)
    except StravaNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    blob = json.dumps(streams, indent=2)
    if out:
        Path(out).write_text(blob, encoding="utf-8")
        print(f"Wrote {out} ({len(blob)} bytes)")
    else:
        print(blob)
    return 0


# ---------- Fitbit commands ----------

async def _fitbit_login() -> int:
    await init_db()
    code, verifier = fitbit_auth.run_local_oauth_flow()
    pair = await fitbit_auth.exchange_code(code, verifier)
    await token_store.save("fitbit", pair)
    expires = datetime.fromtimestamp(pair.expires_at, tz=timezone.utc).isoformat()
    print(f"Fitbit authorized. Access token expires at {expires}.")
    return 0


async def _fitbit_list(after_date: str, limit: int) -> int:
    try:
        async with FitbitClient.open() as client:
            activities = await client.list_activities(after_date=after_date, limit=limit)
    except FitbitNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    for a in activities:
        print(
            f"{a.get('startTime', '')[:19]:20}  {a.get('logId'):>14}  "
            f"{a.get('activityName', '')[:24]:24}  "
            f"{(a.get('distance') or 0):6.2f}{a.get('distanceUnit', 'km')}  "
            f"dur={a.get('duration', 0) // 1000}s"
        )
    return 0


async def _fitbit_find_near(when_iso: str, window_minutes: int) -> int:
    try:
        when = datetime.fromisoformat(when_iso.replace("Z", "+00:00"))
    except ValueError:
        print(f"invalid ISO-8601 timestamp: {when_iso}", file=sys.stderr)
        return 2

    try:
        async with FitbitClient.open() as client:
            matches = await client.find_near(when, window_minutes=window_minutes)
    except FitbitNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2

    if not matches:
        print(f"No Fitbit activities within +/- {window_minutes} min of {when_iso}")
        return 0
    print(f"Found {len(matches)} match(es), closest first:")
    for a in matches:
        print(
            f"  {a.get('startTime', '')[:19]}  log_id={a.get('logId')}  "
            f"{a.get('activityName', '')}  duration={a.get('duration', 0) // 1000}s"
        )
    return 0


async def _fitbit_fetch_tcx(log_id: int, out: str | None) -> int:
    try:
        async with FitbitClient.open() as client:
            tcx = await client.get_activity_tcx(log_id)
    except FitbitNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2

    if out:
        Path(out).write_bytes(tcx)
        print(f"Wrote {out} ({len(tcx)} bytes)")
    else:
        sys.stdout.buffer.write(tcx)
    return 0


# ---------- Merge command (file-based, no API calls) ----------

async def _merge(
    strava_streams_path: str,
    fitbit_tcx_path: str,
    start_iso: str,
    out: str,
    activity_name: str,
) -> int:
    streams = json.loads(Path(strava_streams_path).read_text(encoding="utf-8"))
    tcx = Path(fitbit_tcx_path).read_bytes()
    try:
        start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    except ValueError:
        print(f"invalid --start-time: {start_iso}", file=sys.stderr)
        return 2
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    try:
        result = merge_streams_to_fit(
            strava_streams=streams,
            strava_start_time=start_dt,
            fitbit_tcx=tcx,
            activity_name=activity_name,
        )
    except MergeError as e:
        print(f"merge failed: {e}", file=sys.stderr)
        return 3

    Path(out).write_bytes(result.fit_bytes)
    print(
        f"merged: {len(result.fit_bytes)} bytes, {result.record_count} records, "
        f"{len(result.warnings)} warnings, distance={result.distance_meters:.1f}m -> {out}"
    )
    for w in result.warnings:
        print(f"  warning [{w.code}]: {w.message}")
    return 0


# ---------- Run-merge command (full pipeline via worker) ----------

async def _run_merge(strava_id: int, fitbit_log_id: int | None, dry_run: bool) -> int:
    from app.worker import run_merge_job

    await init_db()
    job_id = await jobs_mod.enqueue(
        strava_id,
        fitbit_log_id=fitbit_log_id,
        trigger="manual",
        dry_run=dry_run,
    )
    print(f"enqueued job {job_id} (strava_id={strava_id}, dry_run={dry_run})")
    await run_merge_job(job_id)
    job = await jobs_mod.get(job_id)
    if job is None:
        print("job vanished after run", file=sys.stderr)
        return 4
    print(f"\njob {job_id} status={job['status']}")
    if job.get("error"):
        print(f"  error: {job['error']}")
    if job.get("log"):
        print("---- log ----")
        print(job["log"])
    return 0 if job["status"] == "success" else 5


# ---------- Argparse wiring ----------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stravafit")
    sub = parser.add_subparsers(dest="cmd", required=False)

    sub.add_parser("version", help="Print version")

    # strava ---------------------------------------------------------------
    strava = sub.add_parser("strava", help="Strava CLI")
    strava_sub = strava.add_subparsers(dest="strava_cmd", required=True)
    strava_sub.add_parser("login", help="Run OAuth flow via local listener on :8001")

    p_list = strava_sub.add_parser("list", help="List recent activities")
    p_list.add_argument("--limit", type=int, default=20)

    p_fetch = strava_sub.add_parser("fetch-activity", help="Print activity + stream summary")
    p_fetch.add_argument("activity_id", type=int)

    p_streams = strava_sub.add_parser("streams", help="Dump raw streams JSON")
    p_streams.add_argument("activity_id", type=int)
    p_streams.add_argument("--out", help="Write to file instead of stdout")

    # fitbit ---------------------------------------------------------------
    fitbit = sub.add_parser("fitbit", help="Fitbit CLI")
    fitbit_sub = fitbit.add_subparsers(dest="fitbit_cmd", required=True)
    fitbit_sub.add_parser("login", help="Run OAuth (PKCE) via local listener on :8002")

    p_fb_list = fitbit_sub.add_parser("list", help="List Fitbit activities after a date")
    p_fb_list.add_argument("--after-date", required=True, help="YYYY-MM-DD")
    p_fb_list.add_argument("--limit", type=int, default=20)

    p_fb_near = fitbit_sub.add_parser("find-near", help="Find Fitbit activities near an ISO datetime")
    p_fb_near.add_argument("when", help="ISO-8601 timestamp, e.g. 2026-04-30T08:00:00Z")
    p_fb_near.add_argument("--window-minutes", type=int, default=120)

    p_fb_tcx = fitbit_sub.add_parser("fetch-tcx", help="Download a Fitbit activity as TCX")
    p_fb_tcx.add_argument("log_id", type=int)
    p_fb_tcx.add_argument("--out", help="Write to file (default: stdout, binary)")

    # merge ----------------------------------------------------------------
    p_merge = sub.add_parser("merge", help="Merge streams JSON + TCX -> FIT (no API calls)")
    p_merge.add_argument("--strava-streams", required=True, help="JSON file from `stravafit strava streams`")
    p_merge.add_argument("--fitbit-tcx", required=True, help="TCX file from `stravafit fitbit fetch-tcx`")
    p_merge.add_argument("--start-time", required=True, help="ISO-8601 UTC start of the Strava activity")
    p_merge.add_argument("--out", required=True, help="Path to write merged.fit")
    p_merge.add_argument("--name", default="Merged ride")

    # run-merge ------------------------------------------------------------
    p_run = sub.add_parser(
        "run-merge",
        help="Run the full merge pipeline (Strava + Fitbit + upload) for an activity ID",
    )
    p_run.add_argument("strava_id", type=int)
    p_run.add_argument("--fitbit-log-id", type=int, default=None,
                       help="Force a specific Fitbit log id (skips auto-match)")
    p_run.add_argument("--dry-run", action="store_true",
                       help="Produce the merged FIT but do NOT delete/upload to Strava")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "version":
        from importlib.metadata import version
        print(version("stravafit"))
        return 0

    if args.cmd == "strava":
        if args.strava_cmd == "login":
            return asyncio.run(_strava_login())
        if args.strava_cmd == "list":
            return asyncio.run(_strava_list(args.limit))
        if args.strava_cmd == "fetch-activity":
            return asyncio.run(_strava_fetch_activity(args.activity_id))
        if args.strava_cmd == "streams":
            return asyncio.run(_strava_streams(args.activity_id, args.out))

    if args.cmd == "fitbit":
        if args.fitbit_cmd == "login":
            return asyncio.run(_fitbit_login())
        if args.fitbit_cmd == "list":
            return asyncio.run(_fitbit_list(args.after_date, args.limit))
        if args.fitbit_cmd == "find-near":
            return asyncio.run(_fitbit_find_near(args.when, args.window_minutes))
        if args.fitbit_cmd == "fetch-tcx":
            return asyncio.run(_fitbit_fetch_tcx(args.log_id, args.out))

    if args.cmd == "merge":
        return asyncio.run(_merge(
            args.strava_streams, args.fitbit_tcx, args.start_time, args.out, args.name,
        ))

    if args.cmd == "run-merge":
        return asyncio.run(_run_merge(args.strava_id, args.fitbit_log_id, args.dry_run))

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
