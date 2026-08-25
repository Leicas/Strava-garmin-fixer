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
from app.config import settings
from app.db import init_db
from app.google_health.client import GoogleHealthClient, GoogleNotConfigured
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


async def _strava_delete(activity_id: int) -> int:
    """One-shot probe: try deleting an activity and report the exact result.
    Use a throwaway test activity. Strava's DELETE endpoint behavior has been
    inconsistent over the years; this confirms whether the token can delete."""
    import httpx
    try:
        async with StravaClient.open() as client:
            try:
                await client.delete_activity(activity_id)
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                body = e.response.text[:200]
                if code in (401, 403):
                    print(f"DELETE returned {code} → token lacks delete permission")
                    print("   (re-authorize and check ALL scope boxes, especially activity:write)")
                elif code == 404:
                    print(f"DELETE returned 404 → token CAN delete; activity {activity_id} doesn't exist")
                    print("   This is a successful permission test — try with a real activity id.")
                else:
                    print(f"DELETE returned {code}: {body}")
                return 0
    except StravaNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2

    print(f"DELETE returned 204 — activity {activity_id} was actually deleted.")
    print("  ⚠ It's gone for good. Hope that was a test ride.")
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


# ---------- Garmin commands ----------

async def _garmin_login() -> int:
    """Interactive login: seeds the on-disk token cache (handles MFA).
    After this, headless server logins resume from the cache."""
    from app.garmin.client import GarminClient, GarminNotConfigured

    if not (settings.garmin_email and settings.garmin_password):
        print("Set GARMIN_EMAIL and GARMIN_PASSWORD in .env first.", file=sys.stderr)
        return 2
    try:
        async with GarminClient.open(mfa_prompt=lambda: input("Garmin MFA code: ")) as gc:
            acts = await gc.list_recent_activities(limit=1)
    except GarminNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    print(f"Garmin authorized. Token cache: {settings.garmin_tokens_path}")
    if acts:
        print(f"Latest activity: {acts[0].get('startTimeLocal')}  {acts[0].get('activityName')}")
    return 0


async def _garmin_list(limit: int) -> int:
    from app.garmin.client import GarminClient, GarminNotConfigured

    try:
        async with GarminClient.open() as gc:
            activities = await gc.list_recent_activities(limit=limit)
    except GarminNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    for a in activities:
        type_key = (a.get("activityType") or {}).get("typeKey") or ""
        print(
            f"{(a.get('startTimeLocal') or ''):20}  {a.get('activityId'):>13}  "
            f"{type_key:16}  {(a.get('distance') or 0) / 1000:6.2f} km  "
            f"{(a.get('activityName') or '')[:50]}"
        )
    return 0


async def _garmin_fetch_fit(activity_id: int, out: str | None) -> int:
    from app.garmin.client import GarminClient, GarminNotConfigured
    from app.garmin.fitparse import parse_fit_streams

    try:
        async with GarminClient.open() as gc:
            fit = await gc.download_original_fit(activity_id)
    except GarminNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    if out:
        Path(out).write_bytes(fit)
        print(f"Wrote {out} ({len(fit)} bytes)")
    parsed = parse_fit_streams(fit)
    print(f"start={parsed.start_time.isoformat()} records={parsed.record_count} sport={parsed.sport}")
    for key, payload in parsed.streams.items():
        data = payload["data"]
        n_set = sum(1 for v in data if v is not None)
        print(f"  {key:18} n={len(data):>6}  non-null={n_set}")
    return 0


async def _garmin_delete(activity_id: int) -> int:
    """Probe: try deleting a (throwaway!) Garmin activity to confirm the
    unofficial API's delete works for this account."""
    from app.garmin.client import GarminClient, GarminNotConfigured

    try:
        async with GarminClient.open() as gc:
            await gc.delete_activity(activity_id)
    except GarminNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001 - report the exact failure
        print(f"DELETE failed: {type(e).__name__}: {e}")
        return 1
    print(f"Deleted Garmin activity {activity_id}. ⚠ It's gone for good.")
    return 0


async def _garmin_run_merge(
    garmin_id: int, external_id: str | None, mode: str
) -> int:
    from app.worker import run_merge_job

    await init_db()
    job_id = await jobs_mod.enqueue(
        garmin_id,
        external_id=external_id,
        trigger="manual",
        mode=mode,
        source=jobs_mod.SOURCE_GARMIN,
    )
    print(f"enqueued job {job_id} (garmin_id={garmin_id}, mode={mode})")
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
    return 0 if job["status"] in ("success", "awaiting_delete") else 5


# ---------- Google Health commands ----------

async def _google_list(after_date: str, before_date: str | None) -> int:
    try:
        async with GoogleHealthClient.open() as client:
            activities = await client.list_exercises(after_date=after_date, before_date=before_date)
    except GoogleNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2
    from app.google_health.client import activity_type_label, _exercise_root
    for a in activities:
        ex = _exercise_root(a)
        interval = ex.get("interval") or {}
        name = (a.get("name") or "").rsplit("/", 1)[-1]
        print(
            f"{(interval.get('startTime') or '')[:19]:20}  {name:>20}  "
            f"{activity_type_label(a):14}"
        )
    return 0


async def _google_find_near(when_iso: str, window_minutes: int) -> int:
    try:
        when = datetime.fromisoformat(when_iso.replace("Z", "+00:00"))
    except ValueError:
        print(f"invalid ISO-8601 timestamp: {when_iso}", file=sys.stderr)
        return 2

    try:
        async with GoogleHealthClient.open() as client:
            matches = await client.find_near(when, window_minutes=window_minutes)
    except GoogleNotConfigured as e:
        print(str(e), file=sys.stderr)
        return 2

    if not matches:
        print(f"No Google Health exercises within +/- {window_minutes} min of {when_iso}")
        return 0
    from app.google_health.client import activity_type_label, _exercise_root
    print(f"Found {len(matches)} match(es), closest first:")
    for a in matches:
        ex = _exercise_root(a)
        interval = ex.get("interval") or {}
        name = (a.get("name") or "").rsplit("/", 1)[-1]
        print(
            f"  {(interval.get('startTime') or '')[:19]}  id={name}  "
            f"type={activity_type_label(a) or '—'}"
        )
    return 0


async def _google_fetch_tcx(data_point_id: str, out: str | None) -> int:
    try:
        async with GoogleHealthClient.open() as client:
            tcx = await client.get_exercise_tcx(data_point_id)
    except GoogleNotConfigured as e:
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

async def _run_merge(strava_id: int, external_id: str | None, dry_run: bool) -> int:
    from app.worker import run_merge_job

    await init_db()
    job_id = await jobs_mod.enqueue(
        strava_id,
        external_id=external_id,
        trigger="manual",
        mode=jobs_mod.MODE_DRY_RUN if dry_run else jobs_mod.MODE_AUTO,
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
    strava_sub.add_parser("login", help="Run OAuth flow via local listener on :8001 (dev only)")

    p_list = strava_sub.add_parser("list", help="List recent activities")
    p_list.add_argument("--limit", type=int, default=20)

    p_fetch = strava_sub.add_parser("fetch-activity", help="Print activity + stream summary")
    p_fetch.add_argument("activity_id", type=int)

    p_streams = strava_sub.add_parser("streams", help="Dump raw streams JSON")
    p_streams.add_argument("activity_id", type=int)
    p_streams.add_argument("--out", help="Write to file instead of stdout")

    p_del = strava_sub.add_parser("delete-activity",
        help="Probe: try deleting an activity to confirm token write permission. "
             "Use a throwaway test activity id — if it works, the activity is gone.")
    p_del.add_argument("activity_id", type=int)

    # garmin ---------------------------------------------------------------
    garmin = sub.add_parser("garmin", help="Garmin Connect CLI (unofficial API)")
    garmin_sub = garmin.add_subparsers(dest="garmin_cmd", required=True)
    garmin_sub.add_parser(
        "login",
        help="Interactive credential login (handles MFA) — seeds the token cache "
             "so headless server logins work afterwards",
    )

    p_gm_list = garmin_sub.add_parser("list", help="List recent Garmin activities")
    p_gm_list.add_argument("--limit", type=int, default=20)

    p_gm_fit = garmin_sub.add_parser("fetch-fit", help="Download + summarize an activity's original FIT")
    p_gm_fit.add_argument("activity_id", type=int)
    p_gm_fit.add_argument("--out", help="Also write the raw FIT to this path")

    p_gm_del = garmin_sub.add_parser("delete-activity",
        help="Probe: delete a throwaway Garmin activity to confirm write access works")
    p_gm_del.add_argument("activity_id", type=int)

    p_gm_run = garmin_sub.add_parser(
        "run-merge",
        help="Run the full Garmin merge pipeline (download FIT + Google Health + replace on Garmin)",
    )
    p_gm_run.add_argument("garmin_id", type=int)
    p_gm_run.add_argument("--external-id", default=None,
                          help="Force a specific Google Health exercise data-point id")
    p_gm_run.add_argument("--dry-run", action="store_true",
                          help="Produce the merged FIT but do NOT delete/upload")
    p_gm_run.add_argument("--semi", action="store_true",
                          help="Merge, then pause for manual delete on Garmin Connect")

    # google ---------------------------------------------------------------
    google = sub.add_parser("google", help="Google Health CLI")
    google_sub = google.add_subparsers(dest="google_cmd", required=True)

    p_g_list = google_sub.add_parser("list", help="List exercises after a date")
    p_g_list.add_argument("--after-date", required=True, help="YYYY-MM-DD")
    p_g_list.add_argument("--before-date", help="YYYY-MM-DD (optional upper bound)")

    p_g_near = google_sub.add_parser("find-near", help="Find exercises near an ISO datetime")
    p_g_near.add_argument("when", help="ISO-8601 timestamp, e.g. 2026-04-30T08:00:00Z")
    p_g_near.add_argument("--window-minutes", type=int, default=120)

    p_g_tcx = google_sub.add_parser("fetch-tcx", help="Download an exercise as TCX bytes")
    p_g_tcx.add_argument("data_point_id", help="Numeric id, or full users/.../dataPoints/<id>")
    p_g_tcx.add_argument("--out", help="Write to file (default: stdout, binary)")

    # merge ----------------------------------------------------------------
    p_merge = sub.add_parser("merge", help="Merge streams JSON + TCX -> FIT (no API calls)")
    p_merge.add_argument("--strava-streams", required=True, help="JSON file from `stravafit strava streams`")
    p_merge.add_argument("--fitbit-tcx", required=True, help="TCX file (from `google fetch-tcx`)")
    p_merge.add_argument("--start-time", required=True, help="ISO-8601 UTC start of the Strava activity")
    p_merge.add_argument("--out", required=True, help="Path to write merged.fit")
    p_merge.add_argument("--name", default="Merged ride")

    # run-merge ------------------------------------------------------------
    p_run = sub.add_parser(
        "run-merge",
        help="Run the full merge pipeline (Strava + Google Health + upload) for an activity ID",
    )
    p_run.add_argument("strava_id", type=int)
    p_run.add_argument("--external-id", default=None,
                       help="Force a specific Google Health exercise data-point id (skips auto-match)")
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
        if args.strava_cmd == "delete-activity":
            return asyncio.run(_strava_delete(args.activity_id))

    if args.cmd == "garmin":
        if args.garmin_cmd == "login":
            return asyncio.run(_garmin_login())
        if args.garmin_cmd == "list":
            return asyncio.run(_garmin_list(args.limit))
        if args.garmin_cmd == "fetch-fit":
            return asyncio.run(_garmin_fetch_fit(args.activity_id, args.out))
        if args.garmin_cmd == "delete-activity":
            return asyncio.run(_garmin_delete(args.activity_id))
        if args.garmin_cmd == "run-merge":
            mode = jobs_mod.MODE_AUTO
            if args.dry_run:
                mode = jobs_mod.MODE_DRY_RUN
            elif args.semi:
                mode = jobs_mod.MODE_SEMI_AUTO
            return asyncio.run(_garmin_run_merge(args.garmin_id, args.external_id, mode))

    if args.cmd == "google":
        if args.google_cmd == "list":
            return asyncio.run(_google_list(args.after_date, args.before_date))
        if args.google_cmd == "find-near":
            return asyncio.run(_google_find_near(args.when, args.window_minutes))
        if args.google_cmd == "fetch-tcx":
            return asyncio.run(_google_fetch_tcx(args.data_point_id, args.out))

    if args.cmd == "merge":
        return asyncio.run(_merge(
            args.strava_streams, args.fitbit_tcx, args.start_time, args.out, args.name,
        ))

    if args.cmd == "run-merge":
        return asyncio.run(_run_merge(args.strava_id, args.external_id, args.dry_run))

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
