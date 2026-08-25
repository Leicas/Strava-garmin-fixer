"""Merge pipeline for Garmin-sourced jobs.

Mirrors app.worker.run_merge_job but with Garmin Connect as both the
source (original FIT download) and the replace target (delete + upload).
The Google Health matching, merge core, and recovery-file safety net are
identical to the Strava path.

Strava note: with Garmin→Strava native sync enabled, the ORIGINAL activity
reaches Strava seconds after the ride ends and the merged replacement syncs
over later as a separate activity. Without Strava API access the original
can't be deleted programmatically — the job log reminds the user to clean
it up in the Strava web UI.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import structlog

from app import jobs
from app.config import settings
from app.garmin.client import GarminClient, GarminNotConfigured, parse_start_gmt
from app.garmin.fitparse import FitParseError, parse_fit_streams
from app.google_health.client import GoogleHealthClient, GoogleNotConfigured
from app.merge import MergeError, merge_streams_to_fit

log = structlog.get_logger()

STRAVA_CLEANUP_HINT = (
    "Reminder: the ORIGINAL already synced to Strava and can't be deleted via "
    "API — delete it on strava.com; the merged version syncs over from Garmin."
)


async def _say(job_id: int, line: str, **kwargs: Any) -> None:
    log.info(line, job_id=job_id, **kwargs)
    if kwargs:
        suffix = " " + " ".join(f"{k}={v!r}" for k, v in kwargs.items())
        await jobs.append_log(job_id, line + suffix)
    else:
        await jobs.append_log(job_id, line)


async def _record(garmin_id: int, **kwargs: Any) -> None:
    await jobs.record_processed(garmin_id, source=jobs.SOURCE_GARMIN, **kwargs)


async def _export_to_dreeve(job_id: int, garmin_id: int, fit_bytes: bytes, kind: str) -> None:
    """Drop the definitive FIT into <data>/export/ for the Dreeve puller.

    Atomic (tmp + rename) so the puller never sees a partial file. Name is
    deterministic per activity, so a re-run overwrites rather than duplicates.
    No-op when the hand-off is disabled.
    """
    if not settings.dreeve_export_enabled:
        return
    export_dir = settings.dreeve_export_dir
    export_dir.mkdir(parents=True, exist_ok=True)
    final = export_dir / f"stravafit-garmin-{garmin_id}.fit"
    tmp = export_dir / f".{final.name}.tmp"
    tmp.write_bytes(fit_bytes)
    tmp.replace(final)
    await _say(job_id, "dreeve.exported", file=final.name, kind=kind, bytes=len(fit_bytes))


async def run_garmin_merge_job(job_id: int) -> None:
    """Drive the merge pipeline for a Garmin-sourced job. Must not raise."""
    job = await jobs.get(job_id)
    if job is None:
        log.warning("garmin_worker.job_missing", job_id=job_id)
        return

    garmin_id = int(job["strava_id"])  # legacy column name; holds the Garmin activityId
    external_id_in: str | None = job["external_id"] if job["external_id"] is not None else None
    mode = job.get("mode") or jobs.MODE_AUTO

    bound = log.bind(job_id=job_id, garmin_id=garmin_id, mode=mode)
    bound.info("garmin_worker.start")
    await jobs.mark_running(job_id)
    await jobs.append_log(job_id, f"start garmin_id={garmin_id} mode={mode}")

    chosen_external_id: str | None = external_id_in

    try:
        try:
            async with GarminClient.open() as gc:
                await _say(job_id, "garmin.fetch_summary")
                summary = await gc.get_activity(garmin_id)
                await _say(job_id, "garmin.download_original")
                fit_bytes_in = await gc.download_original_fit(garmin_id)
        except GarminNotConfigured as exc:
            await _say(job_id, "garmin.not_configured", err=str(exc))
            await jobs.mark_error(job_id, f"garmin_not_configured: {exc}")
            await _record(garmin_id, external_id=external_id_in,
                          result="error:garmin_not_configured")
            return

        try:
            parsed = parse_fit_streams(fit_bytes_in)
        except FitParseError as exc:
            await _say(job_id, "garmin.fit_parse_error", err=str(exc))
            await jobs.mark_error(job_id, f"fit_parse_error: {exc}")
            await _record(garmin_id, external_id=external_id_in,
                          result=f"error:fit_parse:{exc}")
            return

        start_dt = parsed.start_time
        activity_name = "Merged ride"
        if summary:
            activity_name = (
                summary.get("activityName")
                or (summary.get("summaryDTO") or {}).get("activityName")
                or activity_name
            )
            start_dt = parse_start_gmt(summary) or start_dt
        await _say(job_id, "garmin.parsed",
                   start_dt=start_dt.isoformat(), records=parsed.record_count,
                   name=activity_name)

        try:
            async with GoogleHealthClient.open() as gclient:
                if chosen_external_id is None:
                    await _say(job_id, "google.find_near", window_minutes=120)
                    matches = await gclient.find_near(start_dt, window_minutes=120)
                    if not matches:
                        await _say(job_id, "google.no_match")
                        if settings.dreeve_export_enabled and mode != jobs.MODE_DRY_RUN:
                            # No HR source to merge — Dreeve still needs the
                            # activity, so deliver the original as-is. Final:
                            # Dreeve dedups on (sport, start), so a later
                            # merged version of this activity would be skipped.
                            await _export_to_dreeve(job_id, garmin_id, fit_bytes_in, "original")
                            await _record(garmin_id, external_id=None,
                                          result="passthrough:no_match")
                        else:
                            await _record(garmin_id, external_id=None,
                                          result="skipped:no_match")
                        await jobs.mark_success(job_id)
                        return
                    first = matches[0]
                    name = first.get("name") or ""
                    chosen_external_id = (
                        name.rsplit("/", 1)[-1] if "/dataPoints/" in name else None
                    )
                    if not chosen_external_id:
                        await _say(job_id, "google.match_missing_id", first=first)
                        await jobs.mark_error(job_id, "google match has no parseable name")
                        return
                    await _say(job_id, "google.chose_match",
                               external_id=chosen_external_id, candidates=len(matches))
                else:
                    await _say(job_id, "google.using_explicit_id",
                               external_id=chosen_external_id)

                await _say(job_id, "google.fetch_tcx", external_id=chosen_external_id)
                tcx = await gclient.get_exercise_tcx(chosen_external_id)
        except GoogleNotConfigured as exc:
            await _say(job_id, "google.not_configured", err=str(exc))
            await jobs.mark_error(job_id, f"google_not_configured: {exc}")
            await _record(garmin_id, external_id=external_id_in,
                          result="error:google_not_configured")
            return

        try:
            await _say(job_id, "merge.start", name=activity_name)
            result = merge_streams_to_fit(
                strava_streams=parsed.streams,
                strava_start_time=start_dt,
                source_tcx=tcx,
                activity_name=activity_name,
            )
        except MergeError as exc:
            await _say(job_id, "merge.error", err=str(exc))
            await jobs.mark_error(job_id, f"merge_error: {exc}")
            await _record(garmin_id, external_id=chosen_external_id,
                          result=f"error:merge:{exc}")
            return

        n_bytes = len(result.fit_bytes)
        warnings = list(getattr(result, "warnings", []) or [])
        await _say(job_id, "merge.done", bytes=n_bytes, warnings=len(warnings))
        for w in warnings:
            await jobs.append_log(job_id, f"warning: {w}")

        recovery_dir = Path(settings.database_path).parent / "recovery"
        recovery_dir.mkdir(parents=True, exist_ok=True)
        recovery_path = recovery_dir / f"garmin-{garmin_id}-{int(time.time())}.fit"
        recovery_path.write_bytes(result.fit_bytes)
        await jobs.set_recovery_path(job_id, str(recovery_path))
        await _say(job_id, "recovery.saved", path=str(recovery_path))

        if mode == jobs.MODE_DRY_RUN:
            await jobs.append_log(
                job_id,
                f"dry-run: produced {n_bytes} bytes, {len(warnings)} warnings",
            )
            await _record(garmin_id, external_id=chosen_external_id, result="success",
                          notes=f"dry_run bytes={n_bytes} warnings={len(warnings)}")
            await jobs.mark_success(job_id)
            return

        if mode == jobs.MODE_SEMI_AUTO:
            await jobs.append_log(
                job_id,
                f"semi-auto: merged FIT ready ({n_bytes} bytes). Delete the "
                f"original on Garmin Connect, then click 'Upload now'.",
            )
            await jobs.set_status(job_id, "awaiting_delete")
            await _say(job_id, "garmin_worker.awaiting_delete", garmin_id=garmin_id)
            return

        # MODE_AUTO: delete original on Garmin, then upload the merged FIT.
        # Garmin also rejects duplicate uploads for the same timeframe, so the
        # delete must come first; the recovery file is the safety net.
        try:
            async with GarminClient.open() as gc:
                await _say(job_id, "garmin.delete_original")
                await gc.delete_activity(garmin_id)

                await _say(job_id, "garmin.upload", bytes=n_bytes)
                resp = await gc.upload_fit(
                    result.fit_bytes, stem=f"stravafit-{garmin_id}"
                )
                failures = gc.upload_failures(resp)
                if failures:
                    raise RuntimeError(f"garmin upload rejected: {'; '.join(failures)[:300]}")
                new_id = gc.uploaded_activity_id(resp)
                await _say(job_id, "garmin.upload_finished", new_activity_id=new_id)

                if new_id is not None and activity_name:
                    try:
                        await gc.set_activity_name(new_id, activity_name)
                        await _say(job_id, "garmin.name_restored", name=activity_name)
                    except Exception as ne:  # noqa: BLE001 - non-fatal touch-up
                        await _say(job_id, "garmin.name_restore_failed", err=str(ne))
        except Exception as exc:  # noqa: BLE001 - surface in job state
            await _say(job_id, "garmin.replace_failed", err=str(exc),
                       recovery=str(recovery_path))
            bound.error("garmin_worker.replace_failed", err=str(exc),
                        recovery=str(recovery_path))
            await jobs.append_log(
                job_id,
                f"RECOVERY: merged FIT is on disk at {recovery_path.name}. "
                f"Download it from /jobs/{job_id}/recovery.fit and upload to "
                f"Garmin Connect manually (Import Data) to recover.",
            )
            await jobs.mark_error(job_id, f"replace_failed: {exc}")
            await _record(garmin_id, external_id=chosen_external_id,
                          result=f"error:replace:{exc}",
                          notes=f"recovery_file={recovery_path.name}")
            return

        try:
            recovery_path.unlink(missing_ok=True)
            await jobs.set_recovery_path(job_id, None)
        except OSError as ce:
            await _say(job_id, "recovery.cleanup_failed", err=str(ce))

        await _export_to_dreeve(job_id, garmin_id, result.fit_bytes, "merged")
        await jobs.append_log(job_id, STRAVA_CLEANUP_HINT)
        await _record(garmin_id, external_id=chosen_external_id, result="success",
                      notes=f"new_id={new_id} strava_cleanup=manual")
        await jobs.mark_success(job_id)
        await _say(job_id, "garmin_worker.done", new_activity_id=new_id)

    except Exception as exc:  # noqa: BLE001 - top-level safety net
        bound.exception("garmin_worker.unhandled")
        try:
            await jobs.append_log(job_id, f"unhandled error: {exc}")
            await jobs.mark_error(job_id, f"unhandled: {exc}")
            await _record(garmin_id, external_id=chosen_external_id,
                          result=f"error:unhandled:{exc}")
        except Exception:  # noqa: BLE001 - last-ditch
            bound.exception("garmin_worker.failed_to_record_failure")


async def resume_garmin_upload(job_id: int) -> None:
    """Finish a semi-auto Garmin job: upload the merged FIT after the user has
    deleted the original in the Garmin Connect UI. Must not raise."""
    job = await jobs.get(job_id)
    if job is None:
        log.warning("garmin_worker.resume_missing", job_id=job_id)
        return
    if job["status"] != "awaiting_delete":
        log.warning("garmin_worker.resume_wrong_status", job_id=job_id, status=job["status"])
        return
    rp_str = job.get("recovery_path")
    if not rp_str:
        await jobs.mark_error(job_id, "resume_failed: no recovery_path")
        return
    rp = Path(rp_str)
    if not rp.is_file():
        await jobs.mark_error(job_id, f"resume_failed: recovery file missing at {rp}")
        return

    garmin_id = int(job["strava_id"])
    chosen_external_id = job.get("external_id")
    bound = log.bind(job_id=job_id, garmin_id=garmin_id)
    bound.info("garmin_worker.resume.start")
    await jobs.set_status(job_id, "running")
    await jobs.append_log(job_id, "resume: uploading merged FIT to Garmin")

    new_id: int | None = None
    try:
        fit_bytes = rp.read_bytes()
        async with GarminClient.open() as gc:
            await _say(job_id, "garmin.upload", bytes=len(fit_bytes))
            resp = await gc.upload_fit(fit_bytes, stem=f"stravafit-{garmin_id}")
            failures = gc.upload_failures(resp)
            if failures:
                raise RuntimeError(f"garmin upload rejected: {'; '.join(failures)[:300]}")
            new_id = gc.uploaded_activity_id(resp)
            await _say(job_id, "garmin.upload_finished", new_activity_id=new_id)
    except Exception as exc:  # noqa: BLE001 - surface in job state
        await _say(job_id, "garmin.resume_failed", err=str(exc))
        bound.error("garmin_worker.resume_failed", err=str(exc))
        await jobs.append_log(
            job_id,
            "RECOVERY: merged FIT is still on disk. Most likely the original "
            "wasn't actually deleted yet (Garmin duplicate detection), or a "
            "transient error. Click 'Upload now' again to retry, or download "
            "the recovery FIT and import it manually on connect.garmin.com.",
        )
        await jobs.set_status(job_id, "awaiting_delete")
        return

    try:
        rp.unlink(missing_ok=True)
        await jobs.set_recovery_path(job_id, None)
    except OSError as ce:
        await _say(job_id, "recovery.cleanup_failed", err=str(ce))

    await _export_to_dreeve(job_id, garmin_id, fit_bytes, "merged")
    await jobs.append_log(job_id, STRAVA_CLEANUP_HINT)
    await _record(garmin_id, external_id=chosen_external_id, result="success",
                  notes=f"new_id={new_id} strava_cleanup=manual")
    await jobs.mark_success(job_id)
    await _say(job_id, "garmin_worker.resume.done", new_activity_id=new_id)
