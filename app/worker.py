from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from app import jobs
from app.config import settings
from app.google_health.client import GoogleHealthClient, GoogleNotConfigured
from app.merge import MergeError, merge_streams_to_fit
from app.strava.client import StravaClient, StravaNotConfigured, StravaUploadError

log = structlog.get_logger()


def _parse_strava_start(raw: str | None) -> datetime:
    """Strava ISO timestamps end with 'Z'. Return tz-aware UTC datetime."""
    if not raw:
        raise ValueError("Strava activity has no start_date")
    s = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def _say(job_id: int, line: str, **kwargs: Any) -> None:
    """Log to structlog AND append a line to jobs.log so the dashboard can replay."""
    log.info(line, job_id=job_id, **kwargs)
    if kwargs:
        suffix = " " + " ".join(f"{k}={v!r}" for k, v in kwargs.items())
        await jobs.append_log(job_id, line + suffix)
    else:
        await jobs.append_log(job_id, line)


async def run_merge_job(job_id: int) -> None:
    """Drive the merge pipeline for a queued job. Must not raise — all errors recorded."""
    job = await jobs.get(job_id)
    if job is None:
        log.warning("worker.job_missing", job_id=job_id)
        return

    strava_id = int(job["strava_id"])
    external_id_in: str | None = job["external_id"] if job["external_id"] is not None else None
    mode = job.get("mode") or (jobs.MODE_DRY_RUN if job.get("dry_run") else jobs.MODE_AUTO)
    dry_run = mode == jobs.MODE_DRY_RUN

    bound = log.bind(job_id=job_id, strava_id=strava_id, mode=mode)
    bound.info("worker.start")
    await jobs.mark_running(job_id)
    await jobs.append_log(job_id, f"start strava_id={strava_id} mode={mode}")

    activity: dict[str, Any]
    streams: dict[str, Any]
    chosen_external_id: str | None = external_id_in
    new_id: int | None = None

    try:
        try:
            async with StravaClient.open() as sc:
                await _say(job_id, "strava.fetch_activity")
                activity = await sc.get_activity(strava_id)
                await _say(job_id, "strava.fetch_streams")
                streams = await sc.get_streams(strava_id)
        except StravaNotConfigured as exc:
            await _say(job_id, "strava.not_configured", err=str(exc))
            await jobs.mark_error(job_id, f"strava_not_configured: {exc}")
            await jobs.record_processed(
                strava_id,
                external_id=external_id_in,
                result="error:strava_not_configured",
            )
            return

        start_dt = _parse_strava_start(activity.get("start_date"))
        await _say(job_id, "strava.parsed_start", start_dt=start_dt.isoformat())

        try:
            async with GoogleHealthClient.open() as gc:
                if chosen_external_id is None:
                    await _say(job_id, "google.find_near", window_minutes=120)
                    matches = await gc.find_near(start_dt, window_minutes=120)
                    if not matches:
                        await _say(job_id, "google.no_match")
                        await jobs.record_processed(
                            strava_id,
                            external_id=None,
                            result="skipped:no_match",
                        )
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
                    await _say(
                        job_id,
                        "google.chose_match",
                        external_id=chosen_external_id,
                        candidates=len(matches),
                    )
                else:
                    await _say(job_id, "google.using_explicit_id", external_id=chosen_external_id)

                await _say(job_id, "google.fetch_tcx", external_id=chosen_external_id)
                tcx = await gc.get_exercise_tcx(chosen_external_id)
        except GoogleNotConfigured as exc:
            await _say(job_id, "google.not_configured", err=str(exc))
            await jobs.mark_error(job_id, f"google_not_configured: {exc}")
            await jobs.record_processed(
                strava_id,
                external_id=external_id_in,
                result="error:google_not_configured",
            )
            return

        activity_name = activity.get("name") or "Merged ride"
        try:
            await _say(job_id, "merge.start", name=activity_name)
            result = merge_streams_to_fit(
                strava_streams=streams,
                strava_start_time=start_dt,
                fitbit_tcx=tcx,
                activity_name=activity_name,
            )
        except MergeError as exc:
            await _say(job_id, "merge.error", err=str(exc))
            await jobs.mark_error(job_id, f"merge_error: {exc}")
            await jobs.record_processed(
                strava_id,
                external_id=chosen_external_id,
                result=f"error:merge:{exc}",
            )
            return

        n_bytes = len(result.fit_bytes)
        warnings = list(getattr(result, "warnings", []) or [])
        await _say(job_id, "merge.done", bytes=n_bytes, warnings=len(warnings))
        for w in warnings:
            await jobs.append_log(job_id, f"warning: {w}")

        # Always save the merged bytes to disk — used by semi-auto pause,
        # by /preview/.../download.fit (separate path), and as the recovery
        # safety net for auto-replace.
        recovery_dir = Path(settings.database_path).parent / "recovery"
        recovery_dir.mkdir(parents=True, exist_ok=True)
        recovery_path = recovery_dir / f"strava-{strava_id}-{int(time.time())}.fit"
        recovery_path.write_bytes(result.fit_bytes)
        await jobs.set_recovery_path(job_id, str(recovery_path))
        await _say(job_id, "recovery.saved", path=str(recovery_path))

        if mode == jobs.MODE_DRY_RUN:
            await jobs.append_log(
                job_id,
                f"dry-run: produced {n_bytes} bytes, {len(warnings)} warnings",
            )
            await jobs.record_processed(
                strava_id,
                external_id=chosen_external_id,
                result="success",
                notes=f"dry_run bytes={n_bytes} warnings={len(warnings)}",
            )
            await jobs.mark_success(job_id)
            return

        if mode == jobs.MODE_SEMI_AUTO:
            # Pause here. The user is expected to delete the original on
            # Strava's web UI (since API DELETE is unreliable), then click
            # "Upload now" on /jobs which calls resume_upload(job_id).
            await jobs.append_log(
                job_id,
                f"semi-auto: merged FIT ready ({n_bytes} bytes). "
                f"Delete the original on Strava, then click 'Upload now'.",
            )
            await jobs.set_status(job_id, "awaiting_delete")
            await _say(job_id, "worker.awaiting_delete", strava_id=strava_id)
            return

        # mode == MODE_AUTO: full automated delete + upload.
        # Strava's content-duplicate detection rejects an upload whose start
        # time matches an existing activity, so we MUST delete the original
        # before uploading. The recovery file above is the safety net.
        base_desc = activity.get("description") or ""
        new_desc = f"{base_desc} {jobs.LOOP_MARKER}".strip()
        original_name = activity.get("name")

        try:
            async with StravaClient.open() as sc:
                await _say(job_id, "strava.delete_original")
                await sc.delete_activity(strava_id)

                await _say(job_id, "strava.upload", bytes=n_bytes)
                resp = await sc.upload(
                    result.fit_bytes,
                    data_type="fit",
                    name=original_name,
                    description=new_desc,
                    external_id=f"stravafit-{strava_id}",
                )
                upload_id = int(resp["id"])
                await _say(job_id, "strava.upload_submitted", upload_id=upload_id)
                final = await sc.wait_for_upload(upload_id, timeout_s=120, poll_s=3)
                new_id = int(final["activity_id"])
                await _say(job_id, "strava.upload_finished", new_activity_id=new_id)

                if original_name:
                    try:
                        await sc.update_activity(new_id, name=original_name)
                        await _say(job_id, "strava.name_restored", name=original_name)
                    except Exception as ne:  # noqa: BLE001 - non-fatal touch-up
                        await _say(job_id, "strava.name_restore_failed", err=str(ne))
        except Exception as exc:
            # Replace failed somewhere between delete and upload+wait. The
            # recovery file on disk is the safety net.
            await _say(job_id, "strava.replace_failed", err=str(exc),
                       recovery=str(recovery_path))
            bound.error("worker.replace_failed", err=str(exc),
                        recovery=str(recovery_path))
            await jobs.append_log(
                job_id,
                f"RECOVERY: merged FIT is on disk at {recovery_path.name}. "
                f"Download it from /jobs/{job_id}/recovery.fit and upload to "
                f"Strava manually to recover.",
            )
            await jobs.mark_error(job_id, f"replace_failed: {exc}")
            await jobs.record_processed(
                strava_id,
                external_id=chosen_external_id,
                result=f"error:replace:{exc}",
                notes=f"recovery_file={recovery_path.name}",
            )
            return

        # Replace succeeded: clean up the recovery file. Best-effort — if
        # cleanup fails the file just sticks around in data/recovery/.
        try:
            recovery_path.unlink(missing_ok=True)
            await jobs.set_recovery_path(job_id, None)
        except OSError as ce:
            await _say(job_id, "recovery.cleanup_failed", err=str(ce))

        await jobs.record_processed(
            strava_id,
            external_id=chosen_external_id,
            result="success",
            notes=f"new_id={new_id}",
        )
        await jobs.mark_success(job_id)
        await _say(job_id, "worker.done", new_activity_id=new_id)

    except Exception as exc:  # noqa: BLE001 - top-level safety net
        bound.exception("worker.unhandled")
        try:
            await jobs.append_log(job_id, f"unhandled error: {exc}")
            await jobs.mark_error(job_id, f"unhandled: {exc}")
            await jobs.record_processed(
                strava_id,
                external_id=chosen_external_id,
                result=f"error:unhandled:{exc}",
            )
        except Exception:  # noqa: BLE001 - last-ditch
            bound.exception("worker.failed_to_record_failure")


async def resume_upload(job_id: int) -> None:
    """Finish a semi-auto job: read the merged FIT off disk and upload to
    Strava. Called by POST /jobs/{id}/finish-upload after the user has
    deleted the original on Strava's web UI. Must not raise."""
    job = await jobs.get(job_id)
    if job is None:
        log.warning("worker.resume_missing", job_id=job_id)
        return
    if job["status"] != "awaiting_delete":
        log.warning("worker.resume_wrong_status", job_id=job_id, status=job["status"])
        return
    rp_str = job.get("recovery_path")
    if not rp_str:
        await jobs.mark_error(job_id, "resume_failed: no recovery_path")
        return
    rp = Path(rp_str)
    if not rp.is_file():
        await jobs.mark_error(job_id, f"resume_failed: recovery file missing at {rp}")
        return

    strava_id = int(job["strava_id"])
    chosen_external_id = job.get("external_id")
    bound = log.bind(job_id=job_id, strava_id=strava_id)
    bound.info("worker.resume.start")
    await jobs.set_status(job_id, "running")
    await jobs.append_log(job_id, "resume: uploading merged FIT")

    try:
        fit_bytes = rp.read_bytes()
        async with StravaClient.open() as sc:
            # Best-effort: pull the original name/description for the new upload.
            original_name: str | None = None
            base_desc = ""
            try:
                act = await sc.get_activity(strava_id)
                original_name = act.get("name")
                base_desc = act.get("description") or ""
            except Exception:  # noqa: BLE001 - if 404 (already deleted) just continue
                await _say(job_id, "strava.original_lookup_failed",
                           note="probably already deleted, continuing")

            new_desc = f"{base_desc} {jobs.LOOP_MARKER}".strip()

            await _say(job_id, "strava.upload", bytes=len(fit_bytes))
            resp = await sc.upload(
                fit_bytes,
                data_type="fit",
                name=original_name,
                description=new_desc,
                external_id=f"stravafit-{strava_id}",
            )
            upload_id = int(resp["id"])
            await _say(job_id, "strava.upload_submitted", upload_id=upload_id)
            final = await sc.wait_for_upload(upload_id, timeout_s=120, poll_s=3)
            new_id = int(final["activity_id"])
            await _say(job_id, "strava.upload_finished", new_activity_id=new_id)

            if original_name:
                try:
                    await sc.update_activity(new_id, name=original_name)
                    await _say(job_id, "strava.name_restored", name=original_name)
                except Exception as ne:  # noqa: BLE001 - non-fatal touch-up
                    await _say(job_id, "strava.name_restore_failed", err=str(ne))
    except Exception as exc:  # noqa: BLE001 - surface in job state
        await _say(job_id, "strava.resume_failed", err=str(exc))
        bound.error("worker.resume_failed", err=str(exc))
        await jobs.append_log(
            job_id,
            f"RECOVERY: merged FIT is still on disk. Most likely cause: "
            f"the original wasn't actually deleted yet (Strava dedup), or "
            f"a transient API error. Click 'Upload now' again to retry, or "
            f"download the recovery FIT and upload manually via Strava UI.",
        )
        # Keep status as awaiting_delete so the user can retry from the UI.
        await jobs.set_status(job_id, "awaiting_delete")
        return

    # Success: clean up the recovery file.
    try:
        rp.unlink(missing_ok=True)
        await jobs.set_recovery_path(job_id, None)
    except OSError as ce:
        await _say(job_id, "recovery.cleanup_failed", err=str(ce))

    await jobs.record_processed(
        strava_id,
        external_id=chosen_external_id,
        result="success",
        notes=f"new_id={new_id}",
    )
    await jobs.mark_success(job_id)
    await _say(job_id, "worker.resume.done", new_activity_id=new_id)
