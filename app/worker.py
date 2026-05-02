from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from app import jobs
from app.fitbit.client import FitbitClient, FitbitNotConfigured
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
    fitbit_log_id_in: int | None = (
        int(job["fitbit_log_id"]) if job["fitbit_log_id"] is not None else None
    )
    dry_run = bool(job["dry_run"])

    bound = log.bind(job_id=job_id, strava_id=strava_id, dry_run=dry_run)
    bound.info("worker.start")
    await jobs.mark_running(job_id)
    await jobs.append_log(job_id, f"start strava_id={strava_id} dry_run={dry_run}")

    activity: dict[str, Any]
    streams: dict[str, Any]
    chosen_log_id: int | None = fitbit_log_id_in
    new_id: int | None = None

    try:
        # Step 2: pull Strava activity + streams
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
                fitbit_log_id=fitbit_log_id_in,
                result=f"error:strava_not_configured",
            )
            return

        start_dt = _parse_strava_start(activity.get("start_date"))
        await _say(job_id, "strava.parsed_start", start_dt=start_dt.isoformat())

        # Step 3: pull Fitbit TCX (find_near if no explicit log id)
        try:
            async with FitbitClient.open() as fc:
                if chosen_log_id is None:
                    await _say(job_id, "fitbit.find_near", window_minutes=120)
                    matches = await fc.find_near(start_dt, window_minutes=120)
                    if not matches:
                        await _say(job_id, "fitbit.no_match")
                        await jobs.record_processed(
                            strava_id,
                            fitbit_log_id=None,
                            result="skipped:no_match",
                        )
                        await jobs.mark_success(job_id)
                        return
                    first = matches[0]
                    chosen_log_id = int(first["logId"])
                    await _say(
                        job_id,
                        "fitbit.chose_match",
                        log_id=chosen_log_id,
                        candidates=len(matches),
                    )
                else:
                    await _say(job_id, "fitbit.using_explicit_log_id", log_id=chosen_log_id)

                await _say(job_id, "fitbit.fetch_tcx", log_id=chosen_log_id)
                tcx = await fc.get_activity_tcx(chosen_log_id)
        except FitbitNotConfigured as exc:
            await _say(job_id, "fitbit.not_configured", err=str(exc))
            await jobs.mark_error(job_id, f"fitbit_not_configured: {exc}")
            await jobs.record_processed(
                strava_id,
                fitbit_log_id=fitbit_log_id_in,
                result="error:fitbit_not_configured",
            )
            return

        # Step 4: merge
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
                fitbit_log_id=chosen_log_id,
                result=f"error:merge:{exc}",
            )
            return

        n_bytes = len(result.fit_bytes)
        warnings = list(getattr(result, "warnings", []) or [])
        await _say(
            job_id, "merge.done", bytes=n_bytes, warnings=len(warnings)
        )
        for w in warnings:
            await jobs.append_log(job_id, f"warning: {w}")

        # Step 5: dry-run short-circuit
        if dry_run:
            await jobs.append_log(
                job_id,
                f"dry-run: produced {n_bytes} bytes, {len(warnings)} warnings",
            )
            await jobs.record_processed(
                strava_id,
                fitbit_log_id=chosen_log_id,
                result="success",
                notes=f"dry_run bytes={n_bytes} warnings={len(warnings)}",
            )
            await jobs.mark_success(job_id)
            return

        # Step 6: delete original + upload merged
        base_desc = activity.get("description") or ""
        new_desc = f"{base_desc} {jobs.LOOP_MARKER}".strip()
        original_name = activity.get("name")

        # Capture bytes for potential restore — we don't have the original .fit
        # bytes (Strava streams != original file), so the recovery story is to
        # log loudly. See the task notes.
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

                # Strava sometimes overwrites the name from FIT metadata.
                # If we had an original name, force it back.
                if original_name:
                    try:
                        await sc.update_activity(new_id, name=original_name)
                        await _say(job_id, "strava.name_restored", name=original_name)
                    except Exception as ne:  # noqa: BLE001 - non-fatal touch-up
                        await _say(job_id, "strava.name_restore_failed", err=str(ne))
        except Exception as exc:
            # Original may already be deleted. We do not have the original .fit
            # bytes locally, so the safest thing is to record loudly and stop.
            # Future improvement noted in the task spec: re-upload the unmodified
            # file BEFORE deleting next time.
            await _say(
                job_id,
                "strava.upload_failed_after_delete",
                err=str(exc),
                strava_id=strava_id,
            )
            bound.error("worker.upload_failed_after_delete", err=str(exc))
            await jobs.append_log(
                job_id,
                "ALERT: original activity may have been deleted and re-upload failed. "
                "Manual review required.",
            )
            await jobs.mark_error(job_id, f"upload_failed_after_delete: {exc}")
            await jobs.record_processed(
                strava_id,
                fitbit_log_id=chosen_log_id,
                result=f"error:upload_failed_after_delete:{exc}",
                notes="original may be lost; manual review required",
            )
            return

        # Step 7: record success
        await jobs.record_processed(
            strava_id,
            fitbit_log_id=chosen_log_id,
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
                fitbit_log_id=chosen_log_id,
                result=f"error:unhandled:{exc}",
            )
        except Exception:  # noqa: BLE001 - last-ditch
            bound.exception("worker.failed_to_record_failure")
