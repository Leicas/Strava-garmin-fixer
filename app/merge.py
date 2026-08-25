"""Pure, deterministic merge of source TCX (GPS + HR — historically Fitbit, now
Google Health) and Strava streams (cadence, speed, temperature, altitude) into
a Strava-uploadable FIT file.

This module is intentionally side-effect free:
  - No I/O.
  - No `datetime.now()` or other clock reads.
  - No randomness.
  - Same inputs => byte-identical output.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timezone

from lxml import etree

from fit_tool.fit_file_builder import FitFileBuilder
from fit_tool.profile.messages.event_message import EventMessage
from fit_tool.profile.messages.file_id_message import FileIdMessage
from fit_tool.profile.messages.lap_message import LapMessage
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage
from fit_tool.profile.profile_type import (
    Event,
    EventType,
    FileType,
    Manufacturer,
    Sport,
    SubSport,
)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MergeWarning:
    code: str
    message: str


@dataclass(frozen=True)
class MergeResult:
    fit_bytes: bytes
    record_count: int
    warnings: tuple[MergeWarning, ...]
    distance_meters: float
    gps_source: str = "none"  # "strava", "google_health", or "none"


class MergeError(RuntimeError):
    """Raised when the merge cannot produce a trustworthy output."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TCD_NS = {"tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
_EARTH_RADIUS_M = 6371008.8  # mean earth radius (meters)


# ---------------------------------------------------------------------------
# TCX parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Trackpoint:
    epoch_s: float  # seconds since unix epoch (UTC)
    lat: float | None
    lon: float | None
    hr: int | None


def _parse_tcx_time(value: str) -> datetime:
    """Parse an ISO 8601 UTC timestamp from TCX (e.g. ``2024-01-02T03:04:05.000Z``)."""
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_tcx(tcx_bytes: bytes) -> list[_Trackpoint]:
    """Parse trackpoints from a TCX byte string. Returns chronologically sorted points."""
    if not tcx_bytes:
        raise MergeError("Empty TCX payload")
    try:
        root = etree.fromstring(tcx_bytes)
    except etree.XMLSyntaxError as exc:
        raise MergeError(f"Invalid TCX XML: {exc}") from exc

    points: list[_Trackpoint] = []
    for tp in root.iterfind(".//tcd:Trackpoint", _TCD_NS):
        time_el = tp.find("tcd:Time", _TCD_NS)
        if time_el is None or time_el.text is None:
            # No time on a trackpoint => unusable
            continue
        ts = _parse_tcx_time(time_el.text)

        lat: float | None = None
        lon: float | None = None
        pos = tp.find("tcd:Position", _TCD_NS)
        if pos is not None:
            lat_el = pos.find("tcd:LatitudeDegrees", _TCD_NS)
            lon_el = pos.find("tcd:LongitudeDegrees", _TCD_NS)
            if lat_el is not None and lat_el.text and lon_el is not None and lon_el.text:
                lat = float(lat_el.text)
                lon = float(lon_el.text)

        hr: int | None = None
        hr_el = tp.find("tcd:HeartRateBpm/tcd:Value", _TCD_NS)
        if hr_el is not None and hr_el.text:
            try:
                hr = int(float(hr_el.text))
            except ValueError:
                hr = None

        points.append(_Trackpoint(epoch_s=ts.timestamp(), lat=lat, lon=lon, hr=hr))

    points.sort(key=lambda p: p.epoch_s)
    return points


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


def _interp_position(
    points_with_pos: list[_Trackpoint],
    times: list[float],
    target_s: float,
) -> tuple[float | None, float | None, bool]:
    """Linearly interpolate (lat, lon) at ``target_s`` (seconds since epoch).

    Returns (lat, lon, extrapolated). Coordinates may be ``None`` if no
    positioned trackpoints are available.
    """
    if not points_with_pos:
        return None, None, False
    if target_s <= times[0]:
        p = points_with_pos[0]
        return p.lat, p.lon, target_s < times[0]
    if target_s >= times[-1]:
        p = points_with_pos[-1]
        return p.lat, p.lon, target_s > times[-1]

    idx = bisect_left(times, target_s)
    # idx > 0 because target_s > times[0]
    a = points_with_pos[idx - 1]
    b = points_with_pos[idx]
    span = times[idx] - times[idx - 1]
    if span <= 0:
        return a.lat, a.lon, False
    frac = (target_s - times[idx - 1]) / span
    lat = a.lat + (b.lat - a.lat) * frac  # type: ignore[operator]
    lon = a.lon + (b.lon - a.lon) * frac  # type: ignore[operator]
    return lat, lon, False


def _nearest_hr(
    points_with_hr: list[_Trackpoint],
    times: list[float],
    target_s: float,
) -> int | None:
    if not points_with_hr:
        return None
    idx = bisect_left(times, target_s)
    if idx <= 0:
        return points_with_hr[0].hr
    if idx >= len(points_with_hr):
        return points_with_hr[-1].hr
    before = points_with_hr[idx - 1]
    after = points_with_hr[idx]
    if abs(target_s - times[idx - 1]) <= abs(times[idx] - target_s):
        return before.hr
    return after.hr


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return _EARTH_RADIUS_M * c


# ---------------------------------------------------------------------------
# Stream extraction
# ---------------------------------------------------------------------------


def _extract_stream(streams: dict, key: str) -> list | None:
    blob = streams.get(key)
    if blob is None:
        return None
    if isinstance(blob, dict):
        data = blob.get("data")
    else:
        data = blob
    if data is None:
        return None
    return list(data)


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


def _strava_latlng_trackpoints(
    strava_streams: dict,
    start_epoch_s: float,
) -> list[_Trackpoint]:
    """Build Trackpoints from Strava's ``latlng`` + ``time`` streams.

    Returns ``[]`` when Strava has no GPS data (the common case for indoor
    activities or non-GPS Edge head units).
    """
    times = _extract_stream(strava_streams, "time") or []
    latlng = _extract_stream(strava_streams, "latlng") or []
    if not times or not latlng:
        return []
    out: list[_Trackpoint] = []
    for i in range(min(len(times), len(latlng))):
        pair = latlng[i]
        if not pair or len(pair) != 2:
            continue
        lat, lon = pair[0], pair[1]
        if lat is None or lon is None:
            continue
        try:
            t_off = float(times[i])
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue
        out.append(_Trackpoint(
            epoch_s=start_epoch_s + t_off,
            lat=lat_f,
            lon=lon_f,
            hr=None,
        ))
    return out


def _haversine_total_m(points: list[_Trackpoint]) -> float:
    """Total path length in meters along the trackpoints (lat/lon must be set)."""
    total = 0.0
    prev: _Trackpoint | None = None
    for p in points:
        if prev is not None and prev.lat is not None and p.lat is not None:
            total += _haversine_m(prev.lat, prev.lon, p.lat, p.lon)  # type: ignore[arg-type]
        prev = p
    return total


def merge_streams_to_fit(
    *,
    strava_streams: dict,
    strava_start_time: datetime,
    fitbit_tcx: bytes | None = None,
    source_tcx: bytes | None = None,
    activity_name: str = "Merged ride",
    distance_tolerance: float = 0.10,
) -> MergeResult:
    """Combine a source TCX (GPS + HR) with Strava streams into a FIT file.

    The source TCX historically came from Fitbit; today it comes from Google
    Health (which inherited the Fitbit data). The keyword argument
    ``fitbit_tcx`` is kept as an alias for ``source_tcx`` for backwards
    compatibility — exactly one must be provided.

    Picks the more accurate GPS source automatically:
      - If only one of {Strava ``latlng``, source TCX} has GPS, that one wins.
      - If both have GPS, picks whichever haversine total is closer to Strava's
        ``distance`` stream.
      - The strict distance sanity check is only enforced when Strava itself
        contributed GPS truth (its ``latlng`` stream); otherwise we have no
        independent GPS to compare against and a stream-distance mismatch is
        downgraded to a warning. This matters for indoor / non-GPS Edge head
        units where Strava records distance from a wheel sensor.

    Pure / deterministic: same inputs produce byte-identical output.
    """
    if (fitbit_tcx is None) == (source_tcx is None):
        raise MergeError("Pass exactly one of fitbit_tcx= or source_tcx=")
    tcx_bytes = source_tcx if source_tcx is not None else fitbit_tcx
    assert tcx_bytes is not None  # for type checkers

    if strava_start_time.tzinfo is None:
        raise MergeError("strava_start_time must be timezone-aware (UTC)")
    start_utc = strava_start_time.astimezone(timezone.utc)
    start_epoch_s = start_utc.timestamp()

    times = _extract_stream(strava_streams, "time")
    if not times:
        raise MergeError("Strava streams missing required 'time' axis")

    distances = _extract_stream(strava_streams, "distance") or []
    speeds = _extract_stream(strava_streams, "velocity_smooth") or []
    cadences = _extract_stream(strava_streams, "cadence") or []
    temps = _extract_stream(strava_streams, "temp") or []
    altitudes = _extract_stream(strava_streams, "altitude") or []
    # heartrate from Strava is intentionally ignored: we trust the source TCX's HR.
    # If the source has no HR we may fall back later (not in this version).

    # Parse both potential GPS sources.
    tcx_trackpoints = _parse_tcx(tcx_bytes)
    tcx_pos = [p for p in tcx_trackpoints if p.lat is not None and p.lon is not None]
    strava_pos = _strava_latlng_trackpoints(strava_streams, start_epoch_s)
    points_with_hr = [p for p in tcx_trackpoints if p.hr is not None]
    hr_times = [p.epoch_s for p in points_with_hr]

    # Pre-compute Strava's stream-distance total for source picking + sanity check.
    strava_total_distance: float
    if distances:
        last_valid: float | None = None
        for d in distances:
            if d is None:
                continue
            try:
                last_valid = float(d)
            except (TypeError, ValueError):
                continue
        strava_total_distance = float(last_valid) if last_valid is not None else 0.0
    else:
        strava_total_distance = 0.0

    # Pick the GPS source. When both exist, the one whose haversine total is
    # closer to Strava's reported distance wins; this favors the device that
    # actually recorded the activity (no GPS drift through tunnels, etc.).
    warnings: list[MergeWarning] = []

    def _err(points: list[_Trackpoint]) -> float | None:
        if not points or strava_total_distance <= 0:
            return None
        return abs(_haversine_total_m(points) - strava_total_distance)

    tcx_err = _err(tcx_pos)
    strava_err = _err(strava_pos)

    gps_source = "none"
    points_with_pos: list[_Trackpoint] = []
    if strava_pos and tcx_pos:
        if strava_err is not None and tcx_err is not None and strava_err <= tcx_err:
            points_with_pos = strava_pos
            gps_source = "strava"
        elif strava_err is not None and tcx_err is not None:
            points_with_pos = tcx_pos
            gps_source = "google_health"
        else:
            # No Strava distance to compare against — prefer Strava (recording
            # device's own GPS) as it's normally truer to the route.
            points_with_pos = strava_pos
            gps_source = "strava"
        # Only warn when the picker actually had a meaningful decision to make
        # (one source materially worse than the other). Routine merges where
        # both sources agree shouldn't generate noise.
        if (
            strava_err is not None and tcx_err is not None
            and strava_total_distance > 0
            and (abs(strava_err - tcx_err) / strava_total_distance) > 0.05
        ):
            warnings.append(MergeWarning(
                code="gps_source_picked",
                message=(
                    f"Both sources have GPS (strava={len(strava_pos)} pts, "
                    f"google={len(tcx_pos)} pts). Chose {gps_source} "
                    f"(error vs strava distance: strava={strava_err:.0f}m, "
                    f"google={tcx_err:.0f}m)."
                ),
            ))
    elif strava_pos:
        points_with_pos = strava_pos
        gps_source = "strava"
    elif tcx_pos:
        points_with_pos = tcx_pos
        gps_source = "google_health"
    pos_times = [p.epoch_s for p in points_with_pos]

    extrapolated_count = 0

    # Build records.
    records: list[RecordMessage] = []
    prev_lat: float | None = None
    prev_lon: float | None = None
    merged_distance_m = 0.0

    n = len(times)
    for i in range(n):
        try:
            t_off = float(times[i])
        except (TypeError, ValueError):
            continue
        sample_epoch_s = start_epoch_s + t_off

        lat, lon, extrapolated = _interp_position(points_with_pos, pos_times, sample_epoch_s)
        if extrapolated:
            extrapolated_count += 1

        if lat is not None and lon is not None and prev_lat is not None and prev_lon is not None:
            merged_distance_m += _haversine_m(prev_lat, prev_lon, lat, lon)
        if lat is not None and lon is not None:
            prev_lat, prev_lon = lat, lon

        hr = _nearest_hr(points_with_hr, hr_times, sample_epoch_s)

        rec = RecordMessage()
        # timestamp: ms since unix epoch (fit-tool converts internally)
        rec.timestamp = int(round(sample_epoch_s * 1000.0))
        if lat is not None and lon is not None:
            rec.position_lat = float(lat)
            rec.position_long = float(lon)
        if hr is not None:
            rec.heart_rate = int(hr)

        if i < len(speeds) and speeds[i] is not None:
            try:
                rec.speed = float(speeds[i])
            except (TypeError, ValueError):
                pass
        if i < len(cadences) and cadences[i] is not None:
            try:
                rec.cadence = int(round(float(cadences[i])))
            except (TypeError, ValueError):
                pass
        if i < len(distances) and distances[i] is not None:
            try:
                rec.distance = float(distances[i])
            except (TypeError, ValueError):
                pass
        if i < len(temps) and temps[i] is not None:
            try:
                rec.temperature = int(round(float(temps[i])))
            except (TypeError, ValueError):
                pass
        if i < len(altitudes) and altitudes[i] is not None:
            try:
                rec.altitude = float(altitudes[i])
            except (TypeError, ValueError):
                pass

        records.append(rec)

    if not records:
        raise MergeError("No records produced from Strava streams")

    # Distance sanity check. Only raises when Strava itself provided GPS truth
    # (a non-empty ``latlng`` stream); otherwise a mismatch against Strava's
    # distance stream is downgraded to a warning, since indoor / non-GPS Edge
    # head units commonly report wheel-sensor distances that diverge from
    # any external GPS source by more than the default 10% tolerance.
    if strava_total_distance > 0:
        rel_err = abs(merged_distance_m - strava_total_distance) / strava_total_distance
        if rel_err > distance_tolerance:
            msg = (
                "Distance mismatch between merged GPS path and Strava distance: "
                f"merged={merged_distance_m:.1f}m strava={strava_total_distance:.1f}m "
                f"(relative error {rel_err:.2%}, tolerance {distance_tolerance:.2%})"
            )
            if strava_pos:
                raise MergeError(msg)
            warnings.append(MergeWarning(
                code="distance_mismatch_soft",
                message=(
                    msg
                    + " — Strava has no GPS to compare against; merge proceeds."
                ),
            ))

    if extrapolated_count > 0:
        warnings.append(
            MergeWarning(
                code="gps_extrapolated",
                message=(
                    f"{extrapolated_count} of {len(records)} samples fell outside the source "
                    "trackpoint time range; nearest-endpoint coordinates were used."
                ),
            )
        )

    # ------------------------------------------------------------------
    # Assemble FIT
    # ------------------------------------------------------------------
    builder = FitFileBuilder(auto_define=True, min_string_size=0)

    # FileId
    file_id = FileIdMessage()
    file_id.type = FileType.ACTIVITY
    file_id.manufacturer = Manufacturer.DEVELOPMENT.value
    file_id.product = 0
    file_id.serial_number = 0  # constant for determinism
    # time_created locked to strava_start_time (no clock reads).
    file_id.time_created = int(round(start_epoch_s * 1000.0))
    builder.add(file_id)

    start_ms = int(round(start_epoch_s * 1000.0))
    last_ms = records[-1].timestamp
    elapsed_s = max(0.0, (last_ms - start_ms) / 1000.0)

    # Timer start event
    ev_start = EventMessage()
    ev_start.event = Event.TIMER
    ev_start.event_type = EventType.START
    ev_start.timestamp = start_ms
    builder.add(ev_start)

    builder.add_all(records)

    # Timer stop event
    ev_stop = EventMessage()
    ev_stop.event = Event.TIMER
    ev_stop.event_type = EventType.STOP_ALL
    ev_stop.timestamp = last_ms
    builder.add(ev_stop)

    # Lap envelope
    lap = LapMessage()
    lap.message_index = 0
    lap.timestamp = last_ms
    lap.start_time = start_ms
    lap.total_elapsed_time = elapsed_s
    lap.total_timer_time = elapsed_s
    lap.total_distance = merged_distance_m
    lap.sport = Sport.CYCLING
    lap.sub_sport = SubSport.GENERIC
    lap.event = Event.LAP
    lap.event_type = EventType.STOP
    builder.add(lap)

    # Session envelope
    session = SessionMessage()
    session.message_index = 0
    session.timestamp = last_ms
    session.start_time = start_ms
    session.total_elapsed_time = elapsed_s
    session.total_timer_time = elapsed_s
    session.total_distance = merged_distance_m
    session.sport = Sport.CYCLING
    session.sub_sport = SubSport.GENERIC
    session.num_laps = 1
    session.event = Event.SESSION
    session.event_type = EventType.STOP
    builder.add(session)

    fit_file = builder.build()
    fit_bytes = fit_file.to_bytes()

    return MergeResult(
        fit_bytes=fit_bytes,
        record_count=len(records),
        warnings=tuple(warnings),
        distance_meters=merged_distance_m,
        gps_source=gps_source,
    )


__all__ = [
    "MergeError",
    "MergeResult",
    "MergeWarning",
    "merge_streams_to_fit",
]
