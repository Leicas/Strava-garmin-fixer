"""Parse a Garmin FIT file into the stream-dict shape merge_streams_to_fit
expects (the shape Strava's /streams endpoint used to give us).

Pure and deterministic: bytes in, dicts out, no I/O.

fit-tool property accessors return SI units symmetric with what the merge
builder writes: timestamps in unix milliseconds, positions in degrees,
speed m/s, distance m, altitude m, temperature °C.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fit_tool.fit_file import FitFile
from fit_tool.profile.messages.record_message import RecordMessage
from fit_tool.profile.messages.session_message import SessionMessage


class FitParseError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParsedFit:
    streams: dict[str, Any]  # {"time": {"data": [...]}, "latlng": ..., ...}
    start_time: datetime  # tz-aware UTC
    record_count: int
    sport: str | None = None


def parse_fit_streams(fit_bytes: bytes) -> ParsedFit:
    if not fit_bytes:
        raise FitParseError("Empty FIT payload")
    try:
        ff = FitFile.from_bytes(fit_bytes)
    except Exception as exc:  # noqa: BLE001 - fit-tool raises assorted types
        raise FitParseError(f"Invalid FIT file: {exc}") from exc

    records: list[RecordMessage] = []
    session_start_ms: int | None = None
    sport: str | None = None
    for rec in ff.records:
        msg = rec.message
        if isinstance(msg, RecordMessage):
            if msg.timestamp is not None:
                records.append(msg)
        elif isinstance(msg, SessionMessage):
            if session_start_ms is None and msg.start_time is not None:
                session_start_ms = int(msg.start_time)
            if sport is None and msg.sport is not None:
                sport = str(getattr(msg.sport, "name", msg.sport)).lower()

    if not records:
        raise FitParseError("FIT file contains no record messages")

    records.sort(key=lambda r: int(r.timestamp))
    start_ms = session_start_ms if session_start_ms is not None else int(records[0].timestamp)
    # Session start can postdate the first record on some devices; the merge
    # needs offsets from the true first sample.
    start_ms = min(start_ms, int(records[0].timestamp))
    start_time = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)

    times: list[float] = []
    latlng: list[list[float] | None] = []
    distance: list[float | None] = []
    speed: list[float | None] = []
    cadence: list[float | None] = []
    temp: list[float | None] = []
    altitude: list[float | None] = []
    heartrate: list[float | None] = []

    for r in records:
        times.append(round((int(r.timestamp) - start_ms) / 1000.0, 3))
        lat, lon = r.position_lat, r.position_long
        latlng.append([float(lat), float(lon)] if lat is not None and lon is not None else None)
        distance.append(float(r.distance) if r.distance is not None else None)
        speed.append(float(r.speed) if r.speed is not None else None)
        cadence.append(float(r.cadence) if r.cadence is not None else None)
        temp.append(float(r.temperature) if r.temperature is not None else None)
        # enhanced_altitude wins when plain altitude is absent (common on newer units)
        alt = r.altitude if r.altitude is not None else getattr(r, "enhanced_altitude", None)
        altitude.append(float(alt) if alt is not None else None)
        heartrate.append(float(r.heart_rate) if r.heart_rate is not None else None)

    def _wrap(data: list) -> dict[str, Any]:
        return {"data": data}

    streams = {
        "time": _wrap(times),
        "latlng": _wrap(latlng),
        "distance": _wrap(distance),
        "velocity_smooth": _wrap(speed),
        "cadence": _wrap(cadence),
        "temp": _wrap(temp),
        "altitude": _wrap(altitude),
        "heartrate": _wrap(heartrate),
    }
    return ParsedFit(
        streams=streams,
        start_time=start_time,
        record_count=len(records),
        sport=sport,
    )
