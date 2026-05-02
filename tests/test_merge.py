"""Tests for ``app.merge.merge_streams_to_fit``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.merge import (
    MergeError,
    MergeResult,
    MergeWarning,
    merge_streams_to_fit,
)


# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------


def _make_fixtures(
    *,
    n_seconds: int = 60,
    tcx_step_s: int = 5,
    drop_first_tcx: int = 0,
    tcx_circle_at_origin: bool = False,
    omit_temp: bool = False,
) -> tuple[dict, bytes, datetime]:
    """Generate deterministic Strava-streams + TCX bytes + start_time fixtures.

    Defaults: 60 one-second samples, a roughly-500m straight-NE path starting at
    (45.0, -73.5), HR 130->145, distance 0->500m, speed 8.33 m/s, cadence 85,
    temp 18.5, altitude 100->105.

    The lat/lon deltas are sized so the haversine length of the path is ~500m
    (matches the Strava ``distance`` stream). Pure NE direction: ``dlat`` and
    ``dlon`` chosen so each contributes ~half the distance at 45 degN.
    """
    start_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

    times = list(range(n_seconds))
    # Net path: ~353m north, ~353m east  ->  hypotenuse ~= 500m.
    # 1 deg lat ~= 111_320 m;  1 deg lon at 45 degN ~= 78_715 m.
    dlat_total = 353.0 / 111_320.0  # ~ 0.003171
    dlon_total = 353.0 / 78_715.0   # ~ 0.004485
    lats = [45.0 + dlat_total * (i / (n_seconds - 1)) for i in range(n_seconds)]
    lons = [-73.5 + dlon_total * (i / (n_seconds - 1)) for i in range(n_seconds)]
    distances = [500.0 * (i / (n_seconds - 1)) for i in range(n_seconds)]
    speeds = [8.33] * n_seconds
    cadences = [85] * n_seconds
    temps = [18.5] * n_seconds
    altitudes = [100.0 + 5.0 * (i / (n_seconds - 1)) for i in range(n_seconds)]

    streams: dict = {
        "time": {"data": times},
        "latlng": {"data": [[lats[i], lons[i]] for i in range(n_seconds)]},
        "distance": {"data": distances},
        "velocity_smooth": {"data": speeds},
        "cadence": {"data": cadences},
        "altitude": {"data": altitudes},
    }
    if not omit_temp:
        streams["temp"] = {"data": temps}

    # Build TCX with a trackpoint every ``tcx_step_s`` seconds. Include the
    # last sample index so the merge's time axis is fully covered (otherwise
    # the trailing samples would extrapolate).
    tcx_indices = list(range(0, n_seconds, tcx_step_s))
    if (n_seconds - 1) not in tcx_indices:
        tcx_indices.append(n_seconds - 1)
    if drop_first_tcx > 0:
        tcx_indices = tcx_indices[drop_first_tcx:]

    tp_xml_chunks: list[str] = []
    for idx in tcx_indices:
        ts = (start_time + timedelta(seconds=idx)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if tcx_circle_at_origin:
            # Tight little path near (0, 0): wildly wrong vs Strava distance.
            import math

            angle = 2.0 * math.pi * (idx / n_seconds)
            lat = 0.0 + 0.00001 * math.cos(angle)
            lon = 0.0 + 0.00001 * math.sin(angle)
        else:
            lat = lats[idx]
            lon = lons[idx]
        hr = int(130 + 15 * (idx / (n_seconds - 1)))
        tp_xml_chunks.append(
            f"""        <Trackpoint>
          <Time>{ts}</Time>
          <Position>
            <LatitudeDegrees>{lat:.7f}</LatitudeDegrees>
            <LongitudeDegrees>{lon:.7f}</LongitudeDegrees>
          </Position>
          <HeartRateBpm><Value>{hr}</Value></HeartRateBpm>
        </Trackpoint>"""
        )

    tcx_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<TrainingCenterDatabase '
        'xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">\n'
        '  <Activities>\n'
        '    <Activity Sport="Biking">\n'
        '      <Lap>\n'
        '        <Track>\n'
        + "\n".join(tp_xml_chunks)
        + "\n        </Track>\n"
        '      </Lap>\n'
        '    </Activity>\n'
        '  </Activities>\n'
        '</TrainingCenterDatabase>\n'
    )
    return streams, tcx_xml.encode("utf-8"), start_time


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_merge_happy_path() -> None:
    streams, tcx_bytes, start = _make_fixtures()

    result = merge_streams_to_fit(
        strava_streams=streams,
        strava_start_time=start,
        fitbit_tcx=tcx_bytes,
    )

    assert isinstance(result, MergeResult)
    assert isinstance(result.fit_bytes, bytes)
    assert len(result.fit_bytes) > 0
    # FIT files start with header byte indicating header size (12 or 14)
    assert result.fit_bytes[0] in (12, 14)
    assert result.record_count == 60
    assert result.warnings == ()
    # Distance should be within ~1% of 500m using haversine over the synthetic path.
    assert abs(result.distance_meters - 500.0) / 500.0 < 0.01


def test_merge_is_deterministic() -> None:
    streams, tcx_bytes, start = _make_fixtures()

    a = merge_streams_to_fit(
        strava_streams=streams,
        strava_start_time=start,
        fitbit_tcx=tcx_bytes,
    )
    b = merge_streams_to_fit(
        strava_streams=streams,
        strava_start_time=start,
        fitbit_tcx=tcx_bytes,
    )

    assert a.fit_bytes == b.fit_bytes
    assert a.record_count == b.record_count
    assert a.distance_meters == b.distance_meters


def test_merge_distance_mismatch_raises() -> None:
    streams, tcx_bytes, start = _make_fixtures(tcx_circle_at_origin=True)

    with pytest.raises(MergeError) as excinfo:
        merge_streams_to_fit(
            strava_streams=streams,
            strava_start_time=start,
            fitbit_tcx=tcx_bytes,
        )

    msg = str(excinfo.value)
    # Both numbers must appear in the message.
    assert "500" in msg  # strava distance (500m)
    # Merged distance for a tiny ~1m circle near (0,0) should be very small;
    # the exact integer may differ but "merged=" prefix is present.
    assert "merged=" in msg
    assert "strava=" in msg


def test_merge_handles_missing_fitbit_segment() -> None:
    # Drop the first 2 TCX trackpoints (covers the first 10 seconds).
    streams, tcx_bytes, start = _make_fixtures(drop_first_tcx=2)

    # Loose tolerance: the extrapolated head-of-track samples reuse the first
    # remaining trackpoint, which slightly shrinks the merged path length.
    result = merge_streams_to_fit(
        strava_streams=streams,
        strava_start_time=start,
        fitbit_tcx=tcx_bytes,
        distance_tolerance=0.5,
    )

    assert result.record_count == 60
    codes = [w.code for w in result.warnings]
    assert "gps_extrapolated" in codes


def test_merge_handles_missing_streams() -> None:
    streams, tcx_bytes, start = _make_fixtures(omit_temp=True)
    assert "temp" not in streams

    result = merge_streams_to_fit(
        strava_streams=streams,
        strava_start_time=start,
        fitbit_tcx=tcx_bytes,
    )

    assert result.record_count == 60
    assert isinstance(result.fit_bytes, bytes) and len(result.fit_bytes) > 0
