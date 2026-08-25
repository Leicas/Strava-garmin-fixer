"""Roundtrip tests for app.garmin.fitparse: a FIT produced by our own merge
builder must parse back into the same stream shape the merge consumes."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.garmin.fitparse import FitParseError, parse_fit_streams
from app.merge import merge_streams_to_fit

_TCX = b"""<?xml version="1.0"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
<Activities><Activity Sport="Biking"><Lap>
<Track>
<Trackpoint><Time>2026-08-20T10:00:00Z</Time><Position><LatitudeDegrees>45.5</LatitudeDegrees><LongitudeDegrees>-73.6</LongitudeDegrees></Position><HeartRateBpm><Value>120</Value></HeartRateBpm></Trackpoint>
<Trackpoint><Time>2026-08-20T10:00:10Z</Time><Position><LatitudeDegrees>45.5010</LatitudeDegrees><LongitudeDegrees>-73.6</LongitudeDegrees></Position><HeartRateBpm><Value>125</Value></HeartRateBpm></Trackpoint>
<Trackpoint><Time>2026-08-20T10:00:20Z</Time><Position><LatitudeDegrees>45.5020</LatitudeDegrees><LongitudeDegrees>-73.6</LongitudeDegrees></Position><HeartRateBpm><Value>130</Value></HeartRateBpm></Trackpoint>
</Track></Lap></Activity></Activities></TrainingCenterDatabase>
"""

_START = datetime(2026, 8, 20, 10, 0, 0, tzinfo=timezone.utc)

_STREAMS = {
    "time": {"data": [0, 10, 20]},
    "distance": {"data": [0.0, 111.2, 222.4]},
    "velocity_smooth": {"data": [0.0, 11.12, 11.12]},
    "cadence": {"data": [80, 82, 84]},
    "temp": {"data": [21, 21, 22]},
    "altitude": {"data": [30.0, 31.0, 32.0]},
}


def _build_fit() -> bytes:
    result = merge_streams_to_fit(
        strava_streams=_STREAMS,
        strava_start_time=_START,
        source_tcx=_TCX,
        activity_name="roundtrip",
    )
    return result.fit_bytes


def test_roundtrip_shape_and_units():
    parsed = parse_fit_streams(_build_fit())

    assert parsed.start_time == _START
    assert parsed.record_count == 3
    assert parsed.streams["time"]["data"] == [0.0, 10.0, 20.0]

    cad = parsed.streams["cadence"]["data"]
    assert cad == [80.0, 82.0, 84.0]
    assert parsed.streams["temp"]["data"] == [21.0, 21.0, 22.0]
    assert parsed.streams["altitude"]["data"] == pytest.approx([30.0, 31.0, 32.0], abs=0.5)
    assert parsed.streams["velocity_smooth"]["data"] == pytest.approx(
        [0.0, 11.12, 11.12], abs=0.01
    )
    assert parsed.streams["heartrate"]["data"] == [120.0, 125.0, 130.0]

    latlng = parsed.streams["latlng"]["data"]
    assert latlng[0] == pytest.approx([45.5, -73.6], abs=1e-5)
    assert latlng[2] == pytest.approx([45.5020, -73.6], abs=1e-5)


def test_roundtrip_feeds_merge_again():
    """The parsed streams must be directly consumable by merge_streams_to_fit —
    this is exactly what the Garmin worker does."""
    parsed = parse_fit_streams(_build_fit())
    result = merge_streams_to_fit(
        strava_streams=parsed.streams,
        strava_start_time=parsed.start_time,
        source_tcx=_TCX,
        activity_name="second pass",
    )
    assert result.record_count == 3
    assert result.fit_bytes


def test_garbage_bytes_raise():
    with pytest.raises(FitParseError):
        parse_fit_streams(b"not a fit file at all")
    with pytest.raises(FitParseError):
        parse_fit_streams(b"")
