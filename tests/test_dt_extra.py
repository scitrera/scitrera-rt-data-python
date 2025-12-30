import pytest
import pandas as pd
from scitrera_rt_data.dt import (
    parse_dt, localize, naive_time,
    dt_to_unix_ns, dt_to_unix_ms, dt_to_unix_s,
    dt_from_unix_ns, dt_from_unix_ms, dt_from_unix_s,
    UTC
)


def test_parse_dt():
    # Parse string
    ts = parse_dt("2023-01-01 12:00:00")
    assert ts == pd.Timestamp("2023-01-01 12:00:00", tz="UTC")

    # Parse with specific TZ
    ts = parse_dt("2023-01-01 12:00:00", tz="US/Eastern")
    assert ts == pd.Timestamp("2023-01-01 12:00:00", tz="US/Eastern")

    # Parse DatetimeIndex
    idx = pd.date_range("2023-01-01", periods=3, freq="D")
    parsed_idx = parse_dt(idx)
    assert len(parsed_idx) == 3
    assert all(ts.tzname() == "UTC" for ts in parsed_idx)


def test_localize_and_naive():
    ts = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
    local_ts = localize(ts)
    assert local_ts.tzinfo is not None

    naive_ts = naive_time(ts)
    assert naive_ts.tzinfo is None
    assert naive_ts.hour == 12


def test_unix_conversions():
    ts = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
    # 2023-01-01 00:00:00 UTC is 1672531200 seconds
    expected_s = 1672531200

    assert dt_to_unix_s(ts) == expected_s
    assert dt_to_unix_ms(ts) == expected_s * 1000
    assert dt_to_unix_ns(ts) == expected_s * 1_000_000_000

    assert dt_from_unix_s(expected_s) == ts
    assert dt_from_unix_ms(expected_s * 1000) == ts
    assert dt_from_unix_ns(expected_s * 1_000_000_000) == ts

def test_unix_conversions_non_utc():
    expected_s = 1672531200
    ts_local = dt_from_unix_s(expected_s, utc=False)
    # The note says it should always return UTC time
    assert ts_local.tzname() == "UTC"
    # If the local timezone is not UTC, the actual time will be different from the expected_s as UTC
    # 1672531200 as LOCAL should be different from 1672531200 as UTC if local tz != UTC
    ts_utc = dt_from_unix_s(expected_s, utc=True)
    import dateutil.tz
    if dateutil.tz.tzlocal() != dateutil.tz.tzutc():
        assert ts_local != ts_utc
