import pytest
import pandas as pd
from scitrera_rt_data.dt import (
    parse_dt, localize, naive_time,
    dt_to_unix_ns, dt_to_unix_ms, dt_to_unix_s,
    dt_from_unix_ns, dt_from_unix_ms, dt_from_unix_s,
    UTC, US_EASTERN, US_CENTRAL
)

def test_parse_dt_basic():
    # Test string parsing (naive assumes UTC by default)
    ts = parse_dt("2023-01-01 12:00:00")
    assert ts == pd.Timestamp("2023-01-01 12:00:00", tz=UTC)
    
    # Test with explicit timezone
    ts = parse_dt("2023-01-01 12:00:00", tz=US_EASTERN)
    assert ts == pd.Timestamp("2023-01-01 12:00:00", tz=US_EASTERN)

def test_parse_dt_aware_string():
    # Test aware string conversion to target TZ
    ts = parse_dt("2023-01-01 12:00:00+05:00", tz=UTC)
    assert ts == pd.Timestamp("2023-01-01 07:00:00", tz=UTC)
    
    ts = parse_dt("2023-01-01 12:00:00+05:00", tz=US_CENTRAL)
    expected = pd.Timestamp("2023-01-01 07:00:00", tz=UTC).tz_convert(US_CENTRAL)
    assert ts == expected

def test_parse_dt_pandas_objects():
    # DatetimeIndex
    idx = pd.DatetimeIndex(["2023-01-01", "2023-01-02"])
    parsed = parse_dt(idx)
    assert isinstance(parsed, pd.DatetimeIndex)
    assert parsed[0] == pd.Timestamp("2023-01-01", tz=UTC)
    
    # Series
    s = pd.Series(["2023-01-01", "2023-01-02"])
    parsed_s = parse_dt(s)
    assert isinstance(parsed_s, pd.DatetimeIndex)
    assert parsed_s[1] == pd.Timestamp("2023-01-02", tz=UTC)

def test_localize_and_naive():
    ts = pd.Timestamp("2023-01-01 12:00:00", tz=UTC)
    
    # naive_time
    nt = naive_time(ts)
    assert nt.tzinfo is None
    assert nt == pd.Timestamp("2023-01-01 12:00:00")
    
    # localize (to local tz)
    lt = localize(ts)
    assert lt.tzinfo is not None
    # Represents the same point in time
    assert abs(lt.timestamp() - ts.timestamp()) < 1e-6

def test_unix_conversions():
    ts = pd.Timestamp("2023-01-01 00:00:00", tz=UTC)
    val_ns = ts.value # 1672531200000000000
    
    assert dt_to_unix_ns(ts) == val_ns
    assert dt_to_unix_ms(ts) == val_ns // 1_000_000
    assert dt_to_unix_s(ts) == val_ns // 1_000_000_000
    
    assert dt_from_unix_ns(val_ns) == ts
    assert dt_from_unix_ms(val_ns // 1_000_000) == ts
    assert dt_from_unix_s(val_ns // 1_000_000_000) == ts

def test_dt_from_unix_local():
    ts_ns = 1672531200000000000 # 2023-01-01 00:00:00 UTC
    ts = dt_from_unix_ns(ts_ns, utc=False)
    # New behavior: always return UTC, but interpret input as local
    assert ts.tzinfo is not None
    assert ts.tzname() == "UTC"
