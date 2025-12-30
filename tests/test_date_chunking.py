import pytest
import pandas as pd
from scitrera_rt_data.remote_rest.date_chunking import (
    chunk_dated_call,
    chunk_dates_by_month,
    chunk_dates_by_year
)

def test_chunk_dated_call():
    def dummy_api(start, end):
        return f"{start.date()} to {end.date()}"

    # Test single chunk (start == end)
    start = "2023-01-01"
    end = "2023-01-01"
    results = list(chunk_dated_call(dummy_api, start, end, 1, unit='D'))
    assert results == ["2023-01-01 to 2023-01-01"]

    # Test daily chunks
    start = "2023-01-01"
    end = "2023-01-03"
    results = list(chunk_dated_call(dummy_api, start, end, 1, unit='D'))
    assert results == [
        "2023-01-01 to 2023-01-02",
        "2023-01-02 to 2023-01-03",
        "2023-01-03 to 2023-01-03"
    ]

    # Test weekly chunks
    start = "2023-01-01"
    end = "2023-01-15"
    results = list(chunk_dated_call(dummy_api, start, end, 1, unit='W'))
    assert results == [
        "2023-01-01 to 2023-01-08",
        "2023-01-08 to 2023-01-15",
        "2023-01-15 to 2023-01-15"
    ]

    # Test invalid dates
    with pytest.raises(ValueError, match="invalid start/end dates"):
        list(chunk_dated_call(dummy_api, None, "2023-01-01", 1))

def test_chunk_dates_by_month():
    # Test same month
    start = "2023-01-01"
    end = "2023-01-15"
    results = list(chunk_dates_by_month(start, end))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-01"]

    # Test multiple months
    start = "2023-01-15"
    end = "2023-03-05"
    results = list(chunk_dates_by_month(start, end))
    # MS = Month Start.
    # 2023-01-15
    # 2023-02-01 (Month Start)
    # 2023-03-01 (Month Start)
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-15", "2023-02-01", "2023-03-01"]

    # Test always_output_end
    results = list(chunk_dates_by_month(start, end, always_output_end=True))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-15", "2023-02-01", "2023-03-01", "2023-03-05"]

    # Test start == end
    results = list(chunk_dates_by_month(start, start))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-15"]

    # Test different month but less than a month gap
    start = "2023-01-30"
    end = "2023-02-01"
    results = list(chunk_dates_by_month(start, end))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-30", "2023-02-01"]

def test_chunk_dates_by_year():
    # Test same year
    start = "2023-01-01"
    end = "2023-06-15"
    results = list(chunk_dates_by_year(start, end))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2023-01-01"]

    # Test multiple years
    start = "2022-11-15"
    end = "2024-03-05"
    results = list(chunk_dates_by_year(start, end))
    # YS = Year Start.
    # 2022-11-15
    # 2023-01-01
    # 2024-01-01
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2022-11-15", "2023-01-01", "2024-01-01"]

    # Test always_output_end
    results = list(chunk_dates_by_year(start, end, always_output_end=True))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2022-11-15", "2023-01-01", "2024-01-01", "2024-03-05"]

    # Test start == end
    results = list(chunk_dates_by_year(start, start))
    assert [r.strftime("%Y-%m-%d") for r in results] == ["2022-11-15"]
