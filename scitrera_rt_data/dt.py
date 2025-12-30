from __future__ import annotations

from typing import Any

from datetime import tzinfo
from pytz import timezone, UTC
from dateutil.parser import parse as _parse_date
from dateutil.tz import tzlocal
from pandas import Timestamp, Timedelta, DatetimeIndex, Series, Index, date_range

# Pandas now/utcnow stub
now = Timestamp.utcnow

# Timezones
US_EASTERN = timezone('US/Eastern')
US_CENTRAL = timezone('US/Central')
_tz_local = tzlocal()

NANO = 1e-9
MICRO = 1e-6
MILLI = 1e-3


# ~~~ stockpyl utils legacy / convenience functions ~~~

def parse_dt(time_str: Any, tz: str | tzinfo = 'utc', local_tz_in: bool = False) -> Timestamp | DatetimeIndex:
    """
    Parse a date/time string using dateutil.parser.parse and return a Pandas timestamp. The resulting timestamp will
    be in the given timezone. If a naive time_str is provided, it will be interpreted as if it were tz. If a tz-aware
    time_str is provided, the timezone will be properly converted to tz.

    :param time_str: date/time string to interpret
    :param tz: timezone string, default='utc'. If given time_str has tz info then convert to tz. If not, assign tz as interpreted timezone.
    :param local_tz_in: boolean to indicate that a provided naive input was from the local timezone, so we'll import then convert to tz.
    :return: parsed dt timestamp
    """
    if isinstance(time_str, str):
        date = _parse_date(time_str)
    # special handling for DatetimeIndex objects for convenience
    elif isinstance(time_str, (DatetimeIndex, Index, Series)):
        return DatetimeIndex(parse_dt(dt, tz) for dt in time_str)
    else:
        date = time_str

    if local_tz_in and hasattr(date, 'tzinfo'):
        date = Timestamp(date, tz=_tz_local)

    return Timestamp(date).tz_convert(tz) if getattr(date, 'tzinfo', None) else Timestamp(date, tz=tz)


def localize(dt) -> Timestamp:
    """
    Convert a date/time string / datetime / Timestamp to local timezone

    :param dt: timestamp or similar that will be converted to local timezone
    """
    if not isinstance(dt, Timestamp):
        dt = parse_dt(dt)
    return dt.tz_convert(_tz_local)


def naive_time(dt) -> Timestamp:
    """
    Convert a date/time string / datetime / Timestamp to naive timestamp

    :param dt: timestamp with TZ info set to None
    :return: naive timestamp
    """
    if not isinstance(dt, Timestamp):
        dt = parse_dt(dt)
    return dt.replace(tzinfo=None)


def dt_to_unix_ns(dt) -> int:
    """
    Convert a time (preferably a Timestamp instance) to ns since the unix epoch.

    :param dt:
    :return: integer time in ns
    """
    if not isinstance(dt, Timestamp):
        dt = parse_dt(dt)
    return int(dt.value)


def dt_to_unix_ms(dt) -> int:
    """
    Convert a time (preferably a Timestamp instance) to ms since the unix epoch.

    :param dt:
    :return: integer time in ms
    """
    if not isinstance(dt, Timestamp):
        dt = parse_dt(dt)
    return int(dt.value * MICRO)  # MICRO bridges ns to ms


def dt_to_unix_s(dt) -> int:
    """
    Convert a time (preferably a Timestamp instance) to s since the unix epoch.

    :param dt:
    :return: integer time in s
    """
    if not isinstance(dt, Timestamp):
        dt = parse_dt(dt)
    return int(dt.value * NANO)  # NANO bridges ns to s


def dt_from_unix_ns(ns, utc=True) -> Timestamp:
    """
    Convert a time (time in ns since unix epoch, UTC) to a Timestamp

    :param ns: Time (ns) since unix epoch
    :type ns: int
    :param utc: whether the incoming timestamp is from UTC, default == True
    :type utc: bool
    :return: Timestamp
    """
    if utc:
        return Timestamp(ns, unit='ns', tz=UTC)
    return Timestamp(ns, unit='ns').tz_localize(_tz_local).tz_convert(UTC)


def dt_from_unix_ms(ms, utc=True) -> Timestamp:
    """
    Convert a time (time in ms since unix epoch, UTC) to a Timestamp

    :param ms: Time (ms) since unix epoch
    :type ms: int
    :param utc: whether the incoming timestamp is from UTC, default == True
    :type utc: bool
    :return: Timestamp
    """
    if utc:
        return Timestamp(ms, unit='ms', tz=UTC)
    return Timestamp(ms, unit='ms').tz_localize(_tz_local).tz_convert(UTC)


def dt_from_unix_s(s, utc=True) -> Timestamp:
    """
    Convert a time (time in ms since unix epoch, UTC) to a Timestamp

    :param s: Time (s) since unix epoch
    :type s: int
    :param utc: whether the incoming timestamp is from UTC, default == True
    :type utc: bool
    :return: Timestamp
    """
    if utc:
        return Timestamp(s, unit='s', tz=UTC)
    return Timestamp(s, unit='s').tz_localize(_tz_local).tz_convert(UTC)


__all__ = (
    'date_range', 'parse_dt', 'dt_from_unix_ms', 'dt_from_unix_ns', 'dt_to_unix_s', 'dt_to_unix_ms',
    'localize', 'now', 'Timestamp', 'Timedelta', 'dt_from_unix_s', 'dt_to_unix_ns',
    'naive_time', 'DatetimeIndex',
    # Time zones
    'UTC', 'US_EASTERN', 'US_CENTRAL',
    # simpler conversions
    'NANO', 'MICRO', 'MILLI',
)
