from pandas import Timestamp, isna, Timedelta, date_range


def chunk_dated_call(lse, start, end, frequency, unit='days'):
    """
    convenience method to chunk up some API call by start and end date

    :param lse: lambda/partial function that takes arguments for start & end; lambda start, end: api(x,y,start,end,z,q,...);
    start and end will both be produced as pandas timestamps
    :param start: start time
    :param end: end time
    :param frequency: interval size / maximum rows per call
    :param unit: interval unit (e.g. days/D, weeks/W, months/M, seconds/sec/S, minutes/min/M)
    :return: generator; yield next call per iteration until all dates (start, end) are exhausted
    """
    start = Timestamp(start)
    end = Timestamp(end)
    if isna([start, end]).any():
        raise ValueError('invalid start/end dates')

    interval = Timedelta(frequency, unit)
    if start == end:
        yield lse(start, end)
        return
    for dt in date_range(start, end, freq=interval):
        yield lse(dt, min(dt + interval, end))

    return


# noinspection DuplicatedCode
def chunk_dates_by_month(start, end, always_output_end=False):
    start = Timestamp(start)
    end = Timestamp(end)
    if isna([start, end]).any():
        raise ValueError('invalid start/end dates')

    if start == end:
        yield start
        return

    dr = date_range(start, end, freq='MS')
    if len(dr) == 0 or start < dr[0]:
        yield start
    for dt in dr:
        yield dt
    # if there is less than month gap between start/end but the end is in a different month, then it doesn't get output by date_range...
    if always_output_end or (len(dr) == 0 and end.month != start.month):
        yield end

    return


# noinspection DuplicatedCode
def chunk_dates_by_year(start, end, always_output_end=False):
    start = Timestamp(start)
    end = Timestamp(end)
    if isna([start, end]).any():
        raise ValueError('invalid start/end dates')

    if start == end:
        yield start
        return

    dr = date_range(start, end, freq='YS')
    if len(dr) == 0 or start < dr[0]:
        yield start
    for dt in dr:
        yield dt
    # if there is less than year gap between start/end but the end is in a different year, then it doesn't get output by date_range...
    if always_output_end or (len(dr) == 0 and end.year != start.year):
        yield end

    return
