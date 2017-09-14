from calendar import monthrange
from datetime import datetime
from datetime import timedelta
from datetime import timezone


HOURLY = 'hourly'
DAILY = 'daily'
MONTHLY = 'monthly'
TIME_UNIT_VALUES = [HOURLY, DAILY, MONTHLY]


def convert_iso(time):
    time_dt = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
    return time_dt.replace(tzinfo=timezone.utc).isoformat()

# Breaks the time period into smaller intervals based on the time_unit


def break_time_period(from_time, to_time, time_unit):
    time_periods = []

    # Initialize the first time period
    start_time = parse_time(from_time)
    end_time = get_end_time(start_time, time_unit)

    to_time_parsed = parse_time(to_time)

    first_item = {
        'start_time': start_time, 'end_time': end_time}
    time_periods.append(first_item)
    # Break the time period into successive smaller periods
    while (start_time <= to_time_parsed):
        start_time = end_time + timedelta(seconds=1)
        if start_time > to_time_parsed:
            continue
        end_time = get_end_time(
            start_time, time_unit)
        if end_time > to_time_parsed:
            end_time = to_time_parsed
        item = {
            'start_time': start_time, 'end_time': end_time}
        time_periods.append(item)

    return time_periods

# Returns the end time for time periods
# For example: If date_time = 2017-01-01 22:10:45 and time_unit = HOURLY,
# end_time = 2017-01-01 22:59:59
# Similarly: If date_time = 2017-01-01 22:10:45 and time_unit = DAILY,
# end_time = 2017-01-01 23:59:59
# Also: If date_time = 2017-01-01 22:10:45 and time_unit = MONTHLY,
# end_time = 2017-01-31 23:59:59


def get_end_time(date_time, time_unit):
    if time_unit == HOURLY:
        response = datetime(date_time.year, date_time.month,
                            date_time.day, date_time.hour, 59, 59)
    elif time_unit == DAILY:
        response = datetime(
            date_time.year, date_time.month, date_time.day, 23, 59, 59)
    elif time_unit == MONTHLY:
        last_day = monthrange(date_time.year, date_time.month)[1]
        response = datetime(
            date_time.year, date_time.month, last_day, 23, 59, 59)
    else:  # Time Unit = Threshold value
        response = datetime(
            date_time.year, date_time.month,
            date_time.day, date_time.hour, 59, 59) + timedelta(
                hours=(time_unit - 1))
    return response


def parse_time_with_tz(date_time):
    return datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S+00:00")


def remove_tz(date_time):
    date_time = datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S+00:00")
    return datetime.strftime(date_time, "%Y-%m-%dT%H:%M:%S")

# Parsing value without timezone consideration


def parse_time(date_time):
    return datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S")


def unparse_time(date_time):
    return datetime.strftime(date_time, "%Y-%m-%dT%H:%M:%S")


def current_utc_time():
    return (datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'))


def time_convert(data):
    return datetime.utcfromtimestamp(int(data)).isoformat()


def subtract_seconds(date_time, seconds_count):
    return unparse_time(parse_time(date_time)-timedelta(seconds=seconds_count))
