from datetime import datetime, timedelta

from django.conf import settings
from django.utils import dateparse, timezone


def get_current_year():
    return timezone.now().isocalendar()[0]


def get_current_month():
    return timezone.now().month


def get_current_week():
    return timezone.now().isocalendar()[1]


def get_current_weekday():
    return timezone.now().isocalendar()[2]


def get_year_from_date(date):
    return date.isocalendar()[0]


def get_week_from_date(date):
    return date.isocalendar()[1]


def get_weekday_from_date(date):
    return date.isocalendar()[2]


def get_month_from_date(date):
    return date.month


def all_days_of_a_month(year, month, day_name=None):
    days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    first_date_of_month = get_first_date_of_month(year, month)
    last_date_of_month = get_last_date_of_month(year, month)

    all_date_of_month = date_range_list(first_date_of_month, last_date_of_month)

    date_weekday_mapping = {}
    for date in all_date_of_month:
        weekday_name = days_of_week[date.isocalendar()[2] - 1]
        if day_name:
            if day_name == weekday_name:
                date_weekday_mapping[date] = weekday_name
        else:
            date_weekday_mapping[date] = weekday_name
    return date_weekday_mapping


def get_first_date_of_month(year, month):
    """Return the first date of the month.

    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January

    Returns:
        date (datetime): First date of the current month
    """
    return datetime(year, month, 1).date()


def get_last_date_of_month(year, month):
    """Return the last date of the month.

    Args:
        year (int): Year, i.e. 2022
        month (int): Month, i.e. 1 for January

    Returns:
        date (datetime): Last date of the current month
    """

    if month == 12:
        last_date = datetime(year, month, 31)
    else:
        last_date = datetime(year, month + 1, 1) + timedelta(days=-1)

    return last_date.date()


def get_first_date_of_week(year, week_number):
    """Return the first date(monday) of a week by week number.

    Args:
        year (int): Year, i.e. 2022
        week_number (int): Week, i.e. 45

    Returns:
        date (datetime): First date of the given week for given year
    """
    d = '{0}-W{1}'.format(year, week_number)
    monday_date = datetime.strptime(d + '-1', "%Y-W%W-%w")
    return monday_date.date()


def date_range_list(start_date, end_date):
    # Return generator for a list datetime.date objects (inclusive) between start_date and end_date (inclusive).
    curr_date = start_date
    while curr_date <= end_date:
        yield curr_date
        curr_date = curr_date + timedelta(days=1)


def is_date(value):
    """
    is_date
        Method to check if the value is valid date string.
    :param value:
    :return match object or None:
    """
    match = dateparse.date_re.match(str(value))
    return True if match else False


def is_datetime(value):
    """
    is_datetime
        Method to check if the value is valid datetime string
    :param value:
    :return match object or None:
    """
    match = dateparse.datetime_re.match(str(value))
    return True if match else False


def to_date(value, default=None):
    """
    to_date
        This function returns a string representation of a datetime object, for the date part only
    :param value: Str
    :param default:
    :return date or None:
    """
    try:
        date_object = datetime.strptime(str(value), settings.DATE_FORMAT)
        if date_object:
            return date_object
    except Exception:
        pass
    return default


def to_datetime(value, default=None):
    """
    to_datetime
        This function returns a string representation of a datetime object
    :param value: Str
    :param default:
    :return date or None:
    """
    try:
        datetime_object = datetime.strptime(str(value), settings.DATETIME_FORMAT)
        if datetime_object:
            return datetime_object
    except Exception:
        pass
    return default


def format_date(value, frmt=settings.DATE_FORMAT, default=None):
    """
    to_datetime
        This function returns a string representation of a datetime object
    :param value: Date object
    :param frmt:
    :param default:
    :return date or None:
    """
    try:
        return str(value.strftime(frmt))
    except Exception:
        pass
    return default


def format_datetime(value, frmt=settings.DATETIME_FORMAT, default=None):
    """
    format_date
        Method to covert datetime value in str to date object
    :param value: Datetime object
    :param frmt:
    :param default:
    :return date or None:
    """
    try:
        return str(value.strftime(frmt))
    except Exception:
        pass
    return default

