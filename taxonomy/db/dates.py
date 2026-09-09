"""Validate source-calendar dates and derive Gregorian publication dates.

Partial source dates are resolved to their last possible day before conversion.
Only the supported calendars are accepted; adding an enum member alone must not
silently cause dates in another calendar to be interpreted as Gregorian.
"""

import datetime
import re

from taxonomy.db.constants import Calendar

_DATE = re.compile(r"([0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?")


def last_day(date: str, calendar: Calendar) -> tuple[int, int, int]:
    """Resolve YYYY, YYYY-MM or YYYY-MM-DD in the specified calendar.

    Raise ValueError for unsupported calendars, invalid dates, and date ranges.
    """
    if calendar not in (Calendar.gregorian, Calendar.julian):
        raise ValueError(f"Unsupported calendar: {calendar!r}")
    match = _DATE.fullmatch(date)
    if match is None:
        raise ValueError(f"Invalid date: {date!r}")
    year = int(match[1])
    month = int(match[2]) if match[2] is not None else 12
    if not 1 <= year <= 9999 or not 1 <= month <= 12:
        raise ValueError(f"Invalid date: {date!r}")
    leap = year % 4 == 0 and (
        calendar is Calendar.julian or year % 100 != 0 or year % 400 == 0
    )
    month_days = (31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    day = int(match[3]) if match[3] is not None else month_days[month - 1]
    if not 1 <= day <= month_days[month - 1]:
        raise ValueError(f"Invalid date: {date!r} ({calendar.name})")
    return year, month, day


def to_gregorian(date: str, calendar: Calendar = Calendar.gregorian) -> str:
    """Return a Gregorian date, preserving precision for Gregorian input.

    Julian input returns an exact adopted day. Its original precision remains on
    the source record. Conversion uses Julian day numbers, including century and
    leap-day differences, rather than treating the source as a Gregorian date.
    """
    year, month, day = last_day(date, calendar)
    if calendar is Calendar.gregorian:
        return date
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3
    julian_day = day + (153 * m + 2) // 5 + 365 * y + y // 4 - 32083
    # Gregorian 0001-01-01 is Julian day 1721426 and datetime ordinal 1.
    return datetime.date.fromordinal(julian_day - 1721425).isoformat()
