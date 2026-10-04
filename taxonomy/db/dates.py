"""Validate source-calendar dates, publication dates, and collection dates.

Partial publication dates are resolved to their last possible day before conversion.
Collection dates retain intervals and uncertainty for chronological comparisons.
Only the supported calendars are accepted; adding an enum member alone must not
silently cause dates in another calendar to be interpreted as Gregorian.
"""

import datetime
import re
from dataclasses import dataclass

from taxonomy.db.constants import Calendar

_DATE = re.compile(r"([0-9]{4})(?:-([0-9]{2})(?:-([0-9]{2}))?)?")
_REPUBLICAN_EPOCH = datetime.date(1792, 9, 22)
_REPUBLICAN_LEAP_YEARS = (3, 7, 11)
REPUBLICAN_MONTHS = (
    "Vendémiaire",
    "Brumaire",
    "Frimaire",
    "Nivôse",
    "Pluviôse",
    "Ventôse",
    "Germinal",
    "Floréal",
    "Prairial",
    "Messidor",
    "Thermidor",
    "Fructidor",
    "Complémentaires",
)
_REPUBLICAN_DATE = re.compile(
    r"([1-9][0-9]*)(?:-("
    + "|".join(name.casefold() for name in REPUBLICAN_MONTHS)
    + r")(?:-([1-9][0-9]*))?)?"
)


def last_day(date: str, calendar: Calendar) -> tuple[int, int, int]:
    """Resolve a source date to its last possible year, month, and day.

    Republican dates use year[-Month[-day]], e.g. 12-Brumaire-1.
    Gregorian and Julian dates use YYYY[-MM[-DD]].
    Raise ValueError for unsupported calendars, invalid dates, and date ranges.
    """
    if calendar not in (
        Calendar.gregorian,
        Calendar.julian,
        Calendar.french_republican,
    ):
        raise ValueError(f"Unsupported calendar: {calendar!r}")
    pattern = _REPUBLICAN_DATE if calendar is Calendar.french_republican else _DATE
    match = pattern.fullmatch(
        date.casefold() if calendar is Calendar.french_republican else date
    )
    if match is None:
        raise ValueError(f"Invalid date: {date!r}")
    year = int(match[1])
    if calendar is Calendar.french_republican:
        # Historical concordance only: no speculative Romme/equinox extension.
        # IMCCE: https://promenade.imcce.fr/fr/pages2/277.html
        if not 1 <= year <= 14:
            raise ValueError(
                f"Invalid date: {date!r}; French Republican years I–XIV supported"
            )
        month = (
            next(
                i
                for i, name in enumerate(REPUBLICAN_MONTHS, 1)
                if name.casefold() == match[2].casefold()
            )
            if match[2] is not None
            else 13
        )
        maximum = 30 if month <= 12 else 5 + (year in _REPUBLICAN_LEAP_YEARS)
        day = int(match[3]) if match[3] is not None else maximum
        if not 1 <= day <= maximum:
            raise ValueError(f"Invalid date: {date!r} (french_republican)")
        return year, month, day
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

    Non-Gregorian input returns an exact adopted day. Its original precision
    remains on the source record. Julian conversion uses Julian day numbers,
    including century and leap-day differences. Republican conversion uses the
    historical epoch and leap years.
    """
    year, month, day = last_day(date, calendar)
    if calendar is Calendar.gregorian:
        return date
    return _to_gregorian(year, month, day, calendar)


def _to_gregorian(year: int, month: int, day: int, calendar: Calendar) -> str:
    """Convert already validated calendar components to an exact Gregorian day."""
    if calendar is Calendar.gregorian:
        return datetime.date(year, month, day).isoformat()
    if calendar is Calendar.french_republican:
        elapsed = (
            365 * (year - 1)
            + sum(leap_year < year for leap_year in _REPUBLICAN_LEAP_YEARS)
            + 30 * (month - 1)
            + day
            - 1
        )
        return (_REPUBLICAN_EPOCH + datetime.timedelta(days=elapsed)).isoformat()
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3
    julian_day = day + (153 * m + 2) // 5 + 365 * y + y // 4 - 32083
    # Gregorian 0001-01-01 is Julian day 1721426 and datetime ordinal 1.
    return datetime.date.fromordinal(julian_day - 1721425).isoformat()


def gregorian_bounds(
    date: str, calendar: Calendar = Calendar.gregorian
) -> tuple[str, str]:
    """Return exact earliest/latest Gregorian days permitted by a source date."""
    year, month, day = last_day(date, calendar)
    parts = date.split("-")
    first_month = month if len(parts) >= 2 else 1
    first_day = day if len(parts) == 3 else 1
    return (
        _to_gregorian(year, first_month, first_day, calendar),
        _to_gregorian(year, month, day, calendar),
    )


@dataclass(frozen=True)
class CollectionDate:
    # Alternatives remain separate intervals: 1900/1902 does not include 1901.
    intervals: tuple[tuple[str | None, str], ...]
    uncertain: bool = False

    @property
    def earliest(self) -> datetime.date | None:
        if self.uncertain or any(start is None for start, _ in self.intervals):
            return None
        return datetime.date.fromisoformat(
            min(start for start, _ in self.intervals if start is not None)
        )

    def overlaps(self, other: "CollectionDate") -> bool:
        # Approximation has no specified tolerance; a tentative date is not a
        # firm constraint. Neither can establish a definite contradiction.
        if self.uncertain or other.uncertain:
            return True
        return any(
            (left_start is None or left_start <= right_end)
            and (right_start is None or right_start <= left_end)
            for left_start, left_end in self.intervals
            for right_start, right_end in other.intervals
        )


def parse_collection_date(text: str) -> CollectionDate:
    """Validate ISO collection dates, qualifiers, alternatives, and ranges.

    A single optional prefix (<, ~, ?) applies to the whole expression.
    Slash-separated alternatives and en-dash ranges retain endpoint precision.
    As with publication-date bounds, a < cutoff includes its last possible day:
    a receipt date does not establish an earlier day of collection.
    """
    prefix = text[:1] if text.startswith(("<", "~", "?")) else ""
    raw = text[1:] if prefix else text
    intervals = []
    for alternative in raw.split("/"):
        endpoints = alternative.split("–")
        if len(endpoints) == 1:
            intervals.append(gregorian_bounds(alternative))
        elif len(endpoints) == 2:
            start, _ = gregorian_bounds(endpoints[0])
            _, end = gregorian_bounds(endpoints[1])
            if start > end:
                raise ValueError(f"Reversed collection date range: {alternative!r}")
            intervals.append((start, end))
        else:
            raise ValueError(f"Invalid collection date: {text!r}")
    if prefix == "<":
        return CollectionDate(((None, max(end for _, end in intervals)),))
    return CollectionDate(tuple(intervals), uncertain=prefix in ("~", "?"))
