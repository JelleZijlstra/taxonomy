"""Publication-date constraints, with source precision retained at the cutoff."""

import re
from dataclasses import dataclass

from taxonomy.db import dates
from taxonomy.db.constants import Calendar


@dataclass(frozen=True)
class DateBounds:
    earliest: str | None
    latest: str | None
    adopted: str | None


def parse_date(date: str, calendar: Calendar | None = None) -> DateBounds:
    """Parse an observation or a before/after cutoff into Gregorian bounds.

    Dates do not record times within a day. Inference adopts the cutoff itself,
    not an invented preceding/following day. Partial cutoffs retain the full
    possible interval of the source date.
    """
    operator = date[:1] if date.startswith(("<", ">")) else ""
    raw = date[1:] if operator else date
    calendar = calendar or Calendar.gregorian
    if calendar is Calendar.gregorian and (
        match := re.fullmatch(r"([0-9]{4})-([0-9]{4})", raw)
    ):
        start, end = match.groups()
        if not 1 <= int(start) <= int(end) <= 9999:
            raise ValueError(f"Invalid date range: {raw!r}")
        earliest, latest = f"{start}-01-01", f"{end}-12-31"
        adopted = raw
    else:
        earliest, latest = dates.gregorian_bounds(raw, calendar)
        adopted = dates.to_gregorian(raw, calendar)
    if operator == "<":
        return DateBounds(None, latest, adopted)
    if operator == ">":
        return DateBounds(earliest, None, None)
    return DateBounds(earliest, latest, adopted)


def intersect(observations: list[DateBounds]) -> DateBounds:
    """Return the tightest upper bound; lower bounds alone cannot supply one."""
    earliest = max(
        (o.earliest for o in observations if o.earliest is not None), default=None
    )
    latest = min((o.latest for o in observations if o.latest is not None), default=None)
    if earliest is not None and latest is not None and earliest > latest:
        raise ValueError(
            f"contradictory publication dates: earliest {earliest} is after latest {latest}"
        )
    if latest is None:
        return DateBounds(earliest, None, None)
    candidates = [
        o.adopted for o in observations if o.latest == latest and o.adopted is not None
    ]
    adopted = max(
        candidates, key=lambda date: (parse_date(date).earliest or "", len(date))
    )
    return DateBounds(earliest, latest, adopted)
