"""Helpers for comparing a Location's age with linked records."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .model import Location


# Keep this aligned with the Pleistocene/Pliocene boundary in the Period table.
PLEISTOCENE_START_AGE = 2_590_000


def is_recent_location(location: Location) -> bool:
    """Return whether the Location is unambiguously Recent."""
    return (
        location.min_period is not None
        and location.max_period is not None
        and location.min_period.name == "Recent"
        and location.max_period.name == "Recent"
    )


def is_non_recent_location(location: Location) -> bool:
    """Return whether the Location is unambiguously non-Recent."""
    return (
        location.min_period is not None
        and location.max_period is not None
        and location.min_period.name != "Recent"
        and location.max_period.name != "Recent"
    )


def get_youngest_location_age(location: Location) -> int | None:
    """Return the youngest possible age of the Location, in years ago."""
    if location.min_age is not None:
        return location.min_age
    if location.min_period is None:
        return None
    return location.min_period.get_min_age()


def is_pre_pleistocene_location(location: Location) -> bool:
    """Return whether the Location is entirely older than the Pleistocene."""
    youngest_age = get_youngest_location_age(location)
    return youngest_age is not None and youngest_age >= PLEISTOCENE_START_AGE
