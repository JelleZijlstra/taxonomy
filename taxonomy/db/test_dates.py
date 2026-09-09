import pytest

from taxonomy.db.constants import Calendar
from taxonomy.db.dates import last_day, to_gregorian


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("1582-10-04", "1582-10-14"),
        ("1700-02-28", "1700-03-10"),
        ("1700-02-29", "1700-03-11"),
        ("1800-02-29", "1800-03-12"),
        ("1896-07", "1896-08-12"),
        ("1900-02-28", "1900-03-12"),
        ("1900-02-29", "1900-03-13"),
        ("1900-03-01", "1900-03-14"),
        ("1910-09", "1910-10-13"),
        ("1913-12", "1914-01-13"),
        ("1901", "1902-01-13"),
        ("2000-02-29", "2000-03-13"),
        ("2100-03-01", "2100-03-15"),
    ],
)
def test_julian_to_gregorian(source: str, expected: str) -> None:
    assert to_gregorian(source, Calendar.julian) == expected


@pytest.mark.parametrize("source", ["1910", "1910-09", "1910-09-30", "2000-02-29"])
def test_gregorian_keeps_precision(source: str) -> None:
    assert to_gregorian(source) == source


@pytest.mark.parametrize(
    "source",
    [
        "1900-00",
        "1900-13",
        "0000",
        "1910-02-29",
        "1910-04-31",
        "1910-01-00",
        "1910-1911",
        "1910-1",
        "undated",
        "",
    ],
)
@pytest.mark.parametrize("calendar", list(Calendar))
def test_invalid_date(source: str, calendar: Calendar) -> None:
    with pytest.raises(ValueError, match="Invalid date"):
        to_gregorian(source, calendar)


def test_calendar_specific_leap_day() -> None:
    assert last_day("1900-02", Calendar.julian) == (1900, 2, 29)
    assert last_day("1900-02", Calendar.gregorian) == (1900, 2, 28)
    with pytest.raises(ValueError, match="Invalid date"):
        to_gregorian("1900-02-29")


def test_calendar_outside_supported_set() -> None:
    with pytest.raises(ValueError, match="Unsupported calendar"):
        to_gregorian("1910", 999)  # type: ignore[arg-type]
