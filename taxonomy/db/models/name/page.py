import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from taxonomy import urlparse
from taxonomy.db import helpers
from taxonomy.db.models.base import LintConfig


@dataclass(frozen=True)
class Page:
    text: str
    is_raw: bool = False
    as_page: str | None = None
    within_page_detail: str | None = None

    def __str__(self) -> str:
        text = self.unique_page()
        if self.within_page_detail is not None:
            return f"{text} ({self.within_page_detail})"
        else:
            return text

    def is_range(self) -> bool:
        return (
            not self.is_raw
            and self.as_page is None
            and self.within_page_detail is None
            and bool(re.fullmatch(r"[0-9]+-[0-9]+", self.text))
        )

    def unique_page(self) -> str:
        parts = []
        if self.is_raw:
            parts.append("@")
        parts.append(self.text)
        if self.as_page is not None:
            parts.append(f" [as {self.as_page}]")
        return "".join(parts)

    def lint(self) -> Iterable[str]:
        if not self.is_raw:
            if not is_valid_page_number(self.text):
                yield f"Invalid page number {self.text!r}"
        if self.within_page_detail is not None and not is_valid_detail(
            self.within_page_detail
        ):
            yield f"Invalid detail {self.within_page_detail!r}"
        if (
            self.as_page is not None
            and not is_valid_page_number(self.as_page)
            and not re.fullmatch(r'"[^"]+"', self.as_page)
        ):
            yield f"Invalid as page {self.as_page!r}"

    def sort_key(self) -> tuple[object, ...]:
        if self.is_raw:
            return (3, self.text)
        elif self.text.isnumeric():
            return (1, int(self.text))
        elif helpers.is_valid_roman_numeral(self.text):
            return (0, helpers.parse_roman_numeral(self.text))
        else:
            return (2, self.text)


def parse_page_text(text: str | None) -> Iterable[Page]:
    if text is None:
        return
    # Optimize for some common cases
    if text.isnumeric():
        yield Page(text)
        return
    if "," not in text and "@" not in text and "(" not in text and "[" not in text:
        yield Page(text)
        return
    for match in re.finditer(
        r"(@?)([^@,()\[\]]+)(?: \[as ([^]]+)\])?(?: \(([^)]+)\))?(?=,|$)", text
    ):
        yield Page(
            text=match.group(2).strip(),
            is_raw=match.group(1) == "@",
            as_page=match.group(3),
            within_page_detail=match.group(4),
        )


def get_unique_page_text(text: str | None) -> Sequence[str]:
    unique_pages = []
    for page in parse_page_text(text):
        if page.is_range():
            unique_pages += page.text.split("-")
        else:
            unique_pages.append(page.unique_page())
    return unique_pages


def is_valid_detail(detail: str) -> bool:
    if detail.startswith(("fig. ", "figs. ", "table ")):
        return True
    if " " in detail:
        detail, number = detail.split(" ", 1)
        if not re.fullmatch(r"[0-9a-z]+", number):
            return False
    if detail not in ("fig.", "footnote", "table", "legend", "caption"):
        return False
    return True


def is_valid_page_number(part: str) -> bool:
    if part.isdecimal() and part.isascii():
        return True
    if re.fullmatch(r"[0-9]+-[0-9]+", part):
        return True
    if part.startswith("pl. "):
        number = part.removeprefix("pl. ")
        if helpers.is_valid_roman_numeral(number):
            return True
        if re.fullmatch(r"([A-Z]+-?)?[0-9]+[A-Za-z]*", number):
            return True
    if helpers.is_valid_roman_numeral(part):
        return True
    # Pretty common to see "S40" or "40A"
    if re.fullmatch(r"[A-Z]?[0-9]+[A-Z]?", part):
        return True
    if re.fullmatch(r"ID #\d+", part):
        return True
    if urlparse.is_valid_url(part):
        return True
    return False


def check_page(
    page_text: str | None,
    *,
    set_page: Callable[[str], None],
    obj: object,
    cfg: LintConfig,
    get_raw_page_regex: Callable[[], str | None] | None = None,
) -> Iterable[str]:
    if page_text is None:
        return
    parts = list(parse_page_text(page_text))
    for part in parts:
        yield from part.lint()
        if part.is_raw and get_raw_page_regex is not None:
            raw_page_regex = get_raw_page_regex()
            if raw_page_regex is not None and not re.fullmatch(
                raw_page_regex, part.text
            ):
                yield f"Invalid raw page {part.text!r} (does not match {raw_page_regex!r})"
    sorteed_parts = sorted(set(parts), key=Page.sort_key)
    new_text = ", ".join(str(part) for part in sorteed_parts)
    if new_text != page_text:
        message = f"Fixed page {page_text!r} -> {new_text!r}"
        if cfg.autofix and len(", ".join(str(part) for part in parts)) >= len(
            page_text
        ):
            print(f"{obj}: {message}")
            set_page(new_text)
        else:
            yield message
