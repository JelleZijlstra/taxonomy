# static analysis: ignore[impossible_pattern]
# TODO fix bug in pyanalyze
import collections
import csv
import enum
import functools
import itertools
import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import httpx
import Levenshtein

from taxonomy import config

from ..db.url_cache import CacheDomain, cached, dirty_cache

T = TypeVar("T")


def get_cache_dir() -> Path:
    options = config.get_options()
    path = options.data_path / "bhl"
    path.mkdir(exist_ok=True, parents=True)
    return path


def get_titles_data(force: bool = False) -> list[dict[str, str]]:
    cache_dir = get_cache_dir()
    cache_file = cache_dir / "titles.txt"
    if force or not cache_file.exists():
        data = httpx.get("https://www.biodiversitylibrary.org/Data/TSV/title.txt").text
        cache_file.write_text(data)
    with cache_file.open() as file:
        file.read(1)  # BOM or something
        return list(csv.DictReader(file, delimiter="\t"))


@functools.cache
def get_title_to_data() -> dict[str, list[dict[str, str]]]:
    output: dict[str, list[dict[str, str]]] = {}
    for row in get_titles_data():
        title = row["ShortTitle"].casefold().removeprefix("the ")
        output.setdefault(title, []).append(row)
    return output


def get_title_metadata(title_id: int) -> dict[str, Any]:
    result = json.loads(_get_title_metadata_string(str(title_id)))
    if result["Status"] != "ok":
        raise ValueError(f"Bad status: {result['Status']} {result['ErrorMessage']}")
    return result["Result"][0]


@cached(CacheDomain.bhl_title)
def _get_title_metadata_string(title_id: str) -> str:
    api_key = config.get_options().bhl_api_key
    return httpx.get(
        f"https://www.biodiversitylibrary.org/api3?op=GetTitleMetadata&id={title_id}"
        f"&idtype=bhl&items=t&format=json&apikey={api_key}"
    ).text


def get_item_metadata(item_id: int) -> dict[str, Any] | None:
    result = json.loads(_get_item_metadata_string(str(item_id)))
    if result["Status"] != "ok":
        raise ValueError(f"Bad status: {result['Status']} {result['ErrorMessage']}")
    if not result["Result"]:
        return None
    return result["Result"][0]


@cached(CacheDomain.bhl_item)
def _get_item_metadata_string(item_id: str) -> str:
    api_key = config.get_options().bhl_api_key
    url = (
        f"https://www.biodiversitylibrary.org/api3?op=GetItemMetadata&id={item_id}"
        f"&idtype=bhl&pages=t&parts=t&format=json&apikey={api_key}"
    )
    return httpx.get(url).text


def get_page_metadata(page_id: int) -> dict[str, Any]:
    result = json.loads(_get_page_metadata_string(str(page_id)))
    if result["Status"] != "ok":
        dirty_cache(CacheDomain.bhl_page, str(page_id))
        raise ValueError(f"Bad status: {result['Status']} {result['ErrorMessage']}")
    return result["Result"][0]


@cached(CacheDomain.bhl_page)
def _get_page_metadata_string(page_id: str) -> str:
    api_key = config.get_options().bhl_api_key
    url = (
        f"https://www.biodiversitylibrary.org/api3?op=GetPageMetadata&pageid={page_id}"
        f"&idtype=bhl&ocr=t&names=t&format=json&apikey={api_key}"
    )
    return httpx.get(url).text


def volume_matches(our_volume: str, bhl_volume: str) -> bool:
    if our_volume == bhl_volume:
        return True
    return bool(re.match(rf"v\.{our_volume}(\s|$|:)", bhl_volume))


def get_possible_items(
    title_id: int, year: int, volume: str | None = None
) -> list[int]:
    title_metadata = get_title_metadata(title_id)
    item_ids = []
    for item in title_metadata["Items"]:
        if "Year" not in item:
            continue
        item_year = int(item["Year"])
        if (
            volume is not None
            and "Volume" in item
            and volume_matches(volume, item["Volume"])
            and abs(item_year - year) <= 5
        ):
            item_ids.append(item["ItemID"])
        # Allow the year before in case it was published late
        elif item_year == year or item_year == year - 1:
            item_ids.append(item["ItemID"])
        elif "EndYear" in item:
            item_end_year = int(item["EndYear"])
            if item_year <= year <= item_end_year:
                item_ids.append(item["ItemID"])
    return item_ids


def get_possible_pages(item_id: int, page_number: int) -> list[int]:
    item_metadata = get_item_metadata(item_id)
    if not item_metadata:
        return []
    page_ids = []
    for page in item_metadata["Pages"]:
        if any(
            number.get("Prefix") == "Page" and number.get("Number") == str(page_number)
            for number in page["PageNumbers"]
        ):
            page_ids.append(page["PageID"])
    return page_ids


def contains_name(page_id: int, name: str, max_distance: int = 3) -> bool:
    page_metadata = get_page_metadata(page_id)
    folded_name = name.casefold().replace("_", "")
    for name_data in page_metadata["Names"]:
        for name in name_data.values():
            if name.casefold() == folded_name:
                return True

    ocr_text = page_metadata["OcrText"].casefold()
    if folded_name in ocr_text:
        return True
    words = folded_name.split()
    for window in _sliding_window(ocr_text.split(), len(words)):
        if Levenshtein.distance(" ".join(window), folded_name) < max_distance:
            return True

    return False


@dataclass
class PossiblePage:
    page_id: int
    page_number: int
    contains_text: bool
    contains_end_page: bool
    ocr_text: str = field(repr=False)

    @property
    def page_url(self) -> str:
        return f"https://www.biodiversitylibrary.org/page/{self.page_id}"

    @property
    def is_confident(self) -> bool:
        return self.contains_text and self.contains_end_page


def find_possible_pages(
    title_ids: Sequence[int],
    *,
    year: int,
    volume: str | None = None,
    start_page: int,
    end_page: int | None = None,
    contains_text: Sequence[str],
    known_item_id: int | None = None,
) -> Iterable[PossiblePage]:
    if known_item_id is not None:
        possible_items = [known_item_id]
    else:
        possible_items = [
            item
            for title_id in title_ids
            for item in get_possible_items(title_id, year, volume)
        ]
    for item in possible_items:
        for page_id in get_possible_pages(item, start_page):
            if end_page is None:
                contains_end_page = True
            else:
                contains_end_page = any(get_possible_pages(item, end_page))
            page_metadata = get_page_metadata(page_id)
            ocr_text = page_metadata["OcrText"]
            contains = any(contains_name(page_id, name) for name in contains_text)
            yield PossiblePage(
                page_id, start_page, contains, contains_end_page, ocr_text
            )


class UrlType(enum.Enum):
    bibliography = enum.auto()
    item = enum.auto()
    page = enum.auto()
    biostor = enum.auto()


def parse_possible_bhl_url(url: str) -> tuple[UrlType, int] | None:
    if match := re.fullmatch(
        r"https?://www.biodiversitylibrary.org/([a-z]+)/(\d+)", url
    ):
        match match.group(1):
            case "bibliography":
                return UrlType.bibliography, int(match.group(2))
            case "item":
                return UrlType.item, int(match.group(2))
            case "page":
                return UrlType.page, int(match.group(2))
    elif match := re.fullmatch(r"https?://biostor.org/reference/(\d+)", url):
        return UrlType.biostor, int(match.group(1))
    return None


def get_bhl_item_from_url(url: str) -> int | None:
    pair = parse_possible_bhl_url(url)
    match pair:
        case (UrlType.item, id):
            assert isinstance(id, int), "help mypy"
            return id
        case (UrlType.page, id):
            assert isinstance(id, int), "help mypy"
            data = get_page_metadata(id)
            if data is not None:
                return data["ItemID"]
    return None


def get_bhl_bibliography_from_url(url: str) -> int | None:
    match parse_possible_bhl_url(url):
        case (UrlType.bibliography, id):
            assert isinstance(id, int), "help mypy"
            return id
        case (UrlType.item, id):
            assert isinstance(id, int), "help mypy"
            data = get_item_metadata(id)
            if data is not None:
                return data["TitleID"]
        case (UrlType.page, id):
            assert isinstance(id, int), "help mypy"
            data = get_page_metadata(id)
            if data is not None:
                item_id = data["ItemID"]
                data = get_item_metadata(item_id)
                if data is not None:
                    return data["TitleID"]
    return None


# From https://docs.python.org/3.10/library/itertools.html#itertools-recipes
def _sliding_window(iterable: Iterable[T], n: int) -> Iterable[tuple[T, ...]]:
    it = iter(iterable)
    window = collections.deque(itertools.islice(it, n), maxlen=n)
    if len(window) == n:
        yield tuple(window)
    for x in it:
        window.append(x)
        yield tuple(window)
