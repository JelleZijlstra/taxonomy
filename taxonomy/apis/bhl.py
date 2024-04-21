import collections
import csv
import enum
import functools
import itertools
import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, TypeVar

import httpx
import Levenshtein

from taxonomy import config
from taxonomy.db import helpers

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


@lru_cache(maxsize=1024)
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


# profiling shows significant overhead from JSON decoding otherwise
@lru_cache(maxsize=1024)
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


@lru_cache(maxsize=1024)
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


def get_part_metadata(part_id: int) -> dict[str, Any]:
    result = json.loads(_get_part_metadata_string(str(part_id)))
    if result["Status"] != "ok":
        raise ValueError(f"Bad status: {result['Status']} {result['ErrorMessage']}")
    return result["Result"][0]


@cached(CacheDomain.bhl_part)
def _get_part_metadata_string(part_id: str) -> str:
    api_key = config.get_options().bhl_api_key
    url = (
        f"https://www.biodiversitylibrary.org/api3?op=GetPartMetadata&id={part_id}"
        f"&pages=t&idtype=bhl&format=json&apikey={api_key}"
    )
    return httpx.get(url).text


def is_external_item(item_id: int) -> bool:
    metadata = get_item_metadata(item_id)
    if metadata is None:
        return False
    return not metadata.get("Pages") and bool(metadata.get("ExternalUrl"))


def volume_matches(our_volume: str, bhl_volume: str) -> bool:
    if our_volume == bhl_volume:
        return True
    return bool(
        re.match(rf"(n\.s\. )?(no|v|V|Jahrg)\.{our_volume}(\s|$|:|=)", bhl_volume)
    )


def get_possible_items(
    title_id: int, year: int, volume: str | None = None
) -> list[int]:
    title_metadata = get_title_metadata(title_id)
    item_ids = []
    matching_volume_item_ids = []
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
            matching_volume_item_ids.append(item["ItemID"])
        # Allow the year before in case it was published late
        elif item_year == year or item_year == year - 1:
            item_ids.append(item["ItemID"])
        elif "EndYear" in item:
            item_end_year = int(item["EndYear"])
            if item_year <= year <= item_end_year:
                item_ids.append(item["ItemID"])
        elif "Volume" in item:
            if m := re.fullmatch(r"v.\d+ \((\d+)\)", item["Volume"]):
                volume_year = int(m.group(1))
                if year == volume_year or year == volume_year + 1:
                    item_ids.append(item["ItemID"])
    if matching_volume_item_ids:
        return matching_volume_item_ids
    return item_ids


def get_page_id_to_index(item_id: int) -> dict[int, int]:
    item_metadata = get_item_metadata(item_id)
    if item_metadata is None:
        return {}
    return {page["PageID"]: i for i, page in enumerate(item_metadata["Pages"])}


def get_filtered_pages_and_indices(
    item_id: int,
) -> tuple[dict[int, int], list[dict[str, Any]]]:
    item_metadata = get_item_metadata(item_id)
    if item_metadata is None:
        return {}, []
    pages = [page for page in item_metadata["Pages"] if not _is_plate_page(page)]
    return {page["PageID"]: i for i, page in enumerate(pages)}, pages


def is_contiguous_range(
    item_id: int,
    start_page_id: int,
    end_page_id: int,
    page_id_to_index: dict[int, int] | None = None,
    *,
    allow_unnumbered: bool = True,
    ignore_plates: bool = False,
    verbose: bool = False,
) -> bool:
    if start_page_id == end_page_id:
        return True
    item_metadata = get_item_metadata(item_id)
    if item_metadata is None:
        if verbose:
            print(f"not contiguous: Item {item_id} not found")
        return False
    if page_id_to_index is None:
        page_id_to_index = get_page_id_to_index(item_id)
    start_index = page_id_to_index.get(start_page_id)
    end_index = page_id_to_index.get(end_page_id)
    if start_index is None or end_index is None or end_index < start_index:
        if verbose:
            print(
                f"not contiguous: Page indexes {start_index} and"
                f" {end_index} not found or not in order"
            )
        return False
    start_page_no = _get_number_from_page(item_metadata["Pages"][start_index])
    if start_page_no is None:
        if verbose:
            print(f"not contiguous: Start page {start_page_no} is not numbered")
        return False
    for page in item_metadata["Pages"][start_index + 1 : end_index + 1]:
        page_no = _get_number_from_page(page)
        if page_no is None:
            if allow_unnumbered:
                continue
            if ignore_plates and _is_plate_page(page):
                continue
            if verbose:
                print(f"not contiguous: Unnumbered page {page}")
            return False
        if page_no <= start_page_no:
            if verbose:
                print(f"not contiguous: Page {page_no} is not after {start_page_no}")
            return False
        start_page_no = page_no
    return True


def _is_plate_page(page: dict[str, Any]) -> bool:
    page_types = {t["PageTypeName"].strip() for t in page["PageTypes"]}
    if not (page_types <= {"Blank", "Illustration", "Text"}):
        return False
    if "Illustration" in page_types:
        return not any(_is_numbered_page(number) for number in page["PageNumbers"])
    else:
        return not page["PageNumbers"]


def _get_number_from_page(item: dict[str, Any]) -> int | None:
    for number in item["PageNumbers"]:
        if _is_numbered_page(number):
            try:
                return int(number["Number"])
            except ValueError:
                pass
        elif m := re.fullmatch(r"%(\d+)", number["Number"]):
            return int(m.group(1))
    return None


def _is_numbered_page(page: dict[str, Any]) -> bool:
    prefix = page.get("Prefix", "")
    return prefix.endswith("Page") or prefix == "p."


def get_possible_pages(item_id: int, page_number: str) -> list[int]:
    item_metadata = get_item_metadata(item_id)
    if not item_metadata:
        return []
    return _get_matching_pages(item_metadata["Pages"], page_number)


def _get_matching_pages(pages: list[dict[str, Any]], page_number: str) -> list[int]:
    page_ids = []
    for page in pages:
        if _page_number_matches(page, page_number):
            page_ids.append(page["PageID"])
    return page_ids


def _page_number_matches(page: dict[str, Any], page_number: str) -> bool:
    if str(_get_number_from_page(page)) == page_number:
        return True
    for number in page["PageNumbers"]:
        if (
            _is_numbered_page(number)
            and number.get("Number", "").casefold() == page_number.casefold()
        ):
            return True
        if number.get("Prefix") == "Plate":
            if m := re.fullmatch(r"pl. (\d+)", page_number):
                plate_number = m.group(1)
                if plate_number == number["Number"]:
                    return True
                try:
                    numeric_bhl_number = helpers.parse_roman_numeral(number["Number"])
                    numeric_hesp_number = int(plate_number)
                except ValueError:
                    pass
                else:
                    if numeric_bhl_number == numeric_hesp_number:
                        return True
    return False


def get_possible_pages_from_part(part_id: int, page_number: str) -> list[int]:
    part_metadata = get_part_metadata(part_id)
    if not part_metadata:
        return []
    return _get_matching_pages(part_metadata["Pages"], page_number)


def get_possible_parts_from_page(page_id: int) -> Iterable[int]:
    page_metadata = get_page_metadata(page_id)
    if "ItemID" not in page_metadata:
        return
    item_metadata = get_item_metadata(int(page_metadata["ItemID"]))
    if item_metadata is None or "Parts" not in item_metadata:
        return
    for part in item_metadata["Parts"]:
        part_metadata = get_part_metadata(part["PartID"])
        if any(page["PageID"] == page_id for page in part_metadata["Pages"]):
            yield part["PartID"]


def contains_name(page_id: int, name: str, max_distance: int = 3) -> bool:
    if len(name) <= 6:
        max_distance = 0
    elif len(name) <= 10:
        max_distance = min(max_distance, 1)
    closest = closest_match(page_id, name)
    return closest < max_distance


def contains_name_with_distance(
    page_id: int, name: str, max_distance: int = 3
) -> tuple[bool, int]:
    if len(name) <= 6:
        max_distance = 0
    elif len(name) <= 10:
        max_distance = min(max_distance, 1)
    closest = closest_match(page_id, name)
    return closest < max_distance, closest


def closest_match(page_id: int, name: str) -> int:
    page_metadata = get_page_metadata(page_id)
    folded_name = name.casefold().replace("_", "").replace(".", "").replace(",", "")
    for name_data in page_metadata["Names"]:
        for name in name_data.values():
            if name.casefold() == folded_name:
                return 0

    ocr_text = page_metadata["OcrText"].casefold().replace(".", "").replace(",", "")
    if folded_name in ocr_text:
        return 0
    words = folded_name.split()
    return min(
        (
            Levenshtein.distance(" ".join(window), folded_name)
            for window in _sliding_window(ocr_text.split(), len(words))
        ),
        default=1000,
    )


@dataclass
class PossiblePage:
    page_id: int
    page_number: str
    contains_text: bool
    contains_end_page: bool
    year_matches: bool
    ocr_text: str = field(repr=False)
    item_id: int
    min_distance: int

    @property
    def page_url(self) -> str:
        return f"https://www.biodiversitylibrary.org/page/{self.page_id}"

    @property
    def is_confident(self) -> bool:
        return self.contains_text and self.contains_end_page and bool(self.ocr_text)

    def sort_key(self) -> tuple[object, ...]:
        # Best match first
        return (
            not self.contains_text,
            not self.year_matches,
            not self.contains_end_page,
            self.min_distance,
            self.page_id,
        )


def find_possible_pages(
    title_ids: Sequence[int],
    *,
    year: int,
    volume: str | None = None,
    start_page: str,
    end_page: str | None = None,
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
        item_metadata = get_item_metadata(item)
        if item_metadata is None:
            continue
        if "Year" in item_metadata:
            year_matches = int(item_metadata["Year"]) == year
        else:
            year_matches = False
        for page_id in get_possible_pages(item, start_page):
            if end_page is None:
                contains_end_page = True
            else:
                contains_end_page = any(get_possible_pages(item, end_page))
            page_metadata = get_page_metadata(page_id)
            if "OcrText" not in page_metadata:
                yield PossiblePage(
                    page_id=page_id,
                    page_number=start_page,
                    contains_text=False,
                    contains_end_page=contains_end_page,
                    year_matches=year_matches,
                    ocr_text="",
                    item_id=item,
                    min_distance=1000,
                )
                continue
            ocr_text = page_metadata["OcrText"]
            pairs = [
                contains_name_with_distance(page_id, name) for name in contains_text
            ]
            contains = any(c for c, _ in pairs)
            min_distance = min(distance for _, distance in pairs)
            yield PossiblePage(
                page_id=page_id,
                page_number=start_page,
                contains_text=contains,
                contains_end_page=contains_end_page,
                year_matches=year_matches,
                ocr_text=ocr_text,
                item_id=item,
                min_distance=min_distance,
            )


class UrlType(enum.Enum):
    bhl_bibliography = enum.auto()
    bhl_item = enum.auto()
    bhl_page = enum.auto()
    bhl_part = enum.auto()
    other_bhl = enum.auto()
    biostor_ref = enum.auto()
    other_biostor = enum.auto()
    google_books = enum.auto()
    archive_org = enum.auto()
    hdl = enum.auto()
    hathitrust = enum.auto()
    other = enum.auto()


@dataclass
class ParsedUrl:
    url_type: UrlType
    payload: str

    def __str__(self) -> str:
        match self.url_type:
            case UrlType.bhl_bibliography:
                return (
                    f"https://www.biodiversitylibrary.org/bibliography/{self.payload}"
                )
            case UrlType.bhl_item:
                return f"https://www.biodiversitylibrary.org/item/{self.payload}"
            case UrlType.bhl_page:
                return f"https://www.biodiversitylibrary.org/page/{self.payload}"
            case UrlType.bhl_part:
                return f"https://www.biodiversitylibrary.org/part/{self.payload}"
            case UrlType.other_bhl:
                return self.payload
            case UrlType.biostor_ref:
                return f"http://biostor.org/reference/{self.payload}"
            case (
                UrlType.other_biostor
                | UrlType.google_books
                | UrlType.archive_org
                | UrlType.other
                | UrlType.hathitrust
                | UrlType.hdl
            ):
                return self.payload
        return "<unknown url>"

    def is_bhl(self) -> bool:
        return self.url_type in {
            UrlType.bhl_bibliography,
            UrlType.bhl_item,
            UrlType.bhl_page,
            UrlType.bhl_part,
        }


def parse_possible_bhl_url(url: str) -> ParsedUrl:
    if match := re.fullmatch(
        r"https?://(?:www\.)?biodiversitylibrary\.org/([a-z]+)/(\d+)", url
    ):
        match match.group(1):
            case "bibliography":
                return ParsedUrl(UrlType.bhl_bibliography, match.group(2))
            case "item" | "itempdf":
                return ParsedUrl(UrlType.bhl_item, match.group(2))
            case "page":
                return ParsedUrl(UrlType.bhl_page, match.group(2))
            case "part" | "partpdf":
                return ParsedUrl(UrlType.bhl_part, match.group(2))
    elif match := re.fullmatch(r"https?://biostor\.org/reference/(\d+)", url):
        return ParsedUrl(UrlType.biostor_ref, match.group(1))
    elif "biodiversitylibrary.org" in url:
        return ParsedUrl(UrlType.other_bhl, url)
    elif "biostor.org" in url:
        return ParsedUrl(UrlType.other_biostor, url)
    # TODO parse these more precisely so we get consistent URLs
    # Maybe make specific types into subclasses of ParsedUrl instead,
    # so they can have different types of payloads.
    elif "books.google.com" in url:
        return ParsedUrl(UrlType.google_books, url)
    elif "archive.org" in url:
        return ParsedUrl(UrlType.archive_org, url)
    elif "hdl.handle.net" in url:
        return ParsedUrl(UrlType.hdl, url)
    elif "hathitrust.org" in url:
        return ParsedUrl(UrlType.hathitrust, url)
    return ParsedUrl(UrlType.other, url)


def get_bhl_item_from_url(url: str) -> int | None:
    pair = parse_possible_bhl_url(url)
    match pair:
        case ParsedUrl(UrlType.bhl_item, id):
            return int(id)
        case ParsedUrl(UrlType.bhl_page, id):
            data = get_page_metadata(int(id))
            if data is not None:
                return int(data["ItemID"])
        case ParsedUrl(UrlType.bhl_part, id):
            data = get_part_metadata(int(id))
            if data is not None:
                return int(data["ItemID"])
    return None


def get_bhl_bibliography_from_url(url: str) -> int | None:
    match parse_possible_bhl_url(url):
        case ParsedUrl(UrlType.bhl_bibliography, id):
            return int(id)
        case ParsedUrl(UrlType.bhl_item, id):
            data = get_item_metadata(int(id))
            if data is not None:
                return int(data["TitleID"])
        case ParsedUrl(UrlType.bhl_page, id):
            data = get_page_metadata(int(id))
            if data is not None:
                item_id = data["ItemID"]
                data = get_item_metadata(item_id)
                if data is not None:
                    return int(data["TitleID"])
        case ParsedUrl(UrlType.bhl_part, id):
            data = get_part_metadata(int(id))
            if data is not None:
                item_id = data["ItemID"]
                data = get_item_metadata(item_id)
                if data is not None:
                    return int(data["TitleID"])
    return None


EXCLUDED_FROM_PRINTING = {
    "ItemID",
    "TitleID",
    "ThumbnailPageID",
    "Source",
    "IsVirtual",
    "HoldingInstitution",
    "Sponsor",
    "Language",
    "Rights",
    "CopyrightStatus",
    "ItemUrl",
    "TitleUrl",
    "ItemThumbUrl",
    "ItemTextUrl",
    "ItemPDFUrl",
    "ItemImagesUrl",
    "CreationDate",
    "Pages",
    "Subjects",
    "Items",
}


def print_data_for_possible_bhl_url(url: str) -> bool:
    parsed = parse_possible_bhl_url(url)
    if not parsed.is_bhl():
        return False
    item_id = get_bhl_item_from_url(url)
    if item_id is not None:
        item_metadata = get_item_metadata(item_id)
        if item_metadata is None:
            print(f"No metadata found for item {item_id} from {url}")
        else:
            print_metadata(item_metadata)
    else:
        bibliography_id = get_bhl_bibliography_from_url(url)
        if bibliography_id is not None:
            biblio_metadata = get_title_metadata(bibliography_id)
            if biblio_metadata is None:
                print(
                    f"No metadata found for bibliography {bibliography_id} from {url}"
                )
            else:
                print_metadata(biblio_metadata)
        else:
            print(f"No item or bibliography found for {url}")
    return True


def print_metadata(data: dict[str, Any]) -> None:
    for key, value in data.items():
        if key not in EXCLUDED_FROM_PRINTING:
            print(f"{key}: {value}")


def clear_caches_related_to_url(url: str) -> None:
    item_id = get_bhl_item_from_url(url)
    if item_id is not None:
        dirty_cache(CacheDomain.bhl_item, str(item_id))
    biblio_id = get_bhl_bibliography_from_url(url)
    if biblio_id is not None:
        dirty_cache(CacheDomain.bhl_title, str(biblio_id))
    match parse_possible_bhl_url(url):
        case ParsedUrl(UrlType.bhl_page, id):
            dirty_cache(CacheDomain.bhl_page, id)
        case ParsedUrl(UrlType.bhl_part, id):
            dirty_cache(CacheDomain.bhl_part, id)


# From https://docs.python.org/3.10/library/itertools.html#itertools-recipes
def _sliding_window(iterable: Iterable[T], n: int) -> Iterable[tuple[T, ...]]:
    it = iter(iterable)
    window = collections.deque(itertools.islice(it, n), maxlen=n)
    if len(window) == n:
        yield tuple(window)
    for x in it:
        window.append(x)
        yield tuple(window)
