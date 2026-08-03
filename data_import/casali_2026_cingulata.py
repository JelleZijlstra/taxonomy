import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterable
from pathlib import Path

from data_import import lib
from data_import.ce_file import write_ce_file
from taxonomy.db import constants, models

ARTICLE_NAME = "Cingulata-phylogeny (Casali et al. 2026) (supplement).docx"
OUTPUT_PATH = Path(__file__).parent / "ce_files" / "casali_2026_cingulata.ce.jsonl"
NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
RANKS = {
    "Order": constants.Rank.order,
    "Suborder": constants.Rank.suborder,
    "Superfamily": constants.Rank.superfamily,
    "Family": constants.Rank.family,
    "Subfamily": constants.Rank.subfamily,
    "Tribe": constants.Rank.tribe,
    "Subtribe": constants.Rank.subtribe,
    "Unranked": constants.Rank.unranked,
}
FIXED_UNRANKED_DEPTHS = {
    "Glyptophracta": 3,
    "Euphracta": 4,
    "Glyptodonta": 4,
    "Glyptodontoinei": 6,
}


def _paragraphs(path: Path) -> list[tuple[str, list[tuple[str, bool]]]]:
    """Return paragraph text and directly italicized runs from the DOCX."""
    with zipfile.ZipFile(path) as archive:
        root = ET.fromstring(archive.read("word/document.xml"))
    output = []
    for paragraph in root.findall(".//w:body/w:p", NS):
        runs = []
        for run in paragraph.findall("w:r", NS):
            text = "".join(node.text or "" for node in run.findall("w:t", NS))
            if not text:
                continue
            italic = run.find("w:rPr/w:i", NS) is not None
            runs.append((text, italic))
        output.append(("".join(text for text, _ in runs).strip(), runs))
    return output


def _page(paragraph_index: int) -> str:
    if paragraph_index <= 31:
        return "1"
    if paragraph_index <= 66:
        return "2"
    if paragraph_index <= 101:
        return "3"
    return "4"


def _authority_and_year(text: str) -> tuple[str | None, str | None]:
    text = text.strip().strip("()")
    text = re.sub(r"\s*\[type\]\s*$", "", text)
    if text == "NEW" or not text:
        return None, None
    match = re.fullmatch(r"(.+?)(?:,\s*|\s+)(\d{4}[a-z]?)", text)
    if match is None:
        return text, None
    return match.group(1), match.group(2)


def _heading(text: str) -> tuple[constants.Rank, str, str] | None:
    match = re.fullmatch(
        r"(?:\(!\)\s*)?(Order|Suborder|Superfamily|Family|Subfamily|Tribe|Subtribe|Unranked)\s+†?([^\s]+)\s*(.*)",
        text,
    )
    if match is None:
        return None
    return RANKS[match.group(1)], match.group(2), match.group(3)


def _depth_for_heading(
    rank: constants.Rank, name: str, stack: dict[int, lib.CEDict]
) -> int:
    if rank is constants.Rank.unranked:
        return FIXED_UNRANKED_DEPTHS[name]
    base = {
        constants.Rank.order: 0,
        constants.Rank.suborder: 1,
        constants.Rank.superfamily: 2,
        constants.Rank.family: 3,
        constants.Rank.subfamily: 4,
        constants.Rank.tribe: 5,
        constants.Rank.subtribe: 6,
    }[rank]
    if any(
        depth == 4 and ce["rank"] is constants.Rank.unranked
        for depth, ce in stack.items()
    ):
        base += 2
    return base


def _make_ce(
    *,
    article: models.Article,
    page: str,
    rank: constants.Rank,
    name: str,
    raw: str,
    authority_text: str,
    parent: lib.CEDict | None,
) -> lib.CEDict:
    ce: lib.CEDict = {
        "article": article,
        "page": page,
        "rank": rank,
        "name": name,
        "raw_data": raw,
    }
    if parent is not None:
        ce["parent"] = parent["name"]
        ce["parent_rank"] = parent["rank"]
    authority, year = _authority_and_year(authority_text)
    if authority is not None:
        ce["authority"] = authority
    if year is not None:
        ce["year"] = year
    if "†" in raw:
        ce["age_class"] = constants.AgeClass.fossil
    return ce


def extract(article: models.Article) -> Iterable[lib.CEDict]:
    path = article.get_path()
    assert path is not None
    paragraphs = _paragraphs(path)
    stack: dict[int, lib.CEDict] = {}
    entries: list[lib.CEDict] = []
    by_name_rank: dict[tuple[str, constants.Rank], lib.CEDict] = {}

    for index, (raw, runs) in enumerate(paragraphs[:134]):
        if not raw:
            continue
        if heading := _heading(raw):
            rank, name, authority_text = heading
            depth = _depth_for_heading(rank, name, stack)
            parent = stack.get(depth - 1)
            ce = _make_ce(
                article=article,
                page=_page(index),
                rank=rank,
                name=name,
                raw=raw,
                authority_text=authority_text,
                parent=parent,
            )
            stack = {
                old_depth: old_ce
                for old_depth, old_ce in stack.items()
                if old_depth < depth
            }
            stack[depth] = ce
            entries.append(ce)
            by_name_rank[(name, rank)] = ce
            continue

        if "Laventan Eutatini" in raw:
            parent = stack[max(stack)]
            ce = _make_ce(
                article=article,
                page=_page(index),
                rank=constants.Rank.informal,
                name="Laventan Eutatini",
                raw=raw,
                authority_text="",
                parent=parent,
            )
            entries.append(ce)
            by_name_rank[(ce["name"], ce["rank"])] = ce
            continue

        italic_names: list[str] = []
        for text, italic in runs:
            if not italic or not text.strip():
                continue
            if italic_names and raw.find(italic_names[-1] + text.strip()) >= 0:
                italic_names[-1] += text.strip()
            else:
                italic_names.append(text.strip())
        assert italic_names, (index, raw)
        parent = stack[max(stack)]
        name_starts = []
        search_start = 0
        for name in italic_names:
            name_start = raw.find(name.strip().strip('"'), search_start)
            assert name_start >= 0, (index, raw, name)
            name_starts.append(name_start)
            search_start = name_start + len(name)
        boundaries = [0]
        for name_start in name_starts[1:]:
            boundary = raw.rfind("†", boundaries[-1], name_start)
            assert boundary >= 0, (index, raw, italic_names)
            boundaries.append(boundary)
        boundaries.append(len(raw))

        for position, name in enumerate(italic_names):
            name = name.strip().strip('"')
            entry_raw = raw[boundaries[position] : boundaries[position + 1]].strip()
            entry_name_start = entry_raw.find(name)
            assert entry_name_start >= 0, (index, entry_raw, name)
            authority_text = entry_raw[entry_name_start + len(name) :]
            key = (name, constants.Rank.genus)
            if key in by_name_rank:
                previous = by_name_rank[key]
                alternate = f"Also placed under {parent['name']} in the source."
                previous["comment"] = " ".join(
                    filter(None, [previous.get("comment"), alternate])
                )
                continue
            ce = _make_ce(
                article=article,
                page=_page(index),
                rank=constants.Rank.genus,
                name=name,
                raw=entry_raw,
                authority_text=authority_text,
                parent=parent,
            )
            entries.append(ce)
            by_name_rank[key] = ce

    yield from lib.validate_ce_parents(entries)


def main() -> None:
    article = models.Article.get(name=ARTICLE_NAME)
    write_ce_file(OUTPUT_PATH, extract(article))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
