import pprint
import re
import sys
from collections.abc import Iterable
from pathlib import Path

from bs4 import BeautifulSoup

from taxonomy.db import helpers

Key = tuple[tuple[str, ...], str]


def extract_raw_refs(filename: Path) -> list[list[str]]:
    html_doc = filename.read_text()
    soup = BeautifulSoup(html_doc, "html.parser")
    started_refs = False
    refs: list[list[str]] = []
    for span in soup.find_all("span"):
        if span.get("style") != 'font-size:4px;font-family:"Times"':
            continue
        text = span.get_text()
        if text in ("\nFEO\n", "\nsum\n"):
            continue
        if "). See " in text:
            continue
        if (
            "All titles of papers on land planarians reported from Japan, and titles of"
            in text
        ):
            break
        if not started_refs:
            if (
                "REFERENCES FOR THE GEOPLANINAE, CAENOPLANINAE AND PELMATOPLANINAE"
                " INDICES" in text
            ):
                started_refs = True
            continue
        for div in span.find_all("div"):
            left = int(div.get("style").split(":")[-1])
            if 40 <= left <= 100:
                refs[-1].append(div.get_text())
            elif 120 <= left <= 170:
                refs.append([div.get_text()])
            elif left > 275:
                continue
            else:
                print(left, div.prettify())
                assert False, left
    return refs


def clean_up_refs(refs: list[list[str]]) -> Iterable[str]:
    for ref in refs:
        yield helpers.clean_string(" ".join(ref))


def make_dict(refs: Iterable[str]) -> dict[Key, str]:
    out: dict[Key, str] = {}
    for ref in refs:
        match = re.match(r"^([^\d]+),? (\d{4}(-\d{4})?( [a-z])?)\. (.*)$", ref)
        assert match, ref
        raw_authors = match.group(1)
        year = match.group(2)
        rest = match.group(5)
        authors = re.sub(r"(?<=[ .\-])[A-Zl]\.", "", raw_authors)
        raw_author_list = re.split(r"[,&]", authors)
        raw_author_list = [aut.strip().lower() for aut in raw_author_list]
        author_list = tuple(
            aut
            for aut in raw_author_list
            if aut not in ("", "-", "von", "de", "du", "jr")
        )
        bits = re.split(r"(\d{4}( [a-z])?)(?=\. )", rest)
        print(bits)
        out[(author_list, year)] = f"{raw_authors} {year}. {bits[0]}"
        for i, bit in enumerate(bits[1:]):
            if i % 3 == 0:
                rest_of_ref = bits[1 + i + 2]
                out[(author_list, bit)] = f"{raw_authors} {bit}{rest_of_ref}"
    return out


def parse_refs(filename: Path) -> dict[Key, str]:
    raw_refs = extract_raw_refs(filename)
    half_cooked = clean_up_refs(raw_refs)
    out = make_dict(half_cooked)
    for value in out.values():
        print(value)
        print()
    return out


if __name__ == "__main__":
    refs = parse_refs(Path(sys.argv[1]))
    if False:
        pprint.pprint(refs)
