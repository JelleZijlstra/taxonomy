import re
from typing import List

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("japanese.txt", "Mammalia-named by Japanese.pdf")

RGX = re.compile(
    r"""
    ^(?P<orig_name_author>[^\d]+?),?\s(?P<year>\d{4})\.\s(?P<rest>.*)$
    """,
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    starting_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if line.strip() == "引     用   文     献":
                return
            if re.match(r"^ *(\d+) {3,}", line):
                line = re.sub(r"^ *(\d+) *", "", line)
                if current_name:
                    yield {"raw_text": current_name, "pages": [starting_page]}
                starting_page = page
                current_name = [line]
            elif current_name:
                current_name.append(line)
    yield {"raw_text": current_name, "pages": [starting_page]}


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        match = RGX.match(text)
        if match:
            name.update(match.groupdict())
            text = name["rest"]
            if text.endswith(" nom. nud."):
                name["nomenclature_status"] = constants.NomenclatureStatus.nomen_nudum
                text = text[: -len(" nom. nud.")]
            parts = re.split(
                r"([A-Z][a-z A-Z]+\.?(?<!Mammals)(?<!Bunken|Jyurui| Mamm\.): )", text
            )
            name["verbatim_citation"], *parts = parts
            for label, value in zip(parts[::2], parts[1::2], strict=False):
                name[label.strip().rstrip(":.")] = value
        else:
            print(f"failed to match {text!r}")

        name["specimen_detail"] = name["raw_text"]
        for label in "Type locality", "Type Locality":
            if label in name:
                name["loc"] = name[label]
        if "Collector" in name:
            name["collector"] = name["Collector"]
        for label in "Date of Coll", "Date of Collection":
            if label in name:
                name["date"] = name[label].strip().rstrip(".")
        if "Holotype" in name:
            text = name["Holotype"]
            match = re.search(r"((adult|subadult|old) (male|female))", text)
            if match:
                name["age_gender"] = match.group(1)
            match = re.match(r"^(NSMT-M|YIO) ?(\d+)(, | \()", text)
            if match:
                name["type_specimen"] = f"{match.group(1)} {match.group(2)}"
                coll = match.group(1)
                if coll == "YIO":
                    name["collection"] = models.Collection.by_label("YIO")
                elif coll == "NSMT-M":
                    name["collection"] = models.Collection.by_label("NSMT")
            name["species_type_kind"] = constants.SpeciesGroupType.holotype

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    pages = lib.align_columns(pages, use_first=True, min_column=15)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=False)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(
        names,
        max_distance=0,
        match_year=True,
        try_manual=True,
        start_at="Cervus nippon var. yakushimae",
    )
    names = lib.write_to_db(
        names,
        SOURCE,
        dry_run=False,
        always_edit=True,
        skip_fields={"original_citation", "page_described"},
    )
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
