import re
from typing import List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("gymnophiona.txt", "Gymnophiona-names (Smith & Smith 1972).pdf")


def extract_names(pages: PagesT) -> DataT:
    current_name: List[str] = []
    current_page = 0
    for page, lines in pages:
        for line in lines:
            if re.match(r"\d+(\.\d+)?", line.strip()):
                # new name
                if current_name:
                    yield {
                        "raw_text": current_name,
                        "pages": list(range(current_page, page + 1)),
                    }
                current_name = []
                current_page = page
            else:
                current_name.append(line)
    yield {"raw_text": current_name, "pages": [page]}


def split_names(names: DataT) -> DataT:
    for name in names:
        lines = [line for line in name["raw_text"] if line.strip()]
        if not lines:
            continue
        name["name_line"] = lines[0]
        name["authority_citation"] = lines[1]
        name["type_data"] = lines[2]
        if len(lines) > 3 and lines[3].startswith("="):
            name["current_name"] = lines[3]
        yield name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["original_name"] = lib.clean_string(name["name_line"].replace("*", " "))
        match = re.match(r"(.*?) (\d{4}) (.*)$", name["authority_citation"])
        assert match is not None, name
        name["authority"] = match.group(1)
        name["year"] = match.group(2)
        name["verbatim_citation"] = match.group(3)
        if " " in name["original_name"]:
            name["loc"] = name["type_data"]
        else:
            name["verbatim_type"] = name["type_data"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    names = extract_names(pages)
    names = split_names(names)
    names = split_fields(names)
    names = lib.clean_text(names)
    names = lib.translate_to_db(names, source=SOURCE, verbose=True)

    names = lib.associate_names(
        names,
        use_taxon_match=True,
        try_manual=True,
        max_distance=1,
        start_at="Dermophinae",
    )
    names = lib.write_to_db(
        names,
        SOURCE,
        dry_run=False,
        always_edit=True,
        skip_fields={
            "original_citation",
            "type_specimen",
            "collection",
            "name_complex",
        },
    )
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for n in main():
        print(n)
