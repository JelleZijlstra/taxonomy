import re
from typing import Any, Dict, List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("rodentianafrica.txt", "Rodentia N Africa-types.pdf")

RGX = re.compile(
    r"""
    ^\d+\.\s—\s?(?P<orig_name_author>[^,]+),\s(?P<verbatim_citation>.*?\s\d{4})\.\s(?P<rest>.*)$
    """,
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: Dict[str, Any] = {}
    current_lines: List[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        for line in lines:
            if (
                "Brought to you by | " in line
                or "Authenticated |" in line
                or "Download Date |" in line
            ):
                continue
            line = line.rstrip()
            if not line:
                continue
            match = re.match(r"^ +(\d+)\. —", line)
            if match is not None:
                # new name
                if current_name:
                    yield current_name
                current_name = {"pages": [page], "id": match.group(1)}
                start_label("raw_text", line)
                continue
            elif not current_name:
                continue
            match = re.match(r"^ +([A-Z][a-z]+) : ", line)
            if match is not None:
                label = match.group(1)
                start_label(label, line)
            else:
                current_lines.append(line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        match = RGX.match(text)
        if match:
            name.update(match.groupdict())
            text = name["rest"]
            if text.startswith("Type from"):
                if "; " in text:
                    loc, rest = text.split("; ", 1)
                    name["loc"] = loc
                    name["specimen_detail"] = rest
                else:
                    name["loc"] = text
            else:
                name["specimen_detail"] = text
        else:
            continue
        for field in ("loc", "specimen_detail"):
            if field in name and name[field].strip() == "$":
                del name[field]
            if field in name and "[" in name[field]:
                name[field] = name[field] + " [brackets original]"

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "USNM", SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names)
    names = lib.write_to_db(
        names,
        SOURCE,
        dry_run=False,
        edit_if_no_holotype=False,
        edit_if=lambda name: "specimen_detail" in name,
    )
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
