import re
from typing import Any, Dict, List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source(
    "zmbchiroptera.txt", "Chiroptera Berlin Museum fur Naturkunde-types.pdf"
)
NAME_LINE_RGX = re.compile(
    r"""
    (?P<original_name>([A-Z][a-z]+|[A-Z]\[[a-z]+\]\.)(\s\([A-Z]?[a-z]+\))?((\svar\.)?\s[a-zü]+){1,2})(\s\[[a-z]+\])?\s
    (?P<authority>[A-Z][a-zü]+(\sde\sBalsac|-Edwards|-Neuwied|\s&\s[A-Z][a-z]+)?),\s
    (?P<year>\d{4})$
""",
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if re.match(r"^[A-Z][a-z]{2,} [A-Z][a-z]{2,}, \d{4}$", line):
                continue
            if " | " in line:
                match = re.match(
                    (
                        r"(?P<original_name>[^|]+) \| (?P<authority>[^,]+),"
                        r" (?P<year>\d{4})$"
                    ),
                    line,
                )
                assert match is not None, line
            else:
                match = NAME_LINE_RGX.match(line)
            if match:
                if current_name:
                    yield current_name
                current_name = {"pages": [page], **match.groupdict()}
            elif "verbatim_citation" not in current_name:
                start_label("verbatim_citation", line)
            elif line.startswith("Valid name: "):
                start_label("Valid name", line)
            elif line.startswith(" "):
                if "Valid name" in current_name:
                    match = re.match(r" +([^:(]+)( \([^\)]+\))?: ", line)
                    if match:
                        start_label(match.group(1).strip(), line)
                    else:
                        current_lines.append(line)
                else:
                    current_lines.append(line)
            else:
                current_lines.append(line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        name["specimen_detail"] = "\n".join(
            value for key, value in name.items() if "type" in key.lower()
        )
        name["taxon_name"] = " ".join(name["Valid name"].split()[2:4])

        for key, value in list(name.items()):
            if "type" not in key.lower():
                continue
            sgt = lib.extract_species_type_kind(key)
            if sgt is not None:
                name["species_type_kind"] = sgt
            if key not in ("Holotype", "Lectotype"):
                continue
            match = re.match(r"^(Holotype|Lectotype)( \([^\)]+\))?: (ZMB \d+)", value)
            if match:
                name["type_specimen"] = match.group(3)
            else:
                print(f"failed to match {value}")

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names, clean_labels=False)
    names = split_fields(names)
    names = lib.translate_to_db(names, "ZMB", SOURCE, verbose=False)
    names = lib.associate_names(names)
    # names = lib.write_to_db(
    #     names, SOURCE, dry_run=False, edit_if_no_holotype=True, always_edit=True
    # )
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
