import re
import sys
from collections.abc import Iterable
from typing import Any

from taxonomy.db import constants

from . import lib
from .lib import DataT

CS_RGX = re.compile(
    r"""
    ^(?P<number>[\d/]+)\.\s
    ((?P<body_parts>[^\.]+)\.\s(?P<gender_age>[^\.]+)\.\s)?
    (?P<loc>.+)\.\s
    (Collected|Received|Leg\.\s\(Collected\))
    (
        \s(?P<date>.*)\sby\s(?P<collector>.*)\.\s(Original\s[Nn]umbers?.+|No\soriginal\snumber.*)
        |.*
    )\.$
""",
    re.VERBOSE,
)


def extract_names(pages: Iterable[tuple[int, list[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    found_first = False
    current_name: dict[str, Any] | None = None
    current_label: str | None = None
    current_lines: list[str] = []
    in_headings = True

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert current_label is not None
        if label in current_name:
            if label in ("Syntype", "Type Locality"):
                label = f"Syntype {line}"
            assert (
                label not in current_name
            ), f"duplicate label {label} in {current_name}"
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            if not found_first:
                if line.strip() in ("TYPE SPECIMENS", "SPECIMENS"):
                    found_first = True
                continue
            # ignore family/genus headers
            if re.match(
                (
                    r"^\s*(Genus|Family|Subfamily|Suborder|Order) [A-Z][a-zA-Z]+"
                    r" [a-zA-Z\.’, \-]+(, \d{4})?$"
                ),
                line,
            ):
                in_headings = True
                continue
            # ignore blank lines
            if not line:
                continue
            if in_headings:
                if line.startswith(" "):
                    continue
                else:
                    in_headings = False
            if line.startswith(" "):
                current_lines.append(line)
            elif re.match(r"^[A-Z][A-Z a-z-]+: ", line):
                start_label(line.split(":")[0], line)
            elif line.startswith("Lectotype as designated"):
                start_label("Lectotype", line)
            elif line.startswith("Neotype as designated"):
                start_label("Neotype", line)
            elif line.startswith(
                (
                    "This specimen",
                    "Type ",
                    "No type",
                    "There are",
                    "No additional",
                    "All ",
                    "Subspecies of ",
                    "Neotype designated ",
                    "Padre Island",
                )
            ):
                start_label("comments", line)
            elif line.startswith(
                ("Secondary junior", "Primary junior", "Junior primary")
            ):
                start_label("homonymy", line)
            elif re.match(r"^[\d/]+\. ", line):
                start_label(line.split(".")[0], line)
            elif line.startswith("USNM"):
                start_label(line.split(".")[0], line)
            elif (
                current_label not in ("name", "verbatim_citation", "homonymy")
                and ":" not in line
            ):
                # new name
                if current_name is not None:
                    assert current_label is not None
                    current_name[current_label] = current_lines
                    assert any(
                        field in current_name
                        for field in (
                            "Holotype",
                            "Type Locality",
                            "Lectotype",
                            "Syntype",
                            "Syntypes",
                            "No name-bearing status",
                            "Neotype",
                        )
                    ), current_name
                    yield current_name
                current_name = {"pages": [page]}
                current_label = "name"
                current_lines = [line]
            elif current_label == "name":
                if re.search(
                    r"\d|\b[A-Z][a-z]+\.|\baus\b|\bDas\b|\bPreliminary\b|\., ", line
                ):
                    start_label("verbatim_citation", line)
                else:
                    # probably continuation of the author
                    current_lines.append(line)
            elif (
                current_label == "verbatim_citation"
                or current_label == "homonymy"
                or line.startswith("= ")
            ):
                start_label("synonymy", line)
            else:
                assert False, f"{line!r} with label {current_label}"
    assert current_label is not None
    assert current_name is not None
    current_name[current_label] = current_lines
    yield current_name


def split_fields(names: DataT) -> DataT:
    tried = succeeded = 0
    for name in names:
        name["raw_text"] = dict(name)
        name.update(lib.extract_name_and_author(name["name"]))
        if "Type Locality" in name:
            name["loc"] = name["Type Locality"]
        for field in "Holotype", "Lectotype", "Neotype":
            if field in name:
                tried += 1
                name["species_type_kind"] = constants.SpeciesGroupType[field.lower()]
                raw_data = data = name[field]
                data = re.sub(r"^as designated.*?: ", "", data)
                # TODO: handle field starting with "Lectotype as designated by ..."
                match = re.match(
                    (
                        r"^(USNM [\d/]+)\. ([^\.]+)\. ([^\.]+)\."
                        r" (Collected|Received|Leg\. \(Collected\)) (.*) by (.*)\."
                        r" (Original [Nn]umbers? .+|No original number.*)\.$"
                    ),
                    data,
                )
                if match is None:
                    data = re.sub(r"\[[^\]]+?: ([^\]]+)\]", r"\1", data)
                    data = re.sub(r"\[([\d/]+)\]", r"\1", data)
                    data = re.sub(r"\[([\d/]+\. [A-Za-z ,\.]+)\]", r"\1", data)
                    match = CS_RGX.match(data)
                    if match:
                        succeeded += 1
                        name["type_specimen"] = f'USNM {match.group("number")}'
                        for group_name in (
                            "body_parts",
                            "gender_age",
                            "loc",
                            "date",
                            "collector",
                        ):
                            group = match.group(group_name)
                            if group:
                                name[group_name] = group
                    else:
                        # print(f'failed to match {data!r}')
                        match = re.match(r"^((USNM |ANSP )?[\d/]+)", data)
                        if not match:
                            match = re.match(r"^([\d/]+)", data)
                            if match:
                                name["type_specimen"] = match.group(1)
                            else:
                                print(f"failed to match {data!r} at all")
                        else:
                            name["type_specimen"] = match.group(1)
                else:
                    succeeded += 1
                    name["type_specimen"] = match.group(1)
                    name["body_parts"] = match.group(2)
                    name["gender_age"] = match.group(3)
                    name["date"] = match.group(5)
                    name["collector"] = match.group(6)
                name["specimen_detail"] = raw_data
                break
        yield name
    print(f"succeeded in splitting field: {succeeded}/{tried}")


def translate_to_db(names: DataT, source: lib.Source) -> DataT:
    yield from lib.translate_to_db(names, "USNM", source)


def translate_type_localities(names: DataT) -> DataT:
    for name in names:
        if "loc" in name:
            text = name["loc"].rstrip(".")
            text = re.sub(r"\[.*?: ([^\]]+)\]", r"\1", text)
            text = text.replace("[", "").replace("]", "")
            parts: list[list[str]] = [
                list(filter(None, re.split(r"[()]", part))) for part in text.split(", ")
            ]
            type_loc = lib.extract_region(list(reversed(parts)))
            if type_loc is not None:
                name["type_locality"] = type_loc
            else:
                # print('could not extract type locality from', name['loc'])
                pass
        yield name


def main() -> DataT:
    if len(sys.argv) > 1 and sys.argv[1] == "cpac":
        source = lib.Source("usnmcpac-layout.txt", "Ferungulata-USNM types.pdf")
    elif len(sys.argv) > 1 and sys.argv[1] == "cs":
        source = lib.Source(
            "usnmcs-layout.txt", "Castorimorpha, Sciuromorpha-USNM types.pdf"
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "ahm":
        source = lib.Source(
            "usnmtypesahm-layout.txt",
            "Anomaluromorpha, Hystricomorpha, Myomorpha-USNM types.pdf",
        )
    else:
        assert len(sys.argv) == 1
        source = lib.Source(
            "usnmtypes-layout.txt", "USNM-types (Fisher & Ludwig 2015).pdf"
        )

    lines = lib.get_text(source)
    pages = lib.extract_pages(lines)
    pages = lib.align_columns(pages)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = translate_to_db(names, source)
    names = translate_type_localities(names)
    config = lib.NameConfig(
        {
            "Deleuil & Labbe": "Deleuil & Labbé",
            "Tavares, Gardner, Ramirez-Chaves & Velazco": (
                "Tavares, Gardner, Ramírez-Chaves & Velazco"
            ),
            "Miller & Allen": "Miller & G.M. Allen",
            "Robinson & Lyon": "W. Robinson & Lyon",
            "Goldman & Gardner": "Goldman & M.C. Gardner",
            "Miller.": "Miller",
            "Anderson & Gutierrez": "Anderson & Gutiérrez",
            "Garcia-Perea": "García-Perea",
            "Dalebout, Mead, Baker, Baker & van Helden": (
                "Dalebout, Mead, Baker, Baker & Van Helden"
            ),
            "Wilson Wilson et al.": "Wilson",
        },
        {
            "Tana tana besara": "Tupaia tana besara",
            "Arvicola (Pitymys) pinetorum quasiater": (
                "Arvicola (Pitymys) pinetorum var. quasiater"
            ),
            "Tamias asiaticus borealis": "Tamias asiaticus, var. borealis",
            "Tamias quadrivittatus pallidus": "Tamias quadrivittatus, var. pallidus",
            "Citellus washingtoni washingtoni": "Citellus washingtoni",
        },
    )
    names = lib.associate_names(names, config)
    lib.write_to_db(names, source, dry_run=False)
    # lib.print_counts(names, 'original_name')
    # lib.print_field_counts(names)
    list(names)
    return names


if __name__ == "__main__":
    for _ in main():
        print(_)
