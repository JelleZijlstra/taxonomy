import copy
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Tuple

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("zmmutypes.txt", "ZMMU-types.pdf")
ORDER = "Отряд"
SECTION_INTRO = ("ЛЕКТОТИП", "ГОЛОТИП", "ПАРАТИП", "СИНТИП", "ПАРАЛЕКТОТИП", "НЕОТИП")
LABEL = (
    "МЕСТО СБОРА",
    "L OCALITY",
    "LOCALITY",
    "ДАТА",
    "КОММЕНТАРИЙ",
    "COMMENT",
    "ПРИМЕЧАНИЕ",
    "NOTICE",
    "КОММЕНТАРИИ",
    "ПРИМЕЧАНИЯ",
)
NAME_LINE = re.compile(
    r"""
    ^(?P<original_name>[A-Z]\.?\[?[a-z]+\]?(\s\([A-Z][a-z]+\))?(\s[a-z\-\.\[\]]+)+)\s
    («[^»]+»\s)?
    (?P<latin_authority>[A-Z].+?),\s(?P<year>\d{4})\s
    \((?P<cyrillic_authority>[^:]+?),\s(?P<ref_year>\d{4}[а-яa-z]?):\s(?P<page_described>[^\)]+)\)\.?
    $
    """,
    re.VERBOSE,
)
KEY_TO_KIND = {
    "ЛЕКТОТИП": constants.SpeciesGroupType.lectotype,
    "ГОЛОТИП": constants.SpeciesGroupType.holotype,
    "НЕОТИП": constants.SpeciesGroupType.neotype,
}


def make_translation_table() -> Dict[str, str]:
    out = {}
    with open("data_import/data/zmmu-transcribe.txt") as f:
        for line in f:
            line = unicodedata.normalize("NFC", line.strip())
            if " " in line:
                cyrillic, transcribed = line.split()
                out[transcribed] = cyrillic
    return out


def translate_chars(lines: Iterable[str]) -> Iterable[str]:
    table = {ord(a): ord(b) for a, b in make_translation_table().items()}
    for line in lines:
        yield line.translate(table)


def extract_pages(lines: Iterable[str]) -> Iterable[List[str]]:
    """Split the text into pages."""
    current_lines: List[str] = []
    for line in lines:
        if line.startswith("\x0c"):
            yield current_lines
            current_lines = []
            line = line[1:]
        current_lines.append(line)
    yield current_lines


def label_pages(pages: Iterable[List[str]]) -> PagesT:
    for i, lines in enumerate(pages):
        if i < 164 or i > 240:
            continue  # Before mammal section
        for i in range(1, len(lines) + 1):
            if re.match(r"^\s+\d+\s+$", lines[-i]):
                page_number = int(lines[-i].strip())
                break
        yield page_number, lines[:-i]


def align_columns(pages: PagesT) -> PagesT:
    for page, lines in pages:
        lines = lib.dedent_lines(lines)
        try:
            lines = lib.split_lines(lines, page, min_column=15, dedent_right=False)
        except lib.NoSplitFound:
            # Separately split the parts before and after the "Order ..." line.
            for i, line in enumerate(lines):
                if line.lstrip().startswith(ORDER):
                    break
            else:
                assert False, f"could not find order in {page}"
            before = lines[:i]
            after = lines[i + 1 :]
            lines = lib.split_lines(before, page) + lib.split_lines(after, page)
        yield page, lines


def extract_names(pages: PagesT) -> DataT:
    current_name: Dict[str, Any] = {}
    current_section: Dict[str, Any] = {}
    current_lines: List[str] = []
    current_label = ""

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines, current_label
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        if current_section:
            container = current_section
        else:
            container = current_name
        assert label not in container, f"duplicate label {label} for {container}"
        current_lines = [line]
        container[label] = current_lines
        current_label = label

    def start_section(label: str, line: str) -> None:
        nonlocal current_section
        if label in current_name:
            section_label = line
        else:
            section_label = label
        # This one is repeated in the source, apparently by mistake.
        if section_label != "ПАРАТИП S-32814 Пол: ? Шкура в полной со-":
            assert (
                section_label not in current_name
            ), f"duplicate label {section_label} for {current_name}"
        current_section = {"label": section_label}
        current_name[section_label] = current_section
        start_label(label, line)

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if current_section or not current_name:
                if lib.initial_count(line, " ") > 3:
                    continue
            if current_label == "ДАТА" and re.search(r"[a-z], \d{4}\)?$", line):
                continue
            if re.match(r"^†?[a-z]+$", line):
                if current_name:
                    yield current_name
                current_name = {"pages": [page], "root_name": line}
                current_section = {}
                current_label = ""
            elif "name_line" not in current_name:
                start_label("name_line", line)
            elif line.startswith(SECTION_INTRO):
                start_section(line.split()[0], line)
            elif line.startswith(LABEL):
                for label in LABEL:
                    if line.startswith(label):
                        start_label(label, line)
            else:
                current_lines.append(line)
        if page == 228:
            break  # start of references
    yield current_name


def extract_references(pages: PagesT) -> Iterable[List[str]]:
    current_lines = []
    for _, lines in pages:
        for line in lines:
            if line.strip() == "ЛИТЕРАТУРА" or not line.strip():
                continue
            if line.startswith(" "):
                current_lines.append(line)
            else:
                if current_lines:
                    yield current_lines
                current_lines = [line]
    yield current_lines


def make_references_dict(refs: Iterable[List[str]]) -> Dict[Tuple[str, str], str]:
    out = {}
    for ref in refs:
        text = lib.clean_line_list(ref)
        if text == "The Times' Atlas of the World, 7th ed. London: Times Books, 1986.":
            continue
        match = re.match(r"^([^\d]+)(\d{4}(-\d+)?[^\.]?)\.", text)
        assert match, text
        year = match.group(2)
        authors = match.group(1)
        authority = ", ".join(a.split()[0] for a in authors.split(", ") if a)
        out[(authority, year)] = text
    return out


def handle_specimen(data: Dict[str, Any]) -> Dict[str, Any]:
    detail = data[data["label"].split()[0]]
    match = re.match(r"^(\(\?\) )?(S-\d+) Пол: (\??m\.|f\.|\?,?) (.*)$", detail)
    if not match:
        print(detail)
    else:
        data["type_specimen"] = f"ZMMU {match.group(2)}"
        data["gender_value"] = {
            "?m.": constants.SpecimenGender.male,
            "m.": constants.SpecimenGender.male,
            "f.": constants.SpecimenGender.female,
            "?": constants.SpecimenGender.unknown,
            "?,": constants.SpecimenGender.unknown,
        }[match.group(3)]
        rest = match.group(4)
        if "ювенильный" in rest:
            data["age"] = constants.SpecimenAge.juvenile
        data["body_parts"] = rest
    for label in ("LOCALITY", "L OCALITY"):
        if label in data:
            value = data[label]
            data["loc"] = value
            country = value.split()[-1].strip("«»[].")
            country = lib.NAME_SYNONYMS.get(country, country)
            try:
                data["type_locality"] = models.Region.get(
                    models.Region.name == country
                ).get_location()
            except models.Region.DoesNotExist:
                pass
    date_coll = data["ДАТА"]
    try:
        date, collector = date_coll.split(" КОЛЛ.: ", maxsplit=1)
    except ValueError:
        print(date_coll)
    else:
        if date != "?":
            data["date"] = date.rstrip(".")
        if collector != "?":
            data["collector"] = collector
    return data


def split_fields(names: DataT, refs_dict: Dict[Tuple[str, str], str]) -> DataT:
    for name in names:
        name["raw_text"] = copy.deepcopy(name)
        match = NAME_LINE.match(name["name_line"].replace(" [sic!]", ""))
        if not match:
            assert False, f'failed to match {name["name_line"]}'
        else:
            name.update(match.groupdict())
            name["authority"] = name["latin_authority"]
            name["original_name"] = re.sub(
                r"([a-zA-Z])\.\[([a-z]+)\] ", r"\1\2 ", name["original_name"]
            )
            refs_key = (name["cyrillic_authority"], name["ref_year"])
            if refs_key in refs_dict:
                name["verbatim_citation"] = refs_dict[refs_key]

        paratypes = []
        paralectotypes = []
        syntypes = []
        for key, value in list(name.items()):
            if key != "raw_text" and isinstance(value, dict):
                value = handle_specimen(value)
                if key.startswith("ПАРАТИП"):
                    paratypes.append(value)
                    del name[key]
                elif key.startswith("СИНТИП"):
                    syntypes.append(value)
                    del name[key]
                elif key.startswith("ПАРАЛЕКТОТИП"):
                    paralectotypes.append(value)
                    del name[key]
                elif key in KEY_TO_KIND:
                    name["species_type_kind"] = KEY_TO_KIND[key]
                    for subkey, subval in value.items():
                        if re.match(r"^[a-z_]+$", subkey):
                            name[subkey] = subval
        if paratypes:
            name["paratypes"] = paratypes
        if paralectotypes:
            name["paralectotypes"] = paralectotypes
        if syntypes:
            name["syntypes"] = syntypes
            name["species_type_kind"] = constants.SpeciesGroupType.syntypes
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    lines = translate_chars(lines)
    unlabeled_pages = extract_pages(lines)
    pages = label_pages(unlabeled_pages)
    pages = lib.validate_pages(pages, verbose=False)
    pages = align_columns(pages)
    names: DataT = list(extract_names(pages))
    refs = extract_references(pages)
    refs_dict = make_references_dict(refs)
    names = lib.clean_text(names)
    names = split_fields(names, refs_dict)
    names = lib.translate_to_db(names, "ZMMU", SOURCE, verbose=False)
    conf = lib.NameConfig(
        original_name_fixes={
            "Neomys fodiens brachyotis": "Neomys fodiens brachyotus",
            "Lepus mandshuricus sbph. melanotus": (
                "Lepus mandschuricus subphasa melanonotus"
            ),
            "Lepus timidus transbaikalensis": "Lepus timidus transbaicalicus",
            "Citellus (Urocitellus) eversmanni incertedens": (
                "Citellus (Urocitellus) eversmanni intercedens"
            ),
            "Gulo gulo camtshaticus": "Gulo gulo kamtschaticus",
            "A.[lticola] a.[rgentatus] tarasovi": "Alticola argentatus tarasovi",
            "Microtus oeconomus": "Microtus oeconomus naumovi",
            "Myotis emarginatus turcomanus": "Myotis emarginatus turcomanicus",
        },
        authority_fixes={
            "Vorontsov & Boyeskorov et al.": "Vorontsov, Boyeskorov & Mezhzherin",
            "Lavrenchenko, Likhnova, Baskevich & Bekele": (
                "Lavrenchenko, Likhnova & Baskevich"
            ),
            "Vorontsov, Boyeskorov & Lyapunova et al.": (
                "Vorontsov, Boyeskorov, Lyapunova & Revin"
            ),
        },
    )
    names = lib.associate_names(names, conf, max_distance=2)
    names = lib.write_to_db(names, SOURCE, dry_run=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
