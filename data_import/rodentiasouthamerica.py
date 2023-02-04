import itertools
import re
from collections.abc import Iterable

from taxonomy import shell
from taxonomy.db import constants

from . import lib
from .lib import DataT

SOURCE = lib.Source("rsam.txt", "Rodentia South America.pdf")
RefsDictT = dict[tuple[str, str], str]


def realign_lines(
    pages: Iterable[tuple[int, list[str]]]
) -> Iterable[tuple[int, list[str]]]:
    for page, lines in pages:
        initial_space = min(
            (
                lib.initial_count(line, " ")
                for line in lines
                if line and not line.isspace()
            ),
            default=0,
        )
        yield page, [line[initial_space:] for line in lines]


def extract_names(pages: Iterable[tuple[int, list[str]]]) -> DataT:
    current_lines: list[str] = []
    current_pages: list[int] = []
    in_synonymy = False
    found_references = False
    last_author = ""
    for page, lines in pages:
        if current_pages:
            current_pages.append(page)
        for line in lines:
            if found_references:
                if not line:
                    continue
                elif re.match(r"^ +\. \d{4}", line):
                    line = re.sub(r"^ +\.", last_author, line)

                if line.startswith(" "):
                    assert current_lines
                    current_lines.append(line)
                else:
                    if current_lines:
                        yield {
                            "raw_text": current_lines,
                            "pages": current_pages,
                            "t": 2,
                        }
                        current_lines = []
                    current_lines = [line]
                    current_pages = [page]
                    last_author = re.sub(r" \[?\d{4}[a-z]?\]?\..*$", "", line)
            else:
                if not line:
                    pass
                elif line.strip() == "Literature Cited":
                    found_references = True
                elif line.replace(" ", "").startswith(("synonym:", "synonyms:")):
                    in_synonymy = True
                elif not in_synonymy:
                    pass
                elif re.match(
                    (
                        r" +([a-z] ){5,}| +This subspecies| +This is|KEY TO| +The |"
                        r" +Endemic to| +Additional| +Distribution:| +Although| +In"
                        r" South| +A gray| +Known primarily|Map \d+"
                    ),
                    line,
                ):
                    in_synonymy = False
                elif re.search(
                    r"^[A-Z][a-z]+ [a-z]+ \(?[A-ZÉ][\. a-zA-Z\-]+, \d{4}\)?$", line
                ):
                    in_synonymy = False
                elif line.startswith(" "):
                    assert current_lines
                    current_lines.append(line)
                else:
                    if current_lines:
                        yield {
                            "raw_text": current_lines,
                            "pages": current_pages,
                            "t": 1,
                        }
                        current_lines = []
                    current_lines = [line]
                    current_pages = [page]


def build_refs_dict(refs: DataT) -> RefsDictT:
    refs_dict: RefsDictT = {}
    last_authors: str = ""
    for ref in refs:
        text = ref["raw_text"]
        match = re.match(
            r"^(.*?)[,\.]\'? \[?(\d+(–\d+)?[a-z]?)\]?( [\[\(]\d{4}[\]\)])?\.", text
        )
        assert match, f"failed to match {text}"
        year = match.group(2)
        raw_authors = match.group(1)
        if raw_authors == "———":
            raw_authors = last_authors
        else:
            last_authors = raw_authors
        if " and " not in raw_authors:
            authors = re.sub(r", .*$", "", raw_authors)
            if authors in (
                "Allen",
                "Davis",
                "Geoffroy St.-Hilaire",
                "Fischer",
                "Gervais",
                "Peters",
                "Nelson",
                "Howell",
                "LeConte",
                "Miranda-Ribeiro",
                "Lima",
                "Smith",
                "Anderson",
                "Vieira",
                "Peterson",
                "Johnson",
                "Shaw",
                "Peale",
                "Owen",
                "Cuvier",
                "Carter",
                "Brown",
                "Contreras",
                "Freitas",
                "Gardner",
            ):
                authors = (
                    re.sub(r"^(.+), (.*)$", r"\2. \1", raw_authors)
                    .replace(" da ", " ")
                    .replace(" de. ", " ")
                )
        else:
            authors = re.sub(r"( [A-Z]\.)+,", "", raw_authors)
            authors = re.sub(r",( [A-Z]\.)+", ",", authors)
            authors = re.sub(r", and( [A-Z]\.)+", " and", authors)
            authors = re.sub(r", (.*) and ", r", \1, and ", authors)
        authors = authors.replace(", Jr", "")
        if "———" in text:
            text = text.replace("———", raw_authors)
        assert (authors, year) not in refs_dict, (
            f"duplicate key ({authors!r}, {year!r}) (new: {text}, existing:"
            f" {refs_dict[(authors, year)]}"
        )
        refs_dict[(authors, year)] = text
    # for key, value in refs_dict.items():
    #     print(key)
    #     print(value)
    return refs_dict


def split_text(names: DataT) -> DataT:
    for name in names:
        # (?P<original_name>\[?[A-Z].*( [a-z-\[\],]{3,})):? (?P<authority>(de )?[A-Z].*?)
        match = re.match(
            (
                r"^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?):"
                r" ?(?P<page_described>[^;]+?)(, (?=type locality )|; )(?P<rest>.*)$"
            ),
            name["raw_text"],
        )
        if not match:
            match = re.match(
                (
                    r"^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?)[:,;]"
                    r" ?(?P<page_described>\d+)([;,:] (?P<rest>.*)|\.)$"
                ),
                name["raw_text"],
            )
            if not match:
                match = re.match(
                    (
                        r"^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?)([:,]"
                        r" ?(?P<page_described>\d+))?; (?P<rest>.*)$"
                    ),
                    name["raw_text"],
                )
                if not match:
                    continue
        name.update(match.groupdict())
        name_authority = name["name_authority"].replace("’", "'")
        if ": " in name_authority:
            name["original_name"], name["authority"] = name_authority.split(": ")
            name["has_colon"] = True
        else:
            name.update(split_name_authority(name_authority, try_harder=True))
        name["original_name"] = name["original_name"].replace("[", "").replace("]", "")
        if "original_name" in name:
            if any(
                s in name["original_name"]
                for s in ('"', "sp.", "species", "var. γ", "Var. a.", "spec.")
            ):
                name["is_informal"] = True
        yield name


def split_name_authority(
    name_authority: str, *, try_harder: bool = False, quiet: bool = False
) -> dict[str, str]:
    name_authority = re.sub(
        r"([A-Za-z][a-z]*)\[([a-z?]+( \([A-Z][a-z]+\))?)\]\.", r"\1\2", name_authority
    )
    name_authority = re.sub(r"([A-Z][a-z]*)\[([a-z]+)\]", r"\1\2", name_authority)
    name_authority = re.sub(r"^\[([A-Z][a-z]+)\]", r"\1", name_authority)
    name_authority = re.sub(r"\[\([A-Z][a-z]+\)\] ", r"", name_authority)
    name_authority = re.sub(
        r"^\[[A-Z][a-z]+ \(\]([A-Z][a-z]+)\[\)\]", r"\1", name_authority
    )
    regexes = [
        (
            r"^(?P<original_name>[A-ZÑ][a-zëöiï]+)"
            r" (?P<authority>(d\')?[A-ZÁ][a-zA-Z\-öáñ\.èç]+)$"
        ),
        (
            r"^(?P<original_name>[A-ZÑ][a-zëöiï]+( \([A-Z][a-z]+\))?( [a-z]{3,}){1,2})"
            r" (?P<authority>(d\'|de la )?[A-ZÁ][a-zA-Z\-öáéèíñç\.,\' ]+)$"
        ),
        r"^(?P<original_name>.*?) (?P<authority>[A-ZÉ]\.[\- ].*)$",
        (
            r"^(?P<original_name>[A-ZÑ][a-zëöíï]+) (?P<authority>(d\'|de la"
            r" )?[A-ZÁ][a-zA-Z\-öáéíñ\., ]+ and [A-ZÁ][a-zA-Z\-öáéèíñç]+)$"
        ),
    ]
    if try_harder:
        regexes += [
            r"^(?P<original_name>[a-z]+) (?P<authority>[A-Z].*? and [A-Z].*)$",
            r"^(?P<original_name>.* [a-zë\-]+) (?P<authority>[A-ZÁÉ].*)$",
            r"^(?P<original_name>.*) (?P<authority>[^ ]+)$",
        ]
    for rgx in regexes:
        match = re.match(rgx, name_authority)
        if match:
            return match.groupdict()
    if not quiet:
        print(name_authority)
    return {}


def split_fields(names: DataT, refs_dict: RefsDictT) -> DataT:
    for name in names:
        if "has_colon" in name or "is_informal" in name:
            continue  # we're not interested in name combinations
        text = name["rest"]
        if text:
            text = text.rstrip(".")
            if text == "part" or text.startswith("part; not "):
                continue
            if text == "nomen nudum":
                name["nomenclature_status"] = constants.NomenclatureStatus.nomen_nudum
            match = re.search(
                r"(preoccupied by|incorrect subsequent spelling or invalid emendation"
                r" of|incorrect subsequent spelling( of)?(, but not)?|"
                r"unjustified emendation of|replacement name for|lapsus calami for) "
                r"([^;\d=]+?)(, \d{4}|;|$| \(preoccupied\)|, on the assumption|\(but to"
                r" be|\(see Remarks|\[preoccupied by|\(preoccupied by)",
                text,
            )
            if match:
                name["variant_kind"] = {
                    "preoccupied by": constants.NomenclatureStatus.preoccupied,
                    "lapsus calami for": (
                        constants.NomenclatureStatus.incorrect_subsequent_spelling
                    ),
                    "incorrect subsequent spelling": (
                        constants.NomenclatureStatus.incorrect_subsequent_spelling
                    ),
                    "incorrect subsequent spelling of": (
                        constants.NomenclatureStatus.incorrect_subsequent_spelling
                    ),
                    "incorrect subsequent spelling of, but not": (
                        constants.NomenclatureStatus.incorrect_subsequent_spelling
                    ),
                    "unjustified emendation of": (
                        constants.NomenclatureStatus.unjustified_emendation
                    ),
                    "replacement name for": constants.NomenclatureStatus.nomen_novum,
                    "incorrect subsequent spelling or invalid emendation of": (
                        constants.NomenclatureStatus.variant
                    ),
                }[match.group(1)]
                name_authority = split_name_authority(
                    match.group(4), quiet=True, try_harder=True
                )
                if name_authority:
                    name["variant_name"] = name_authority["original_name"]
                    name["variant_authority"] = name_authority["authority"]
                else:
                    name["variant_name_author"] = match.group(2)

            if text.startswith("type locality"):
                name["loc"] = text[len("type locality ") :]
            elif text.startswith("type localities"):
                name["loc"] = text[len("type localities ") :]
            elif "localit" in text:
                name["loc"] = text
            elif text.startswith("type species"):
                match = re.match(
                    (
                        r"type species (?P<type_name>.*?)( \([^\)]+\))?(,"
                        r" (?P<type_year>\d{4}[a-z]?))?, ?by (?P<type_kind>.*)(;|$)"
                    ),
                    text,
                )
                if match:
                    name_authority = split_name_authority(match.group(1), quiet=True)
                    if name_authority:
                        name["type_name"] = name_authority["original_name"]
                        name["type_authority"] = name_authority["authority"]
                    else:
                        name["type_name_author"] = match.group("type_name")
                    if match.group("type_year"):
                        name["type_year"] = match.group("type_year")
                    type_kind = match.group("type_kind")
                    if type_kind == "monotypy":
                        name["genus_type_kind"] = (
                            constants.TypeSpeciesDesignation.monotypy
                        )
                    elif type_kind == "original designation":
                        name["genus_type_kind"] = (
                            constants.TypeSpeciesDesignation.original_designation
                        )
                    elif type_kind == "tautonymy":
                        name["genus_type_kind"] = (
                            constants.TypeSpeciesDesignation.absolute_tautonymy
                        )
                else:
                    name["raw_type_species"] = text
                name["verbatim_type"] = text

        if not any(
            field in name
            for field in (
                "variant_kind",
                "loc",
                "raw_type_species",
                "type_year",
                "nomenclature_status",
            )
        ):
            pass  # print(text)

        if "authority" in name and "year" in name:
            key = name["authority"], name["year"]
            if key in refs_dict:
                name["verbatim_citation"] = refs_dict[key]
        match = re.match(r"^[A-Z][a-z]+ \(([A-Z][a-z]+)\)$", name["original_name"])
        if match:
            name["original_name"] = match.group(1)

        name["year"] = re.sub(r"[a-z]+$", "", name["year"])
        name["authority"] = lib.unspace_initials(name["authority"])
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = realign_lines(pages)
    pages = lib.align_columns(pages)
    names_refs = extract_names(pages)
    names_refs = lib.clean_text(names_refs)
    names: DataT = list(itertools.takewhile(lambda n: n["t"] == 1, names_refs))
    refs = names_refs
    refs_dict = build_refs_dict(refs)
    shell.ns["refs_dict"] = refs_dict
    names = split_text(names)
    # for key, text, aut in sorted(split_fields(names, refs_dict)):
    #     print(key, text, aut)
    names = split_fields(names, refs_dict)
    names = lib.translate_to_db(names, "UU", SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    config = lib.NameConfig(
        original_name_fixes={
            "Oryzomys lugens": "Oryzomys? lugens",
            "couesi": "Tylomys couesi",
            "macrurus": "Mus (Hesperomys) macrurus",
            "sciureus": "Kerodon sciureus",
            "viscacia": "Lepus viscacia",
            "sericeus": "Ctenomys sericeus",
            "albispinus": "Echimys albispinus",
            "coypus": "Mus coypus",
            "cyanus": "Mus cyanus",
            "S. Roberti": "Sciurus Roberti",
            "dabbenei": "Euneomys dabbenei",
            "inca": "Oxymycterus inca",
            "macconnelli de": "Rhipidomys macconnelli",
            "venezuelae": "Rhipidomys venezuelae",
            "pallidior": "Kerodon niata pallidior",
            "mexianae": "Coelogenys paca mexianae",
        },
        authority_fixes={
            "Zimmerman": "Zimmermann",
            "Menegaux": "Ménègaux",
            "WiedNeuwied": "Wied-Neuwied",
            "I. Geoffroy St.Hilaire": "I. Geoffroy Saint-Hilaire",
            "I. Geoffroy St.-Hilaire": "I. Geoffroy Saint-Hilaire",
            "É. Geoffroy St.Hilaire": "É. Geoffroy Saint-Hilaire",
            "É. Geoffroy St.-Hilaire": "É. Geoffroy Saint-Hilaire",
            "Quay and Gaimard": "Quoy & Gaimard",
            "Albuja & Gardner": "Albuja V. & Gardner",
            "Muñoz, Cuartas & González": "Muñoz, Cuartas-Calle & González",
            "Muchhala, Mena & Albuja": "Muchhala, Mena V. & Albuja V.",
            "F. Cunha & Cruz": "Souza Cunha & Cruz",
            "E.-L. Trouessart": "Trouessart",
        },
        ignored_names={
            ("Sciurus pusillus Desmarest, 1817 (= Sciurus pusillus", "Cuvier"),
            ("Saccomys anthophile", "Cuvier"),
            ("Habrothrix lasiotis", "Wagner"),
            ('the "Moco" of', "Molina"),
            ("Agouti paca", "Cuvier"),
            ("Cavia paca", "Cuvier"),
            ("Sciurus pusillus", "Desmarest"),
        },
    )
    names = lib.associate_types(names, config, quiet=True)
    names = lib.associate_variants(names, config, quiet=True)
    names = lib.associate_names(
        names,
        config,
        max_distance=2,
        try_manual=True,
        start_at="Sciurus granatensis agricolae",
    )
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # for name in names:
    #     print(name)
    lib.print_field_counts(names)
    # print(list(refs_dict.keys()))
    print(f"{len(refs_dict)} refs")


if __name__ == "__main__":
    main()
