import re
from typing import List

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("cuba.txt", "Cuba-type specimens.pdf")


COLLECTION_TO_LABEL = {
    "Instituto de Ecología y Sistemática": "IES",
    "Museo Nacional de Historia Natural de La Habana": "MNHNC",
    "Colección privada de Luis S. Varona": "Varona collection",
    "Colección particular de Óscar Arredondo de la Mata": "Arredondo collection",
    "Instituto de Geología y Paleontología": "IGP",
    "Facultad de Biología, Universidad de la Habana": "FBUH",
    "Colección Paleontológica, Museo Felipe Poey": "CPMFP",
}


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    current_page = None
    collection_of_name = None
    for page, lines in pages:
        for line in lines:
            line = line.strip()
            if re.match(r"^\d\. ", line):
                current_collection = line[3:]
                continue
            if (
                re.match(r"^\d\.\d\. ", line)
                or re.match(r"^[A-Z][a-z]+$", line)
                or not line
            ):
                continue
            elif line.startswith("ESPECIE"):
                if current_name:
                    yield {
                        "raw_text": current_name,
                        "pages": [current_page],
                        "collection_name": collection_of_name,
                    }
                current_name = [line]
                current_page = page
                collection_of_name = current_collection
            else:
                current_name.append(line)
    if current_name:
        yield {
            "raw_text": current_name,
            "pages": [current_page],
            "collection_name": collection_of_name,
        }


def split_names(names: DataT) -> DataT:
    for name in names:
        pieces = re.split(r"; (?=[A-Z]{3,}|No\. )", name["raw_text"])
        for piece in pieces:
            label, value = piece.split(": ", maxsplit=1)
            assert label not in name, (name, label)
            name[label] = value
        yield name


SPANISH_MONTHS = {
    "enero": "January",
    "febrero": "February",
    "marzo": "March",
    "abril": "April",
    "mayo": "May",
    "junio": "June",
    "julio": "July",
    "agosto": "August",
    "septiembre": "September",
    "octubre": "October",
    "noviembre": "November",
    "diciembre": "December",
}


def translate_spanish_date(date: str) -> str:
    date = date.replace(" de ", " ")
    for spanish, english in SPANISH_MONTHS.items():
        date = date.replace(spanish, english)
    return date


def split_fields(names: DataT) -> DataT:
    for name in names:
        if "AUTOR Y AÑO" not in name:
            continue
        name["original_name"] = re.sub(r" \(=.*\)", "", name["ESPECIE"]).strip()
        match = re.match(r"^(.*) \((\d+).*\)$", name["AUTOR Y AÑO"])
        assert match, f'failed to match {name["AUTOR Y AÑO"]}'
        name["authority"] = match.group(1)
        name["year"] = match.group(2)
        name["specimen_detail"] = name["TIPO"]
        if "LOCALIDAD TIPO" in name:
            name["loc"] = name["LOCALIDAD TIPO"]
        if "No. DE CATÁLOGO" in name:
            name["type_specimen"] = name["No. DE CATÁLOGO"]
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
        if name["collection_name"] in COLLECTION_TO_LABEL:
            name["collection"] = models.Collection.by_label(
                COLLECTION_TO_LABEL[name["collection_name"]]
            )
        if "FECHA DE RECOLECCIÓN" in name:
            name["date"] = translate_spanish_date(name["FECHA DE RECOLECCIÓN"])
        if "RECOLECTOR" in name:
            name["collector"] = name["RECOLECTOR"]
        if "RECOLECTORES" in name:
            name["collector"] = name["RECOLECTORES"]
        if "SEXO" in name:
            name["age_gender"] = name["SEXO"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_names(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, source=SOURCE, verbose=True)

    names = lib.associate_names(
        names,
        lib.NameConfig(
            authority_fixes={
                "Borroto et al.": "Borroto, Camacho & Ramos",
                "Woloszyn & Mayo": "Wołoszyn & Mayo",
            }
        ),
        start_at="Zazamys veronicae",
    )
    lib.write_to_db(names, SOURCE, dry_run=False, always_edit=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for n in main():
        print(n)
