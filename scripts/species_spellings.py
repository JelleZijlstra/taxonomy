from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass

from taxonomy import getinput
from taxonomy.db.constants import (
    AgeClass,
    NomenclatureStatus,
    Rank,
    SpeciesNameKind,
    Status,
)
from taxonomy.db.models.classification_entry.ce import ClassificationEntry
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.taxon.taxon import Taxon
from taxonomy.upsheeter import upsheet

SC_GROUPS = [
    (
        "ura",
        [3495, 6122, 6207],  # urus  # noun_in_apposition_ura  # noun_in_apposition_urus
    ),
    ("cauda", [148, 6034]),  # caudus  # noun_in_apposition_cauda
    ("otus", [3517, 6326]),  # otus  # noun_in_apposition_otus
    ("otis", [3409]),  # otis
    (
        "phyllum",
        [
            286,  # aphyllus
            1127,  # microphyllus
            3541,  # phyllus
            6037,  # noun_in_apposition_phylla
        ],
    ),
    ("venter", [3380, 3745, 6088]),  # venter  # ventris  # noun_in_apposition_venter
    ("manus", [6344]),  # manus
    ("gaster", [3366, 6087]),  # gaster  # noun_in_apposition_gaster
    ("notus", [3404]),  # notus
    ("cephalus", [3401]),  # cephalus
    ("dactylus", [3414]),  # dactylus
    ("rhynchus", [6206, 165]),  # noun_in_apposition_rhynchus  # rhynchus
    ("spinus", [4802]),  # spinus
    ("pterus", [3702]),  # pterus
]
SC_ID_TO_GROUP = {}
for group_name, ids in SC_GROUPS:
    for sc_id in ids:
        SC_ID_TO_GROUP[sc_id] = group_name


def get_extant_mammals() -> list[Taxon]:
    species = Taxon.select_valid().filter(
        Taxon.age.is_in((AgeClass.extant, AgeClass.recently_extinct)),
        Taxon.rank == Rank.species,
    )
    return [
        spec
        for spec in species
        if spec.base_name.status is Status.valid
        and spec.get_derived_field("class_").valid_name == "Mammalia"
    ]


def get_interesting_group(intname: "InterestingName") -> str | None:
    sc = intname.taxon.base_name.species_name_complex
    if sc is not None:
        return SC_ID_TO_GROUP.get(sc.id)
    return None


@dataclass
class InterestingName:
    taxon: Taxon
    reason: str

    def get_alternative_spellings(self) -> set[str]:
        spellings = set()
        if self.taxon.base_name.nomenclature_status is NomenclatureStatus.as_emended:
            original_epithet = self.taxon.base_name.corrected_original_name.split()[-1]
            spellings.add(original_epithet)
        ces = get_relevant_ces(self.taxon.base_name, boundary_year=None)
        for ce in ces:
            corrected_name = ce.get_corrected_name()
            if corrected_name:
                epithet = corrected_name.split()[-1]
                spellings.add(epithet)
        spellings.add(self.taxon.base_name.corrected_original_name.split()[-1])
        spellings.discard(self.taxon.base_name.root_name)
        return spellings

    def to_csv(self) -> dict[str, str]:
        current_epithet = self.taxon.valid_name.split()[-1]
        original_epithet = (self.taxon.base_name.corrected_original_name or "").split()[
            -1
        ]
        snc = self.taxon.base_name.species_name_complex
        if snc:
            kind = snc.kind
            if kind.is_patronym():
                name_kind = "eponym"
            elif kind is SpeciesNameKind.adjective:
                if (
                    not snc.masculine_ending
                    and not snc.feminine_ending
                    and not snc.neuter_ending
                ):
                    name_kind = "adjective (invariant)"
                else:
                    name_kind = f"adjective (-{snc.masculine_ending}/-{snc.feminine_ending}/-{snc.neuter_ending})"
            else:
                name_kind = kind.name
        else:
            name_kind = "unknown"
        genus = self.taxon.parent_of_rank(Rank.genus)
        if genus.base_name.name_complex:
            gender = genus.base_name.name_complex.gender.short_name
        else:
            gender = ""
        return {
            "valid_name": self.taxon.valid_name,
            "genus_gender": gender,
            "current_epithet": current_epithet,
            "alternative_spellings": ", ".join(
                sorted(self.get_alternative_spellings())
            ),
            "original_combination": self.taxon.base_name.original_name or "",
            "original_epithet": original_epithet,
            "authority": self.taxon.base_name.taxonomic_authority(),
            "year": str(self.taxon.base_name.numeric_year()),
            "species_name_complex": snc.label if snc else "",
            "name_kind": name_kind,
            "citation": (
                self.taxon.base_name.original_citation.cite()
                if self.taxon.base_name.original_citation
                else ""
            ),
            "reason": self.reason,
            "interesting_group": get_interesting_group(self) or "",
            "spelling_changed": "Y" if current_epithet != original_epithet else "",
        }


def get_relevant_ces(nam: Name, boundary_year: int | None) -> list[ClassificationEntry]:
    resolved = nam.resolve_variant()
    root_name = nam.root_name
    ces = []
    for possible_variant in Name.select_valid().filter(Name.taxon == nam.taxon):
        if possible_variant.resolve_name() == resolved:
            ces.extend(get_and_filter_ces(possible_variant, root_name, boundary_year))
    return ces


def get_and_filter_ces(
    nam: Name, root_name: str, boundary_year: int | None
) -> list[ClassificationEntry]:
    ces = nam.get_classification_entries()

    def is_interesting_name(corrected_name: str | None) -> bool:
        return corrected_name is not None and corrected_name.split()[-1] != root_name

    return [
        ce
        for ce in ces
        if (not ce.rank.is_synonym)
        and (boundary_year is None or ce.article.numeric_year() >= boundary_year)
        and is_interesting_name(ce.get_corrected_name())
    ]


def find_interesting_spellings(taxa: list[Taxon]) -> Iterable[InterestingName]:
    for taxon in taxa:
        reason = []
        if taxon.base_name.nomenclature_status is NomenclatureStatus.as_emended:
            original_epithet = taxon.base_name.corrected_original_name.split()[-1]
            reason.append(f"as emended from '{original_epithet}'")
        if (
            taxon.base_name.numeric_year() > 2000
            and taxon.base_name.root_name
            != taxon.base_name.corrected_original_name.split()[-1]
        ):
            reason.append(
                f"changed spelling in original description from '{taxon.base_name.corrected_original_name}' to '{taxon.base_name.root_name}'"
            )
        ces = get_relevant_ces(taxon.base_name, 2000)
        if ces:
            reason.append(
                f"changed spelling in {len(ces)} classification entries: {ces}"
            )
        yield InterestingName(taxon, "; ".join(reason))


def main() -> None:
    taxa = get_extant_mammals()
    interesting = list(find_interesting_spellings(taxa))
    dicts = [item.to_csv() for item in interesting]
    upsheet(
        sheet_name="interesting_spellings",
        worksheet_gid=1765348686,
        data=dicts,
        matching_column="valid_name",
        backup_path_name="species_spellings",
    )
    group_to_pairs = {}
    for label, scs in SC_GROUPS:
        names = [
            (intname, csv_dict)
            for intname, csv_dict in zip(interesting, dicts, strict=True)
            if intname.taxon.base_name.species_name_complex is not None
            and intname.taxon.base_name.species_name_complex.id in scs
        ]
        group_to_pairs[label] = names

    for label, pairs in sorted(group_to_pairs.items()):
        getinput.print_header(f"{label} ({len(pairs)} names)")
        kinds: Counter[str] = Counter()
        for _, csv_dict in pairs:
            key = f"original_ending={csv_dict['original_epithet'][-2:]} genus={csv_dict['genus_gender']} current_ending={csv_dict['current_epithet'][-2:]}"
            kinds[key] += 1
        for kind, num_instances in kinds.most_common():
            print(f"{num_instances:3d} {kind}")

    column_to_values: dict[str, Counter[str]] = {}
    for csv_dict in dicts:
        for k, v in csv_dict.items():
            if k not in column_to_values:
                column_to_values[k] = Counter()
            column_to_values[k][v] += 1
    getinput.print_header("All columns")
    for column, counter in sorted(column_to_values.items()):
        if len(counter) < 20:
            print(f"{column:20s}: ", end="")
            for value, count in counter.most_common():
                print(f"{count:3d} '{value}'", end="; ")
            print()


if __name__ == "__main__":
    main()
