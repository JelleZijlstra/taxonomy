import csv
from collections.abc import Iterable
from pathlib import Path
from typing import TypedDict

from taxonomy import getinput
from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models import Article, ClassificationEntry, Name, Taxon
from taxonomy.db.models.name.name import NameTag
from taxonomy.upsheeter import upsheet

MSW3 = 9291  # article ID
MAMMALIA = 1011  # taxon ID


class Row(TypedDict):
    key: str
    order: str
    family: str
    msw3_genus: str
    current_genus: str
    count: str
    species_changed: str
    type_of_change: str
    mdd_taxonomic_comments: str
    mdd_taxonomic_references: str


def taxa_in_msw3(rank: Rank) -> Iterable[ClassificationEntry]:
    art = Article(MSW3)
    for child in art.get_children():
        for ce in child.get_classification_entries():
            if ce.rank == rank:
                yield ce


def taxa_in_current(rank: Rank) -> Iterable[Taxon]:
    taxon = Taxon(MAMMALIA)
    for child in taxon.children_of_rank(rank):
        if (
            child.age in (AgeClass.extant, AgeClass.recently_extinct)
            and child.base_name.status is Status.valid
        ):
            yield child


def open_mdd() -> dict[str, dict[str, str]]:
    mdd_path = "notes/mdd/v2.3/MDD_v2.3_6836species.csv"
    mdd_data = {}
    with Path(mdd_path).open() as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            key = row["sciName"]
            mdd_data[key] = row
    return mdd_data


def _open_batnames() -> list[str]:
    batnames_path = "notes/batnames/list1.txt"
    with Path(batnames_path).open() as f:
        lines = []
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                genus, species, *_ = line.split()
                lines.append(f"{genus} {species}")
    return lines


def resolve_name(name: Name) -> Name:
    return name.resolve_name(exclude=(NameTag.MisidentificationOf,))


def compare_genera() -> None:
    msw3_genera_ces = list(taxa_in_msw3(Rank.genus))
    current_genera_taxa = list(taxa_in_current(Rank.genus))

    msw3_genera_names = {
        resolve_name(ce.mapped_name): ce
        for ce in msw3_genera_ces
        if ce.mapped_name is not None
    }
    assert len(msw3_genera_names) == len(
        msw3_genera_ces
    ), "Some MSW3 genera have no mapped name or are duplicates"

    current_genera_names = {
        resolve_name(taxon.base_name): taxon for taxon in current_genera_taxa
    }
    assert len(current_genera_names) == len(
        current_genera_taxa
    ), "Some current genera are duplicates"

    shared_genera = set(msw3_genera_names.keys()) & set(current_genera_names.keys())
    # new_genera = set(current_genera_names.keys()) - set(msw3_genera_names.keys())
    # lost_genera = set(msw3_genera_names.keys()) - set(current_genera_names.keys())

    getinput.print_header("Genus statistics")
    print("Number of genera in MSW3:", len(msw3_genera_names))
    print("Number of genera in current taxonomy:", len(current_genera_names))
    print("Number of shared genera:", len(shared_genera))


def compare_species() -> None:
    msw3_species_ces = list(taxa_in_msw3(Rank.species))
    current_species_taxa = list(taxa_in_current(Rank.species))

    msw3_species_names = {
        resolve_name(ce.mapped_name): ce
        for ce in msw3_species_ces
        if ce.mapped_name is not None
    }
    assert len(msw3_species_names) == len(
        msw3_species_ces
    ), "Some MSW3 species have no mapped name or are duplicates"

    current_species_names = {
        resolve_name(taxon.base_name): taxon for taxon in current_species_taxa
    }
    assert len(current_species_names) == len(
        current_species_taxa
    ), "Some current species are duplicates"

    shared_species = set(msw3_species_names.keys()) & set(current_species_names.keys())

    new_species = set(current_species_names.keys()) - set(msw3_species_names.keys())
    lost_species = set(msw3_species_names.keys()) - set(current_species_names.keys())

    names_to_msw3_species: dict[Name, ClassificationEntry] = dict(msw3_species_names)
    for rank in (Rank.subspecies, Rank.synonym_species):
        for ce in taxa_in_msw3(rank):
            if ce.mapped_name is not None:
                species_ce = ce.parent_of_rank(Rank.species)
                if species_ce is not None:
                    names_to_msw3_species[resolve_name(ce.mapped_name)] = species_ce
                else:
                    names_to_msw3_species[resolve_name(ce.mapped_name)] = ce

    new_species_with_msw3_info = new_species & set(names_to_msw3_species.keys())
    # fully_new_species = new_species - new_species_with_msw3_info

    getinput.print_header("Species statistics")
    print("Number of species in MSW3:", len(msw3_species_names))
    print("Number of species in current taxonomy:", len(current_species_names))
    print("Number of shared species:", len(shared_species))

    print("Number of new species in current taxonomy:", len(new_species))
    print(
        "Number of new species with MSW3 info (as subspecies or synonyms):",
        len(new_species_with_msw3_info),
    )

    # key is (MSW3 genus, current genus)
    key_to_changed_taxa: dict[
        tuple[str, str], tuple[Taxon, list[tuple[ClassificationEntry, Name]]]
    ] = {}
    for name in shared_species:
        msw3_ce = msw3_species_names[name]
        current_taxon = current_species_names[name]

        msw3_genus_ce = msw3_ce.parent_of_rank(Rank.genus)
        current_genus_taxon = current_taxon.parent_of_rank(Rank.genus)

        if msw3_genus_ce is None or current_genus_taxon is None:
            continue

        msw3_genus_name = msw3_genus_ce.name
        current_genus_name = current_genus_taxon.valid_name

        if msw3_genus_name != current_genus_name:
            key = (msw3_genus_name, current_genus_name)
            key_to_changed_taxa.setdefault(key, (current_genus_taxon, []))[1].append(
                (msw3_ce, name)
            )

    for name in new_species_with_msw3_info:
        if name not in names_to_msw3_species:
            continue
        msw3_ce = names_to_msw3_species[name]
        current_taxon = current_species_names[name]

        msw3_genus_ce = msw3_ce.parent_of_rank(Rank.genus)
        current_genus_taxon = current_taxon.parent_of_rank(Rank.genus)

        if msw3_genus_ce is None or current_genus_taxon is None:
            continue

        msw3_genus_name = msw3_genus_ce.name
        current_genus_name = current_genus_taxon.valid_name

        if msw3_genus_name != current_genus_name:
            key = (msw3_genus_name, current_genus_name)
            key_to_changed_taxa.setdefault(key, (current_genus_taxon, []))[1].append(
                (msw3_ce, name)
            )

    for name in lost_species:
        msw3_ce = msw3_species_names[name]
        msw3_genus_ce = msw3_ce.parent_of_rank(Rank.genus)
        assert msw3_ce.mapped_name is not None
        try:
            current_genus_taxon = msw3_ce.mapped_name.taxon.parent_of_rank(Rank.genus)
        except ValueError:
            continue
        if msw3_genus_ce is None:
            continue
        msw3_genus_name = msw3_genus_ce.name
        if msw3_genus_name == current_genus_taxon.valid_name:
            continue
        key = (msw3_genus_name, current_genus_taxon.valid_name)
        key_to_changed_taxa.setdefault(key, (current_genus_taxon, []))[1].append(
            (msw3_ce, msw3_ce.mapped_name)
        )

    getinput.print_header("Genus changes for shared species")
    rows: list[Row] = []

    all_msw3_genera = {ce.name for ce in taxa_in_msw3(Rank.genus)}
    all_current_genera = {taxon.valid_name for taxon in taxa_in_current(Rank.genus)}
    mdd = open_mdd()

    for (msw3_genus, current_genus), (current_genus_taxon, taxa_pairs) in sorted(
        key_to_changed_taxa.items()
    ):

        match (msw3_genus in all_current_genera, current_genus in all_msw3_genera):
            case (True, True):
                change_type = "reassignment"
            case (True, False):
                change_type = "split"
            case (False, True):
                change_type = "lump"
            case (False, False):
                change_type = "genus renaming"

        print(
            f"[{change_type}] MSW3 genus: {msw3_genus} -> Current genus: {current_genus} ({len(taxa_pairs)} species)"
        )
        for msw3_ce, current_name in taxa_pairs:
            print(
                f"  Species: {msw3_ce.name} -> {current_name.taxon.parent_of_rank(Rank.species).valid_name}"
            )

        mdd_comments: set[str] = set()
        mdd_references: set[str] = set()
        for _, current_name in taxa_pairs:
            mdd_entry = mdd.get(
                current_name.taxon.parent_of_rank(Rank.species).valid_name.replace(
                    " ", "_"
                )
            )
            if mdd_entry:
                comment = mdd_entry.get("taxonomyNotes", "").strip()
                reference = mdd_entry.get("taxonomyNotesCitation", "").strip()
                if comment:
                    mdd_comments.add(comment)
                if reference:
                    mdd_references.update(
                        ref.strip() for ref in reference.split("|") if ref.strip()
                    )

        row = Row(
            key=f"{msw3_genus}>{current_genus}",
            order=(
                current_genus_taxon.parent_of_rank(Rank.order).valid_name
                if current_genus_taxon.parent_of_rank(Rank.order)
                else ""
            ),
            family=(
                current_genus_taxon.parent_of_rank(Rank.family).valid_name
                if current_genus_taxon.parent_of_rank(Rank.family)
                else ""
            ),
            msw3_genus=msw3_genus,
            current_genus=current_genus,
            count=str(len(taxa_pairs)),
            species_changed=", ".join(
                (
                    f"{ce.name} -> {name}"
                    if ce.rank is Rank.species
                    else f"{ce.name} (under {spec.name if (spec := ce.parent_of_rank(Rank.species)) else 'unknown'}) -> {name}"
                )
                for ce, name in taxa_pairs
            ),
            type_of_change=change_type,
            mdd_taxonomic_comments="|".join(sorted(mdd_comments)),
            mdd_taxonomic_references="|".join(sorted(mdd_references)),
        )
        rows.append(row)

    with Path("genus_changes.csv").open("w") as csvfile:
        fieldnames = list(Row.__annotations__.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    upsheet(
        sheet_name="genus_changes",
        worksheet_gid=814555194,
        data=[dict(row) for row in rows],  # type: ignore[arg-type]
        matching_column="key",
        backup_path_name="genus_changes",
    )


def main() -> None:
    compare_genera()
    compare_species()


if __name__ == "__main__":
    main()
