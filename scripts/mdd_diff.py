import argparse
import csv
import enum
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TypedDict

from taxonomy.db import helpers
from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models import Taxon
from taxonomy.db.models.tags import TaxonTag

INCLUDED_AGES = (AgeClass.extant, AgeClass.recently_extinct)


class MddRow(TypedDict):
    sciName: str
    id: str
    phylosort: str
    mainCommonName: str
    otherCommonNames: str
    subclass: str
    infraclass: str
    magnorder: str
    superorder: str
    order: str
    suborder: str
    infraorder: str
    parvorder: str
    superfamily: str
    family: str
    subfamily: str
    tribe: str
    genus: str
    subgenus: str
    specificEpithet: str
    authoritySpeciesAuthor: str
    authoritySpeciesYear: str
    authorityParentheses: str
    originalNameCombination: str
    authoritySpeciesCitation: str
    authoritySpeciesLink: str
    holotypeVoucher: str
    holotypeVoucherURIs: str
    typeLocality: str
    typeLocalityLatitude: str
    typeLocalityLongitude: str
    nominalNames: str
    taxonomyNotes: str
    taxonomyNotesCitation: str
    distributionNotes: str
    distributionNotesCitation: str
    subregionDistribution: str
    countryDistribution: str
    continentDistribution: str
    biogeographicRealm: str
    iucnStatus: str
    extinct: str
    domestic: str
    flagged: str
    CMW_sciName: str
    diffSinceCMW: str
    MSW3_matchtype: str
    MSW3_sciName: str
    diffSinceMSW3: str


class DifferenceKind(enum.Enum):
    higher_classification = 1
    genus_assignment = 2
    spelling = 3
    authority = 4
    year = 5
    original_name = 6
    type_specimen = 7
    extinction_status = 8
    missing_in_mdd = 9
    missing_in_hesperomys = 10
    invalid_mdd_id = 11
    type_specimen_missing_mdd = 12
    type_specimen_missing_hesp = 13
    original_name_missing_mdd = 14
    original_name_missing_hesp = 15


@dataclass
class Difference:
    kind: DifferenceKind
    comment: str | None = None
    mdd: str | None = None
    hesp: str | None = None
    mdd_id: str | None = None
    taxon: Taxon | None = None

    def to_markdown(self) -> str:
        parts = []
        if self.kind is DifferenceKind.missing_in_hesperomys:
            parts.append(f"Missing in Hesperomys: _{self.mdd}_")
        elif self.kind is DifferenceKind.missing_in_mdd:
            parts.append(f"Missing in MDD: _{self.hesp}_")
        else:
            parts.append(
                f"{self.mdd or '(none)'} (MDD) vs. {self.hesp or '(none)'} (Hesperomys)"
            )
        parentheticals = []
        if self.mdd_id:
            parentheticals.append(f"MDD#{self.mdd_id}")
        if self.taxon:
            parentheticals.append(
                f"[{self.taxon.valid_name}](http://hesperomys.com/t/{self.taxon.id})"
            )
        if self.comment:
            parentheticals.append(self.comment)
        parentheticals.append(self.kind.name)
        parts.append(f" ({'; '.join(parentheticals)})")
        return "".join(parts)

    def to_csv(self) -> dict[str, str]:
        return {
            "kind": self.kind.name,
            "comment": self.comment or "",
            "mdd": self.mdd or "",
            "hesp": self.hesp or "",
            "mdd_id": self.mdd_id or "",
            "taxon": str(self.taxon.id) if self.taxon else "",
        }


def all_species(taxon: Taxon) -> Iterable[Taxon]:
    if taxon.age not in INCLUDED_AGES:
        return
    if taxon.rank is Rank.species:
        if taxon.base_name.status is Status.valid:
            yield taxon
    else:
        for child in Taxon.add_validity_check(
            taxon.children.filter(Taxon.age << INCLUDED_AGES)
        ):
            yield from all_species(child)


def get_mdd_id(taxon: Taxon) -> str | None:
    for tag in taxon.get_tags(taxon.tags, TaxonTag.MDD):
        return tag.id
    return None


_rank_cache: dict[tuple[Taxon, Rank], Taxon | None] = {}


def parent_of_rank(taxon: Taxon, rank: Rank) -> Taxon | None:
    key = (taxon, rank)
    if key in _rank_cache:
        return _rank_cache[key]
    if taxon.rank > rank and taxon.rank != Rank.unranked:
        return None
    elif taxon.rank == rank:
        return taxon
    elif taxon.parent is None:
        return None
    else:
        parent = parent_of_rank(taxon.parent, rank)
        _rank_cache[key] = parent
        return parent


def process_mdd_type(text: str) -> str | None:
    if text == "" or text == "NA":
        return None
    text = re.sub(r" \[[^\]]+\]", "", text)
    text = re.sub(r"^([A-Z\-]+)(?=\d)", r"\1 ", text)
    text = re.sub(r"^(BM|NHM) ", "BMNH ", text)
    return text


def process_mdd_authority(text: str) -> str:
    text = re.sub(r" in .*", "", text)
    text = text.replace("J. Edwards Hill", "Hill")
    # TODO compare initials too
    text = re.sub(r"\b[A-ZÉ]\. ", "", text)
    text = text.replace(", & ", " & ")
    text = re.sub(r"^(von|de) ", "", text)
    return text


def compare_single(taxon: Taxon, mdd_row: MddRow) -> Iterable[Difference]:
    mismatched_genus = False
    sci_name = mdd_row["sciName"].replace("_", " ")
    mdd_id = mdd_row["id"]
    # Exclude subclass and infraclass because it's not very interesting
    for rank in (
        Rank.order,
        Rank.suborder,
        Rank.infraorder,
        Rank.superfamily,
        Rank.family,
        Rank.subfamily,
        Rank.tribe,
        Rank.genus,
    ):
        mdd_parent_raw = mdd_row[rank.name]  # type: ignore[literal-required]
        hesp_parent_raw = parent_of_rank(taxon, rank)
        mdd_parent = (
            mdd_parent_raw.title()
            if mdd_parent_raw not in ("NA", "INCERTAE SEDIS")
            else None
        )
        hesp_parent = (
            hesp_parent_raw.valid_name if hesp_parent_raw is not None else None
        )
        if mdd_parent != hesp_parent:
            if rank is Rank.genus:
                kind = DifferenceKind.genus_assignment
                mismatched_genus = True
            else:
                kind = DifferenceKind.higher_classification
            yield Difference(
                kind,
                mdd=mdd_parent,
                hesp=hesp_parent,
                mdd_id=mdd_id,
                taxon=taxon,
                comment=rank.name,
            )

    if not mismatched_genus and taxon.valid_name != sci_name:
        yield Difference(
            DifferenceKind.spelling,
            mdd=sci_name,
            hesp=taxon.valid_name,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    nam = taxon.base_name

    hesp_authority = helpers.romanize_russian(nam.taxonomic_authority())
    if hesp_authority != process_mdd_authority(mdd_row["authoritySpeciesAuthor"]):
        yield Difference(
            DifferenceKind.authority,
            mdd=mdd_row["authoritySpeciesAuthor"],
            hesp=hesp_authority,
            mdd_id=mdd_id,
            taxon=taxon,
        )

    if str(nam.numeric_year()) != mdd_row["authoritySpeciesYear"]:
        yield Difference(
            DifferenceKind.year,
            mdd=mdd_row["authoritySpeciesYear"],
            hesp=str(nam.numeric_year()),
            mdd_id=mdd_id,
            taxon=taxon,
        )

    mdd_orig = mdd_row["originalNameCombination"].replace("_", " ")
    if nam.original_name is not None and not mdd_orig:
        yield Difference(
            DifferenceKind.original_name_missing_mdd,
            mdd=None,
            hesp=nam.original_name,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif nam.original_name is None and mdd_orig:
        yield Difference(
            DifferenceKind.original_name_missing_hesp,
            mdd=mdd_orig,
            hesp=None,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif nam.original_name != mdd_orig:
        yield Difference(
            DifferenceKind.original_name,
            mdd=mdd_orig,
            hesp=nam.original_name,
            mdd_id=mdd_id,
            taxon=taxon,
        )

    mdd_type = process_mdd_type(mdd_row["holotypeVoucher"])
    if nam.type_specimen is not None and mdd_type is None:
        yield Difference(
            DifferenceKind.type_specimen_missing_mdd,
            mdd=None,
            hesp=nam.type_specimen,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif nam.type_specimen is None and mdd_type is not None:
        yield Difference(
            DifferenceKind.type_specimen_missing_hesp,
            mdd=mdd_row["holotypeVoucher"],
            hesp=None,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif nam.type_specimen != mdd_type:
        yield Difference(
            DifferenceKind.type_specimen,
            mdd=mdd_row["holotypeVoucher"],
            hesp=nam.type_specimen,
            mdd_id=mdd_id,
            taxon=taxon,
        )

    mdd_is_extinct = mdd_row["extinct"] == "1"
    hesp_is_extinct = taxon.age is AgeClass.recently_extinct
    if mdd_is_extinct is not hesp_is_extinct:
        yield Difference(
            DifferenceKind.extinction_status,
            mdd="extinct" if mdd_is_extinct else "living",
            hesp=taxon.age.name,
            mdd_id=mdd_id,
            taxon=taxon,
        )


def compare(
    taxa: Iterable[Taxon], mdd_data: Iterable[MddRow], add_ids: bool = False
) -> Iterable[Difference]:
    mdd_by_name = {}
    mdd_by_id = {}
    mdd_row: MddRow | None
    for mdd_row in mdd_data:
        mdd_by_id[mdd_row["id"]] = mdd_row
        mdd_by_name[mdd_row["sciName"].replace("_", " ")] = mdd_row

    for taxon in taxa:
        mdd_id = get_mdd_id(taxon)
        mdd_row = None
        if mdd_id is not None:
            if mdd_id in mdd_by_id:
                mdd_row = mdd_by_id[mdd_id]
            else:
                yield Difference(
                    DifferenceKind.invalid_mdd_id, hesp=mdd_id, taxon=taxon
                )
        if mdd_row is None:
            if taxon.valid_name in mdd_by_name:
                mdd_row = mdd_by_name[taxon.valid_name]
                if add_ids:
                    print(f"Adding MDD id {mdd_row['id']} to {taxon}")
                    taxon.add_tag(TaxonTag.MDD(mdd_row["id"]))
            else:
                yield Difference(
                    DifferenceKind.missing_in_mdd, hesp=taxon.valid_name, taxon=taxon
                )
                continue
        yield from compare_single(taxon, mdd_row)
        del mdd_by_id[mdd_row["id"]]
        del mdd_by_name[mdd_row["sciName"].replace("_", " ")]

    for sci_name, row in mdd_by_name.items():
        yield Difference(
            DifferenceKind.missing_in_hesperomys, mdd=sci_name, mdd_id=row["id"]
        )


def generate_markdown(
    differences: Iterable[Difference],
    f: IO[str],
    ignore_kinds: Sequence[DifferenceKind] = (),
) -> None:
    by_kind: dict[DifferenceKind, list[Difference]] = {}
    for difference in differences:
        by_kind.setdefault(difference.kind, []).append(difference)

    print("## Summary", file=f)
    print(file=f)
    for kind, differences in sorted(by_kind.items(), key=lambda pair: pair[0].name):
        print(f"- {kind.name}: {len(differences)}", file=f)
    print(file=f)

    for kind, differences in sorted(by_kind.items(), key=lambda pair: pair[0].name):
        if kind in ignore_kinds:
            continue
        print(f"## {kind.name} ({len(differences)} differences)", file=f)
        print(file=f)
        for difference in differences:
            print(f"- {difference.to_markdown()}", file=f)
        print(file=f)


def run(
    mdd_file: Path,
    md_output: Path | None = None,
    csv_output: Path | None = None,
    add_ids: bool = False,
    ignore_kinds: Sequence[DifferenceKind] = (),
) -> None:
    print("Reading MDD data... ", end="", flush=True)
    with mdd_file.open() as f:
        reader = csv.DictReader(f)
        mdd_rows: list[MddRow] = list(reader)  # type: ignore[arg-type]
    print("Done")

    print("Reading Hesperomys data... ", end="", flush=True)
    mammalia = Taxon.getter("valid_name")("Mammalia")
    assert isinstance(mammalia, Taxon), repr(mammalia)
    species = all_species(mammalia)
    print("Done")
    print("Generating differences... ", end="", flush=True)
    differences = list(compare(species, mdd_rows, add_ids=add_ids))
    print("Done")

    generate_markdown(differences, sys.stdout, ignore_kinds=ignore_kinds)
    if md_output is not None:
        with md_output.open("w") as f:
            generate_markdown(differences, f, ignore_kinds=ignore_kinds)

    if csv_output is not None:
        with csv_output.open("w") as f:
            writer = csv.DictWriter(f, list(Difference.__annotations__))
            writer.writeheader()
            for difference in differences:
                writer.writerow(difference.to_csv())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mdd_file", help="Input CSV file with MDD data")
    parser.add_argument("--md", help="Markdown file to output")
    parser.add_argument("--csv", help="CSV file to output")
    parser.add_argument(
        "--add-ids", action="store_true", default=False, help="Add MDD ids to taxa"
    )
    parser.add_argument(
        "-i",
        "--ignore",
        nargs="*",
        type=lambda k: DifferenceKind[k],
        help="Kinds to ignore",
    )
    args = parser.parse_args()
    print(args.ignore)
    run(
        mdd_file=Path(args.mdd_file),
        md_output=Path(args.md) if args.md else None,
        csv_output=Path(args.csv) if args.csv else None,
        add_ids=args.add_ids,
        ignore_kinds=args.ignore,
    )


if __name__ == "__main__":
    main()