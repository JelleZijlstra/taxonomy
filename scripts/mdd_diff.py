"""Example invocation:

python -m scripts.mdd_diff notes/mdd/v1.12/MDD_v1.12_6718species.csv --csv mdd-compare.csv -i authority -i authority_exact -i higher_classification -i original_name -i original_name_missing_mdd -i type_specimen -i type_specimen_missing_hesp -i type_specimen_missing_mdd -i year

"""

import argparse
import csv
import enum
import re
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TypedDict

import unidecode

from taxonomy.db import helpers, models
from taxonomy.db.constants import (
    AgeClass,
    NamingConvention,
    NomenclatureStatus,
    Rank,
    Status,
)
from taxonomy.db.models import CitationGroup, Collection, Name, NameTag, Person, Taxon
from taxonomy.db.models.tags import TaxonTag

INCLUDED_AGES = (AgeClass.extant, AgeClass.recently_extinct)
WARN_NO_INITIALS = False


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
    typeVoucher: str
    typeVoucherURIs: str
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
    authority_exact = 16


@dataclass
class Difference:
    kind: DifferenceKind
    comment: str | None = None
    mdd: str | None = None
    hesp: str | None = None
    mdd_id: str | None = None
    taxon: Taxon | None = None

    def to_markdown(self, *, extra: str | None = None, concise: bool = False) -> str:
        parts = []
        if self.kind is DifferenceKind.missing_in_hesperomys:
            parts.append(f"Missing in Hesperomys: _{self.mdd}_")
        elif self.kind is DifferenceKind.missing_in_mdd:
            parts.append(f"Missing in MDD: _{self.hesp}_")
        else:
            parts.append(f"{self.hesp or '(none)'} (H) vs. {self.mdd or '(none)'} (M)")
        parentheticals = []
        if self.mdd_id and not concise:
            parentheticals.append(f"MDD#{self.mdd_id}")
        if self.taxon and not concise:
            parentheticals.append(
                f"[{self.taxon.valid_name}](https://hesperomys.com/t/{self.taxon.id})"
            )
        if self.comment:
            parentheticals.append(self.comment)
        if not concise and self.taxon and self.taxon.base_name.original_citation:
            link = (
                self.taxon.base_name.original_citation.concise_markdown_link().replace(
                    "/a/", "https://hesperomys.com/a/"
                )
            )
            parentheticals.append(f"original citation: {link}")
        if extra:
            parentheticals.append(extra)
        if not concise:
            parentheticals.append(self.kind.name)
        parts.append(f" ({'; '.join(parentheticals)})")
        return "".join(parts)

    @classmethod
    def get_csv_columns(cls) -> list[str]:
        return [*cls.__annotations__.keys(), "taxon_name", "mdd_link", "hesp_link"]

    def to_csv(self) -> dict[str, str]:
        return {
            "kind": self.kind.name,
            "comment": self.comment or "",
            "mdd": self.mdd or "",
            "hesp": self.hesp or "",
            "mdd_id": self.mdd_id or "",
            "taxon": str(self.taxon.id) if self.taxon else "",
            "taxon_name": self.taxon.valid_name if self.taxon else "",
            "mdd_link": (
                f"https://www.mammaldiversity.org/explore.html#{self.mdd_id}"
                if self.mdd_id
                else ""
            ),
            "hesp_link": (
                f"https://hesperomys.com/t/{self.taxon.id}" if self.taxon else ""
            ),
        }


def all_species(taxon: Taxon) -> Iterable[Taxon]:
    if taxon.age not in INCLUDED_AGES:
        return
    if taxon.rank is Rank.species:
        if taxon.base_name.status is Status.valid:
            yield taxon
    else:
        for child in Taxon.add_validity_check(
            taxon.children.filter(Taxon.age.is_in(INCLUDED_AGES))
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


def split_mdd_type(text: str) -> Iterable[tuple[str, str | None]]:
    text = re.sub(r"\[([^,\]]+),([^\]]+)\]", r"[\1]", text)
    pieces = text.split(",")
    for piece in pieces:
        piece = piece.strip()
        if match := re.fullmatch(r"(.*) \[([^\]]+)\]", piece):
            yield match.group(1), match.group(2)
        else:
            yield piece, None


MDD_TYPE_REGEXES = [
    (r"\s+", " "),  # weird whitespace
    (r"^([A-Z\-]+)(?=\d)", r"\1 "),  # space after collection name
    (r"^(BM|NHM) (ZD |GMCM )", "BMNH "),
    (r"^BM ", "BMNH "),
    (r"^AMNHM-", "AMNH "),
    (r"^BPBM ", ""),
    (r"\s+", " "),  # weird whitespace
    (r"^BMNH ", "BMNH Mammals "),
    (r"^USNM ", "USNM:MAMM:"),
    (r"^MCZ ", "MCZ:Mamm:"),
    (r"^FMNH ", "FMNH Mammals "),
    (r"^AMNH ", "AMNH M-"),
]


def process_mdd_type(text: str) -> str | None:
    if text in ("", "NA"):
        return None
    final = []
    for piece, label in split_mdd_type(text):
        if label in ("paratype", "paralectotype"):
            continue
        piece = piece.strip().rstrip(".")
        for rgx, sub in MDD_TYPE_REGEXES:
            piece = re.sub(rgx, sub, piece)
        if "BMNH" in piece:
            piece = models.name.lint.clean_up_bmnh_type(piece)
        final.append(piece)
    return ", ".join(sorted(final))


def get_mdd_style_authority(nam: Name, need_initials: set[str]) -> str:
    authors = nam.get_authors()
    name_authors = get_mdd_style_authority_for_name_list(authors, need_initials, nam)
    if (
        nam.original_citation is not None
        and nam.original_citation.get_authors() != authors
    ):
        article_authors = get_mdd_style_authority_for_name_list(
            nam.original_citation.get_authors(), need_initials, nam
        )
        return f"{name_authors} in {article_authors}"
    return name_authors


def get_need_initials_authors(nams: Iterable[Name]) -> set[str]:
    all_authors: set[Person] = set()
    for nam in nams:
        all_authors.update(nam.get_authors())
        if nam.original_citation:
            all_authors.update(nam.original_citation.get_authors())
    family_name_to_authors: Counter[str] = Counter()
    for author in all_authors:
        family_name_to_authors[helpers.romanize_russian(author.family_name)] += 1
    return {
        family_name
        for family_name, count in family_name_to_authors.items()
        if count > 1
        or family_name
        == "True"  # MDD gives True initials so he doesn't get mangled by Google Sheets
    }


def get_mdd_style_authority_for_name_list(
    persons: Sequence[Person], need_initials: set[str], nam: Name
) -> str:
    match len(persons):
        case 1:
            return get_mdd_style_authority_for_single_person(
                persons[0], need_initials, nam
            )
        case 2:
            first = get_mdd_style_authority_for_single_person(
                persons[0], need_initials, nam
            )
            second = get_mdd_style_authority_for_single_person(
                persons[1], need_initials, nam
            )
            return f"{first} & {second}"
        case _:
            authorities = [
                get_mdd_style_authority_for_single_person(person, need_initials, nam)
                for person in persons
            ]
            return ", ".join(authorities[:-1]) + f", & {authorities[-1]}"


def get_mdd_style_authority_for_single_person(
    person: Person, need_initials: set[str], nam: Name
) -> str:
    # special case!
    if person.family_name == "Hill":
        match person.given_names:
            case "John Eric":
                return "J. Eric Hill"
            case "John Edwards":
                return "J. Edwards Hill"
    match person.naming_convention:
        case NamingConvention.pinyin:
            return f"{person.family_name} {person.given_names.replace('-', '').lower().title()}"
        case NamingConvention.chinese:
            if person.given_names is not None:
                given_name = person.given_names.replace("-", "").lower().title()
                return f"{person.family_name} {given_name}"
            else:
                if WARN_NO_INITIALS:
                    print(f"warning: no initials for {person} in {nam}")
                return person.family_name
        case NamingConvention.korean:
            return f"{person.family_name} {person.given_names}"
        case NamingConvention.vietnamese:
            return f"{person.given_names} {person.family_name}"
        case _:
            family_name = person.get_transliterated_family_name()
            if person.tussenvoegsel is not None:
                if person.naming_convention is NamingConvention.dutch:
                    tsv = person.tussenvoegsel[0].upper() + person.tussenvoegsel[1:]
                    family_name = f"{tsv} {family_name}"
                else:
                    family_name = f"{person.tussenvoegsel} {family_name}"
            if person.family_name not in need_initials:
                return family_name
            initials = person.get_initials()
            if initials is None:
                if WARN_NO_INITIALS:
                    print(f"warning: no initials for {person} in {nam}")
                return family_name
            initials = helpers.romanize_russian(initials)
            initials = re.sub(r"\.(?=[A-Z])", ". ", initials)
            return f"{initials} {family_name}"


def _possible_family_names(hesp_author: Person) -> Iterable[str]:
    yield hesp_author.family_name
    if hesp_author.family_name[0].islower():
        yield f"{hesp_author.family_name[0].upper()}{hesp_author.family_name[1:]}"
    if hesp_author.tussenvoegsel:
        yield f"{hesp_author.tussenvoegsel} {hesp_author.family_name}"
        tussenvoegsel = (
            f"{hesp_author.tussenvoegsel[0].upper()}{hesp_author.tussenvoegsel[1:]}"
        )
        yield f"{tussenvoegsel} {hesp_author.family_name}"


def possible_mdd_authors(hesp_author: Person) -> Iterable[str]:
    if hesp_author.naming_convention in (
        NamingConvention.pinyin,
        NamingConvention.chinese,
    ):
        yield (
            f"{hesp_author.family_name} {hesp_author.given_names.replace('-', '').lower().title()}"
        )
        return
    if hesp_author.naming_convention is NamingConvention.korean:
        yield f"{hesp_author.family_name} {hesp_author.given_names}"
        return
    if hesp_author.naming_convention is NamingConvention.vietnamese:
        name = f"{hesp_author.given_names} {hesp_author.family_name}"
        yield name
        decoded = unidecode.unidecode(name)
        if name != decoded:
            yield decoded
        return
    if hesp_author.naming_convention in (
        NamingConvention.russian,
        NamingConvention.ukrainian,
    ):
        yield hesp_author.get_transliterated_family_name()

    for family_name in _possible_family_names(hesp_author):
        family_name = helpers.romanize_russian(family_name)
        yield family_name
        if initials := hesp_author.get_initials():
            initials = helpers.romanize_russian(initials)
            for remove_infix in (False, True):
                for splits in r" ", r"(?<=\.)(?!-)| ":
                    initials_list = re.split(splits, initials)
                    initials_list = [i for i in initials_list if i]
                    if remove_infix:
                        initials_list = [i for i in initials_list if i.endswith(".")]
                    yield f"{''.join(f'{i} ' for i in initials_list)}{family_name}"
                    # only first initial
                    if len(initials_list) > 1:
                        yield f"{initials_list[0]} {family_name}"
                    # J. Edwards Hill
                    if (
                        hesp_author.given_names
                        and hesp_author.given_names.count(" ") == 1
                    ):
                        before, after = hesp_author.given_names.split()
                        yield f"{before[0]}. {after} {family_name}"


def does_author_match(mdd_author: str, hesp_author: Person) -> bool:
    return mdd_author in possible_mdd_authors(hesp_author)


def compare_authors(
    taxon: Taxon, mdd_row: MddRow, need_initials: set[str]
) -> Iterable[Difference]:
    mdd_style = get_mdd_style_authority(taxon.base_name, need_initials)
    if mdd_row["authoritySpeciesAuthor"] != mdd_style:
        yield Difference(
            DifferenceKind.authority_exact,
            mdd=mdd_row["authoritySpeciesAuthor"],
            hesp=mdd_style,
            mdd_id=mdd_row["id"],
            taxon=taxon,
        )
    yield from compare_authors_to_name(
        taxon.base_name, mdd_row["id"], mdd_row["authoritySpeciesAuthor"], taxon=taxon
    )


def compare_authors_to_name(
    nam: Name, mdd_id: str, raw_mdd_authority: str, taxon: Taxon | None = None
) -> Iterable[Difference]:
    if taxon is None:
        taxon = nam.taxon
    mdd_authority, *_ = raw_mdd_authority.split(" in ")
    mdd_authors = re.split(r", (?:& )?| & ", mdd_authority)
    hesp_authors = nam.get_authors()
    hesp_authority = helpers.romanize_russian(nam.taxonomic_authority())
    if len(mdd_authors) != len(hesp_authors):
        yield Difference(
            DifferenceKind.authority,
            mdd=raw_mdd_authority,
            hesp=hesp_authority,
            mdd_id=mdd_id,
            taxon=taxon,
            comment=(
                f"{len(mdd_authors)} authors in MDD, {len(hesp_authors)} authors in"
                " Hesperomys"
            ),
        )
        return
    for i, (mdd_author, hesp_author) in enumerate(
        zip(mdd_authors, hesp_authors, strict=True), start=1
    ):
        if not does_author_match(mdd_author, hesp_author):
            yield Difference(
                DifferenceKind.authority,
                mdd=mdd_author,
                hesp=(
                    f"{hesp_author} (tried:"
                    f" {', '.join(sorted(set(possible_mdd_authors(hesp_author))))})"
                ),
                mdd_id=mdd_id,
                taxon=taxon,
                comment=f"Author {i}",
            )


def _get_hesp_type_specimen_name(nam: Name) -> Name:
    if nam.nomenclature_status is NomenclatureStatus.nomen_novum:
        target = nam.get_tag_target(NameTag.NomenNovumFor)
        if target is not None:
            return target
    return nam


def clean_hesp_type(nam: Name) -> str | None:
    result = ", ".join(models.name.lint.get_all_type_specimen_texts(nam))
    if not result:
        return None
    return result


def compare_single(
    taxon: Taxon, mdd_row: MddRow, need_initials: set[str]
) -> Iterable[Difference]:
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

    yield from compare_authors(taxon, mdd_row, need_initials)

    if str(nam.numeric_year()) != mdd_row["authoritySpeciesYear"]:
        yield Difference(
            DifferenceKind.year,
            mdd=mdd_row["authoritySpeciesYear"],
            hesp=str(nam.numeric_year()),
            mdd_id=mdd_id,
            taxon=taxon,
        )

    mdd_orig = mdd_row["originalNameCombination"].replace("_", " ") or None
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

    mdd_type = process_mdd_type(mdd_row["typeVoucher"])
    hesp_type_nam = _get_hesp_type_specimen_name(nam)
    hesp_type = clean_hesp_type(hesp_type_nam)
    if hesp_type is not None and mdd_type is None:
        yield Difference(
            DifferenceKind.type_specimen_missing_mdd,
            mdd=None,
            hesp=hesp_type,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif hesp_type is None and mdd_type is not None:
        yield Difference(
            DifferenceKind.type_specimen_missing_hesp,
            mdd=mdd_row["typeVoucher"],
            hesp=None,
            mdd_id=mdd_id,
            taxon=taxon,
        )
    elif hesp_type != mdd_type:
        print(f"{hesp_type!r} vs. {mdd_type!r} (from {mdd_row['typeVoucher']!r})")
        yield Difference(
            DifferenceKind.type_specimen,
            mdd=mdd_row["typeVoucher"],
            hesp=hesp_type_nam.type_specimen,
            mdd_id=mdd_id,
            taxon=taxon,
            comment=f"compared H {hesp_type!r} vs. M {mdd_type!r}",
        )

    mdd_is_extinct = mdd_row.get("extinct") == "1"
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
    taxa: Iterable[Taxon],
    mdd_data: Iterable[MddRow],
    *,
    add_ids: bool = False,
    need_initials: set[str],
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
        yield from compare_single(taxon, mdd_row, need_initials=need_initials)
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
    for kind, diffs in sorted(by_kind.items(), key=lambda pair: pair[0].name):
        print(f"- {kind.name}: {len(diffs)}", file=f)
    print(file=f)

    for kind, diffs in sorted(by_kind.items(), key=lambda pair: pair[0].name):
        if kind in ignore_kinds:
            continue
        print(f"## {kind.name} ({len(diffs)} differences)", file=f)
        print(file=f)
        generate_markdown_for_kind(kind, diffs, f)
        print(file=f)


def generate_markdown_for_kind(
    kind: DifferenceKind, differences: Sequence[Difference], f: IO[str]
) -> None:
    match kind:
        case DifferenceKind.higher_classification:
            by_difference: dict[
                tuple[str | None, str | None, str | None], list[Difference]
            ] = {}
            for difference in differences:
                key = (difference.comment, difference.mdd, difference.hesp)
                by_difference.setdefault(key, []).append(difference)
            for (comment, mdd, hesp), diffs in by_difference.items():
                print(
                    f"- {comment} {mdd} (MDD) vs. {hesp} (Hesperomys):"
                    f" {len(diffs)} differences, e.g.:",
                    file=f,
                )
                print(f"    - {differences[0].to_markdown()}", file=f)
        case DifferenceKind.authority:
            by_mdd_author: dict[str, list[Difference]] = {}
            for difference in differences:
                if difference.comment and not difference.comment.startswith("Author "):
                    author_key = "author count"
                else:
                    author_key = str(difference.mdd)
                by_mdd_author.setdefault(author_key, []).append(difference)
            for author, author_differences in sorted(by_mdd_author.items()):
                print(f"- {author} ({len(author_differences)} differences)", file=f)
                for difference in author_differences:
                    print(f"    - {difference.to_markdown()}", file=f)
        case DifferenceKind.year:
            by_cg: dict[CitationGroup | None, list[Difference]] = {}
            for difference in differences:
                cg: CitationGroup | None
                if difference.taxon is None:
                    cg = None
                else:
                    cg = difference.taxon.base_name.get_citation_group()
                by_cg.setdefault(cg, []).append(difference)
            for cg, cg_differences in sorted(
                by_cg.items(),
                key=lambda pair: pair[0].name if pair[0] is not None else "",
            ):
                print(f"- {cg} ({len(cg_differences)} differences)", file=f)
                for difference in sorted(
                    cg_differences, key=lambda diff: diff.mdd or ""
                ):
                    print(f"    - {difference.to_markdown()}", file=f)
        case DifferenceKind.type_specimen:
            by_coll: dict[Collection | None, list[Difference]] = {}
            for difference in differences:
                coll: Collection | None
                if difference.taxon is None:
                    coll = None
                else:
                    coll = _get_hesp_type_specimen_name(
                        difference.taxon.base_name
                    ).collection
                by_coll.setdefault(coll, []).append(difference)
            for coll, coll_differences in sorted(
                by_coll.items(),
                key=lambda pair: pair[0].name if pair[0] is not None else "",
            ):
                print(f"- {coll} ({len(coll_differences)} differences)", file=f)
                for difference in sorted(
                    coll_differences, key=lambda diff: diff.mdd or ""
                ):
                    print(f"    - {difference.to_markdown()}", file=f)
        case DifferenceKind.type_specimen_missing_hesp:
            for difference in sorted(differences, key=lambda d: d.mdd or ""):
                print(f"- {difference.to_markdown()}", file=f)
        case _:
            for difference in differences:
                print(f"- {difference.to_markdown()}", file=f)


def run(
    *,
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
    species = list(all_species(mammalia))
    print("Done")
    print("Generating need_initials list.. ", end="", flush=True)
    need_initials = get_need_initials_authors(txn.base_name for txn in species)
    print("Done")
    print("Generating differences... ", end="", flush=True)
    differences = list(
        compare(species, mdd_rows, add_ids=add_ids, need_initials=need_initials)
    )
    print("Done")

    generate_markdown(differences, sys.stdout, ignore_kinds=ignore_kinds)
    if md_output is not None:
        with md_output.open("w") as f:
            generate_markdown(differences, f, ignore_kinds=ignore_kinds)

    if csv_output is not None:
        with csv_output.open("w") as f:
            writer = csv.DictWriter(f, Difference.get_csv_columns())
            writer.writeheader()
            for difference in differences:
                if difference.kind in ignore_kinds:
                    continue
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
        type=lambda k: DifferenceKind[k],
        help="Kinds to ignore",
        action="append",
    )
    parser.add_argument(
        "-s",
        "--select",
        type=lambda k: DifferenceKind[k],
        help="Output only these kinds",
        action="append",
    )
    args = parser.parse_args()
    ignore = set()
    if args.select:
        ignore |= {kind for kind in DifferenceKind if kind not in args.select}
    if args.ignore:
        ignore |= set(args.ignore)

    run(
        mdd_file=Path(args.mdd_file),
        md_output=Path(args.md) if args.md else None,
        csv_output=Path(args.csv) if args.csv else None,
        add_ids=args.add_ids,
        ignore_kinds=list(ignore),
    )


if __name__ == "__main__":
    main()
