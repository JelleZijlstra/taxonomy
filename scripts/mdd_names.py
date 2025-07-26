"""Notes for auto-gsheet updating:

- Use https://pypi.org/project/gspread/
- Take a backup of the sheet before any changes
- Generate a summary file with changes applied
- For each category of changes, first show them all and ask whether to apply them all
  at once. If yes, apply them. Else, ask whether to go over them one by one.
- For new names, first get the max() of the existing MDD ids and start from there.

To set up gspread, follow the instructions in https://docs.gspread.org/en/latest/oauth2.html#oauth-client-id
to get an OAuth client id. The tokens appears to expire after a week. To refresh, delete
 ~/.config/gspread/authorized_user.json and run this script. It will open a web browser with a flow  to
 re-authorize the token. Go through the flow, and then the script should run successfully.

You can also create the whole token from scratch again:

- Go to https://console.cloud.google.com/apis/api/sheets.googleapis.com/credentials?authuser=1&project=directed-tracer-123911&supportedpurview=project
- Add an "OAuth 2.0 Client ID" credential for a desktop app
- Download the credentials and put them in ~/.config/gspread/credentials.json
- Delete ~/.config/gspread/authorized_user.json
- Run this script with --gspread-test to re-authorize. Make sure to fix cell A1 in the MDD sheet back afterwards.

"""

import argparse
import csv
import datetime
import enum
import functools
import itertools
import pprint
import re
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import google.auth.exceptions
import gspread
import httpx
import Levenshtein

from scripts import mdd_diff
from taxonomy import getinput
from taxonomy.config import get_options
from taxonomy.db import export, helpers, models
from taxonomy.db.constants import (
    AgeClass,
    Group,
    NomenclatureStatus,
    Rank,
    RegionKind,
    Status,
)
from taxonomy.db.models import Article, Name, Taxon
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.name import NameTag, TypeTag

LIMIT_AUTH_LINKS = False

MDD_ARTICLE_ID = 67057


@functools.cache
def resolve_hesp_id(hesp_id_str: str) -> int | None:
    if hesp_id_str:
        return int(hesp_id_str)
    return None


def get_mdd_like_species_name(name: Name) -> str:
    if name.taxon.base_name.status is not Status.valid:
        try:
            genus = name.taxon.parent_of_rank(Rank.genus)
        except ValueError:
            return "incertae_sedis incertae_sedis"
        else:
            if genus.base_name.status is Status.valid:
                return f"{genus.valid_name} incertae_sedis"
            else:
                return "incertae_sedis incertae_sedis"
    try:
        return name.taxon.parent_of_rank(Rank.species).valid_name
    except ValueError:
        return ""


def get_mdd_status(name: Name, maybe_taxon: Taxon | None) -> str:
    match name.status:
        case Status.valid:
            if maybe_taxon is not None:
                return maybe_taxon.rank.name
            match name.taxon.rank:
                case Rank.subspecies:
                    if name.taxon.parent.base_name == name:
                        return "species"
                    return "synonym"
                case rank:
                    return rank.name
        case Status.synonym:
            match name.taxon.base_name.status:
                case Status.valid:
                    return "synonym"
                case _:
                    return name.taxon.base_name.status.name
        case _:
            return name.status.name


def get_type_locality_country_and_subregion(nam: Name) -> tuple[str, str, str]:
    if nam.type_locality is None:
        return "", "", ""
    region = nam.type_locality.region
    regions = [region.name]
    while region is not None and region.kind not in (
        RegionKind.planet,
        RegionKind.continent,
        RegionKind.country,
        RegionKind.sea,
    ):
        region = region.parent
        regions.append(region.name)
    regions.reverse()
    match len(regions):
        case 1:
            return regions[0], "", ""
        case 2:
            return regions[0], regions[1], ""
        case _:
            # Use the lowest-level region for the third one, as it's
            # usually more interesting than the third-level one.
            return regions[0], regions[1], regions[-1]


def get_authority_link(nam: Name) -> str:
    tags = nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink)
    return " | ".join(sorted({tag.url for tag in tags}))


def get_type_specimen_link(nam: Name) -> str:
    tags = nam.get_tags(nam.type_tags, TypeTag.TypeSpecimenLinkFor)
    return " | ".join(sorted({tag.url for tag in tags}))


def get_nomenclature_status_string(nam: Name) -> str:
    applicable = models.name.lint.get_sorted_applicable_statuses(nam)
    if not applicable:
        return "available"
    return " | ".join(status.name for status in applicable)


VARIANT_OF_TAGS = (
    NameTag.UnjustifiedEmendationOf,
    NameTag.IncorrectSubsequentSpellingOf,
    NameTag.NomenNovumFor,
    NameTag.VariantOf,
    NameTag.MandatoryChangeOf,
    NameTag.IncorrectOriginalSpellingOf,
    NameTag.SubsequentUsageOf,
    NameTag.JustifiedEmendationOf,
    NameTag.NameCombinationOf,
    NameTag.MisidentificationOf,
)
HOMONYM_TAGS = (
    NameTag.PreoccupiedBy,
    NameTag.PrimaryHomonymOf,
    NameTag.SecondaryHomonymOf,
    NameTag.PermanentlyReplacedSecondaryHomonymOf,
)


def get_tag_targets(nam: Name, tags: tuple[type[NameTag], ...]) -> Iterable[Name]:
    for tag in nam.tags:
        if isinstance(tag, tags):
            yield tag.name


def stringify_name_for_mdd(
    nam: Name, need_initials: set[str], hesp_id_to_mdd_id: dict[int, str]
) -> str:
    parts = []
    if nam.corrected_original_name is not None:
        parts.append(nam.corrected_original_name)
    else:
        parts.append(nam.root_name)
    if nam.author_tags:
        parts.append(" ")
        parts.append(mdd_diff.get_mdd_style_authority(nam, need_initials))
    if nam.year:
        parts.append(", ")
        parts.append(nam.year[:4])
    combined_id = combine_rank_and_id(nam.taxon.rank, nam.id)
    if nam.id in hesp_id_to_mdd_id:
        parts.append(f" [{hesp_id_to_mdd_id[nam.id]}]")
    elif combined_id in hesp_id_to_mdd_id:
        parts.append(f" [{hesp_id_to_mdd_id[combined_id]}]")
    else:
        parts.append(" [fossil]")
    return "".join(parts)


def get_tag_targets_string(
    nam: Name,
    tags: tuple[type[NameTag], ...],
    need_initials: set[str],
    hesp_id_to_mdd_id: dict[int, str],
) -> str:
    nams = get_tag_targets(nam, tags)
    return " | ".join(
        stringify_name_for_mdd(nam, need_initials, hesp_id_to_mdd_id) for nam in nams
    )


def get_authority_parens(nam: Name) -> str:
    match nam.should_parenthesize_authority():
        case None:
            if nam.corrected_original_name is not None:
                # probably some weird name that is not the current genus name
                return "1"
            return "?"
        case True:
            return "1"
        case False:
            return "0"
    assert False, "unreachable"


def get_citation_kind(name: Name) -> str:
    if name.original_citation is None:
        return ""
    kind = name.original_citation.get_effective_kind().name
    if name.original_citation.has_tag(models.article.ArticleTag.NonOriginal):
        kind += " (non-original)"
    if name.original_citation.has_tag(models.article.ArticleTag.Incomplete):
        kind += " (incomplete)"
    return kind


def get_type_specimen(nam: Name) -> str:
    if nam.type_specimen:
        return nam.type_specimen
    elif nam.collection:
        return f"{nam.collection.label} (number not known)"
    return ""


OMITTED_COLUMNS = {
    "MDD_old_type_locality",
    "MDD_emended_type_locality",
    "MDD_comments",
    "MDD_subspecificEpithet",
    # Identifiers
    "MDD_syn_ID",
    "Hesp_id",
}


def get_hesp_row(
    name: Name,
    need_initials: set[str],
    hesp_id_to_mdd_id: dict[int, str],
    maybe_taxon: Taxon | None,
) -> dict[str, Any]:
    row = {}
    row["Hesp_species"] = get_mdd_like_species_name(name)
    row["Hesp_group"] = name.group.name
    row["Hesp_taxon"] = (
        maybe_taxon.valid_name if maybe_taxon is not None else name.taxon.valid_name
    )
    row["Hesp_root_name"] = name.root_name
    mdd_style_author = mdd_diff.get_mdd_style_authority(name, need_initials)
    row["Hesp_author"] = mdd_style_author
    row["Hesp_year"] = name.year[:4] if name.year else ""
    row["Hesp_authority_parentheses"] = get_authority_parens(name)
    row["Hesp_validity"] = get_mdd_status(name, maybe_taxon)
    row["Hesp_original_combination"] = name.original_name or ""
    row["Hesp_normalized_original_combination"] = name.corrected_original_name or ""
    row["Hesp_original_rank"] = (
        name.original_rank.name if name.original_rank is not None else ""
    )

    # Nomenclature status
    row["Hesp_nomenclature_status"] = get_nomenclature_status_string(name)
    row["Hesp_variant_of"] = get_tag_targets_string(
        name, VARIANT_OF_TAGS, need_initials, hesp_id_to_mdd_id
    )
    row["Hesp_senior_homonym"] = get_tag_targets_string(
        name, HOMONYM_TAGS, need_initials, hesp_id_to_mdd_id
    )

    # Citation
    if name.verbatim_citation is not None:
        row["Hesp_unchecked_authority_citation"] = name.verbatim_citation
    elif name.original_citation is not None:
        row["Hesp_unchecked_authority_citation"] = "NA"
    else:
        row["Hesp_unchecked_authority_citation"] = ""
    cg = name.get_citation_group()
    row["Hesp_citation_group"] = cg.name if cg else ""
    row["Hesp_authority_page"] = (
        name.page_described.replace("@", "") if name.page_described is not None else ""
    )
    authority_link = get_authority_link(name)
    row["Hesp_authority_page_link"] = authority_link
    if name.original_citation is not None:
        url = name.original_citation.geturl()
        row["Hesp_authority_citation"] = models.article.citations.citepaper(
            name.original_citation,
            include_url=False,
            romanize_authors=True,
            full_date=True,
        )
        row["Hesp_authority_link"] = url or ""
    else:
        row["Hesp_authority_citation"] = ""
        row["Hesp_authority_link"] = ""
    row["Hesp_citation_kind"] = get_citation_kind(name)
    if authority_link:
        row["Hesp_unchecked_authority_page_link"] = ""
    else:
        # At most 3
        try:
            candidates = models.name.lint.get_candidate_bhl_pages(name)
            unchecked_links = " | ".join(
                sorted(page.page_url for page in candidates)[:3]
            )
        except httpx.ReadTimeout:
            # We'll fill up the cache eventually
            unchecked_links = ""
        row["Hesp_unchecked_authority_page_link"] = unchecked_links

    # For nomina nova, get type data from original name
    names_for_tags = [name]
    if name.nomenclature_status is NomenclatureStatus.nomen_novum:
        name_for_types = name.get_tag_target(models.name.NameTag.NomenNovumFor)
        if name_for_types is None:
            name_for_types = name
        else:
            names_for_tags.append(name_for_types)
    else:
        name_for_types = name

    # Type locality
    # Omit: MDD_old_type_locality
    # Omit: MDD_emended_type_locality
    verbatim_tl = []
    emended_tl = []
    citation_details = []
    row["Hesp_type_latitude"] = ""
    row["Hesp_type_longitude"] = ""
    for nam in names_for_tags:
        for tag in nam.type_tags:
            if isinstance(tag, TypeTag.LocationDetail):
                if tag.source == nam.original_citation:
                    verbatim_tl.append(tag.text)
                elif tag.source.id == MDD_ARTICLE_ID:
                    pass  # ignore
                else:
                    citation = helpers.romanize_russian(
                        ", ".join(tag.source.taxonomic_authority())
                    )
                    emended_tl.append(f'"{tag.text}" ({citation})')
            elif isinstance(tag, TypeTag.Coordinates):
                try:
                    _, lat = helpers.standardize_coordinates(
                        tag.latitude, is_latitude=True
                    )
                    row["Hesp_type_latitude"] = str(lat)
                    _, long = helpers.standardize_coordinates(
                        tag.longitude, is_latitude=False
                    )
                    row["Hesp_type_longitude"] = str(long)
                except helpers.InvalidCoordinates:
                    pass
            elif isinstance(tag, TypeTag.CitationDetail):
                citation = helpers.romanize_russian(
                    ", ".join(tag.source.taxonomic_authority())
                )
                citation_details.append(f'"{tag.text}" ({citation})')
    row["Hesp_sourced_unverified_citations"] = " | ".join(citation_details)

    if verbatim_tl:
        row["Hesp_original_type_locality"] = " | ".join(verbatim_tl)
    if emended_tl:
        row["Hesp_unchecked_type_locality"] = " | ".join(emended_tl)
    (
        row["Hesp_type_country"],
        row["Hesp_type_subregion"],
        row["Hesp_type_subregion2"],
    ) = get_type_locality_country_and_subregion(name_for_types)

    # Type specimen
    row["Hesp_holotype"] = get_type_specimen(name_for_types)
    row["Hesp_type_kind"] = (
        name_for_types.species_type_kind.name
        if name_for_types.species_type_kind
        else ""
    )
    row["Hesp_type_specimen_link"] = get_type_specimen_link(name_for_types)

    # Higher classification
    taxon = maybe_taxon if maybe_taxon is not None else name.taxon
    order = taxon.get_derived_field("order")
    if order is not None and order.rank is Rank.order:
        row["Hesp_order"] = order.valid_name
    elif taxon.rank < Rank.order:
        row["Hesp_order"] = "incertae_sedis"
    else:
        row["Hesp_order"] = "NA"
    family = taxon.get_derived_field("family")
    if family is not None and family.rank is Rank.family:
        row["Hesp_family"] = family.valid_name
    elif taxon.rank < Rank.family:
        row["Hesp_family"] = "incertae_sedis"
    else:
        row["Hesp_family"] = "NA"
    try:
        genus = taxon.parent_of_rank(Rank.genus)
    except ValueError:
        genus = None
    if genus is not None and genus.base_name.status is Status.valid:
        row["Hesp_genus"] = genus.valid_name
    elif taxon.rank <= Rank.genus:
        row["Hesp_genus"] = "incertae_sedis"
    else:
        row["Hesp_genus"] = "NA"
    try:
        species = taxon.parent_of_rank(Rank.species)
    except ValueError:
        species = None
    if species is None or species.base_name.status is not Status.valid:
        row["Hesp_specificEpithet"] = "incertae_sedis"
    else:
        row["Hesp_specificEpithet"] = species.base_name.root_name
    # TODO
    # if nam.taxon.rank is Rank.subspecies:
    #     row["Hesp_subspecificEpithet"] = nam.taxon.valid_name.split()[-1]
    # else:
    #     row["Hesp_subspecificEpithet"] = "NA"
    row["Hesp_species_id"] = ""
    if species is not None:
        for tag in species.tags:
            if isinstance(tag, models.tags.TaxonTag.MDD):
                row["Hesp_species_id"] = tag.id
                break
    row["Hesp_name_usages"] = " | ".join(
        _stringify_ce(ce)
        for ce in sorted(
            name.classification_entries, key=lambda ce: ce.article.get_date_object()
        )
    )

    # Other
    # TODO: MDD_subspecificEpithet
    # TODO: MDD_comments
    return {key: value.replace("\\ ", " ") for key, value in row.items()}


def _stringify_ce(ce: ClassificationEntry) -> str:
    page_links = [
        tag.url for tag in ce.get_tags(ce.tags, ClassificationEntryTag.PageLink)
    ]
    author, year = ce.article.taxonomic_authority()
    author = helpers.romanize_russian(author)
    if ce.page:
        year = f"{year}:{ce.page}"
    if page_links:
        year = f"{year}, {', '.join(page_links)}"
    return f"{author} ({year}) (information at {ce.article.get_absolute_url()})"


class DifferenceKind(enum.StrEnum):
    missing_in_mdd = "missing in MDD"
    missing_in_hesp = "missing in Hesp"
    differences = "differences"


@dataclass
class FixableDifference:
    row_idx: int
    col_idx: int
    explanation: str | None
    mdd_column: str
    hesp_value: str
    mdd_value: str
    hesp_row: dict[str, str] = field(repr=False)
    mdd_row: dict[str, str] = field(repr=False)
    hesp_name: Name

    def is_disposable_name(self) -> bool:
        return self.hesp_name.nomenclature_status in (
            NomenclatureStatus.name_combination,
            NomenclatureStatus.incorrect_subsequent_spelling,
        )

    def summary(self) -> str:
        if self.explanation is None:
            return f"{self.hesp_value} (H) / {self.mdd_value} (M) [{self.hesp_name}]"
        return f"{self.explanation}: {self.hesp_value} (H) / {self.mdd_value} (M) [{self.hesp_name}]"

    def print(self) -> None:
        expl = f": {self.explanation}" if self.explanation else ""
        print(f"- {self.mdd_column}{expl} ({self.hesp_name})")
        if self.hesp_value:
            print(f"    - H: {self.hesp_value}")
        if self.mdd_value:
            print(f"    - M: {self.mdd_value}")

    @property
    def kind(self) -> DifferenceKind:
        if not self.hesp_value:
            return DifferenceKind.missing_in_hesp
        if not self.mdd_value:
            return DifferenceKind.missing_in_mdd
        return DifferenceKind.differences

    def apply_to_hesp(self, *, dry_run: bool) -> None:
        match self.mdd_column:
            case "MDD_authority_link":
                if self.hesp_name.original_citation is None:
                    print(f"{self}: skip applying because no original citation")
                    return
                print(
                    f"{self}: set url to {self.mdd_value} for {self.hesp_name.original_citation}"
                )
                if not dry_run:
                    self.hesp_name.original_citation.url = self.mdd_value
            case "MDD_authority_page_link":
                for text in self.mdd_value.split(" | "):
                    if self.hesp_name.page_described is None:
                        tag = models.name.TypeTag.AuthorityPageLink(
                            url=text, confirmed=True, page=""
                        )
                    else:
                        pages = list(
                            models.name.page.get_unique_page_text(
                                self.hesp_name.page_described
                            )
                        )
                        if len(pages) == 1:
                            tag = models.name.TypeTag.AuthorityPageLink(
                                url=text, confirmed=True, page=pages[0]
                            )
                        else:
                            tag = models.name.TypeTag.AuthorityPageLink(
                                url=text, confirmed=True, page=""
                            )
                    print(f"{self}: add tag {tag}")
                    if not dry_run:
                        self.hesp_name.add_type_tag(tag)

            case "MDD_authority_page":
                print(
                    f"{self}: page_described {self.hesp_name.page_described!r} -> {self.mdd_value!r}"
                )
                self.hesp_name.page_described = self.mdd_value

            case "MDD_unchecked_authority_citation":
                print(
                    f"{self}: verbatim_citation {self.hesp_name.verbatim_citation!r} -> {self.mdd_value!r}"
                )
                self.hesp_name.verbatim_citation = self.mdd_value

            case "MDD_type_latitude":
                art = Article(MDD_ARTICLE_ID)
                deg = "°"
                if (
                    self.mdd_row["MDD_type_latitude"]
                    and self.mdd_row["MDD_type_longitude"]
                    and re.fullmatch(
                        r"-?\d+(\.\d+)?", self.mdd_row["MDD_type_latitude"]
                    )
                    and re.fullmatch(
                        r"-?\d+(\.\d+)?", self.mdd_row["MDD_type_longitude"]
                    )
                ):
                    latitude = (
                        f"{self.mdd_row['MDD_type_latitude'][1:]}{deg}S"
                        if self.mdd_row["MDD_type_latitude"][0] == "-"
                        else f"{self.mdd_row['MDD_type_latitude']}{deg}N"
                    )
                    longitude = (
                        f"{self.mdd_row['MDD_type_longitude'][1:]}{deg}W"
                        if self.mdd_row["MDD_type_longitude"][0] == "-"
                        else f"{self.mdd_row['MDD_type_longitude']}{deg}E"
                    )

                    tags = [
                        TypeTag.LocationDetail(
                            f"[Coordinates as given in MDD: {latitude}, {longitude}]",
                            art,
                        ),
                        TypeTag.Coordinates(latitude, longitude),
                    ]
                    print(f"{self}: add tags {tags}")
                    if not dry_run:
                        for tag in tags:
                            self.hesp_name.add_type_tag(tag)
                else:
                    print(f"{self}: skip applying because no valid coordinates")

            case _:
                print(
                    f"{self}: skip applying because no action defined for {self.mdd_column}"
                )

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = {**self.hesp_name.get_adt_callbacks()}
        if self.mdd_column in ("MDD_authority_link", "MDD_authority_page_link"):
            if self.hesp_value:
                callbacks["open_hesp"] = functools.partial(_open_urls, self.hesp_value)
            if self.mdd_value:
                callbacks["open_mdd"] = functools.partial(_open_urls, self.mdd_value)
        return callbacks


def _open_urls(urls: str) -> None:
    for url in urls.split(" | "):
        subprocess.run(["open", url], check=False)


def compare_column(
    hesp_row: dict[str, str],
    mdd_row: dict[str, str],
    *,
    hesp_name: Name,
    mdd_column: str,
    compare_func: Callable[[str, str], object] | None = Levenshtein.distance,
    counts: dict[str, int],
    row_idx: int,
    col_idx: int,
) -> FixableDifference | None:
    mdd_value = mdd_row.get(mdd_column, "")
    hesp_column = mdd_column.replace("MDD", "Hesp")
    hesp_value = hesp_row.get(hesp_column, "")
    match (bool(hesp_value), bool(mdd_value)):
        case (True, False):
            counts[f"{mdd_column} missing in MDD"] += 1
            explanation: str | None = "H only"
        case (False, True):
            counts[f"{mdd_column} missing in Hesperomys"] += 1
            explanation = "M only"
        case (True, True):
            if hesp_value != mdd_value:
                if mdd_column in ("MDD_type_latitude", "MDD_type_longitude"):
                    try:
                        hesp_float = float(hesp_value)
                        mdd_float = float(mdd_value)
                        if abs(hesp_float - mdd_float) < 0.1:
                            return None
                    except ValueError:
                        pass
                if (
                    LIMIT_AUTH_LINKS
                    and mdd_column == "MDD_authority_link"
                    and "biodiversitylibrary.org" not in hesp_value
                    and "//doi.org" not in hesp_value
                ):
                    return None
                if mdd_column in ("MDD_type_latitude", "MDD_type_longitude"):
                    hesp_full = f"{hesp_row['Hesp_type_latitude']} / {hesp_row['Hesp_type_longitude']}"
                    mdd_full = f"{mdd_row['MDD_type_latitude']} / {mdd_row['MDD_type_longitude']}"
                    explanation = f"{hesp_full} (H) / {mdd_full} (M)"
                else:
                    comparison = f"{hesp_value} (H) / {mdd_value} (M)"
                    if compare_func is not None:
                        extra = compare_func(hesp_value, mdd_value)
                        comparison = f"{extra}: {comparison}"
                        explanation = comparison
                    else:
                        explanation = None
                counts[f"{mdd_column} differences"] += 1
            else:
                return None
        case (False, False):
            return None
    return FixableDifference(
        row_idx=row_idx,
        col_idx=col_idx,
        explanation=explanation,
        mdd_column=mdd_column,
        hesp_value=hesp_value,
        mdd_value=mdd_value,
        hesp_row=hesp_row,
        mdd_row=mdd_row,
        hesp_name=hesp_name,
    )


def compare_year(hesp_year: str, mdd_year: str) -> int:
    try:
        return abs(int(hesp_year) - int(mdd_year))
    except ValueError:
        return 1000


COLUMN_RENAMES = {"MDD original combination": "MDD_original_combination"}
REMOVED_COLUMNS = {"citation_status", "author_status", "type_locality_status"}

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[list[T]]:
    it = iter(iterable)
    while chunk := list(itertools.islice(it, n)):
        yield chunk


def process_value_for_sheets(value: str) -> str | int:
    if value.isdigit():
        return int(value)
    return value


def run_gspread_test() -> None:
    options = get_options()
    gc = gspread.oauth()
    sheet = gc.open(options.mdd_sheet)
    worksheet = sheet.get_worksheet_by_id(options.mdd_worksheet_gid)
    worksheet.update_cell(1, 1, "MDD_syn_ID_test")


def pprint_nonempty(row: dict[str, str]) -> None:
    pprint.pp({key: value for key, value in row.items() if value})


def combine_rank_and_id(rank: Rank, id: int) -> int:
    value = f"{rank.value + 100}{id}"
    return int(value)


def run(
    *,
    dry_run: bool = True,
    taxon: Taxon,
    max_names: int | None = None,
    higher: bool = False,
) -> None:
    options = get_options()
    backup_path = (
        options.data_path
        / "mdd_names"
        / datetime.datetime.now(tz=datetime.UTC).isoformat()
    )
    backup_path.mkdir(parents=True, exist_ok=True)

    print("downloading MDD names... ")
    try:
        gc = gspread.oauth()
        sheet = gc.open(options.mdd_sheet)
    except google.auth.exceptions.RefreshError:
        print("need to refresh token")
        token_path = Path("~/.config/gspread/authorized_user.json").expanduser()
        token_path.unlink(missing_ok=True)
        gc = gspread.oauth()
        sheet = gc.open(options.mdd_sheet)

    worksheet = sheet.get_worksheet_by_id(
        options.mdd_higher_worksheet_gid if higher else options.mdd_worksheet_gid
    )
    raw_rows = worksheet.get()
    headings = raw_rows[0]
    column_to_idx = {heading: i for i, heading in enumerate(headings, start=1)}
    rows = [dict(zip(headings, row, strict=False)) for row in raw_rows[1:]]
    hesp_id_to_mdd_id = {
        int(row["Hesp_id"]): row["MDD_syn_ID"] for row in rows if row.get("Hesp_id")
    }
    print(f"done, {len(rows)} found")

    print("backing up MDD names... ")
    with (backup_path / "mdd_names.csv").open("w") as file:
        writer = csv.writer(file)
        for row in raw_rows:
            writer.writerow(row)
    print(f"done, backup at {backup_path}")

    if higher:
        hesp_names = [
            name
            for group in [Group.high, Group.family, Group.genus]
            for name in export.get_names_for_export(
                taxon,
                ages={AgeClass.extant, AgeClass.recently_extinct},
                group=group,
                min_rank_for_age_filtering=Rank.species,
            )
        ]
        hesp_id_to_name: dict[int, tuple[Name, Taxon | None]] = {}
        for name in hesp_names:
            if name.status.is_base_name():
                for corresponding_taxon in Taxon.select_valid().filter(
                    Taxon.base_name == name
                ):
                    hesp_id_to_name[
                        combine_rank_and_id(corresponding_taxon.rank, name.id)
                    ] = (name, corresponding_taxon)
            else:
                hesp_id_to_name[combine_rank_and_id(name.taxon.rank, name.id)] = (
                    name,
                    name.taxon,
                )
    else:
        hesp_names = export.get_names_for_export(
            taxon,
            ages={AgeClass.extant, AgeClass.recently_extinct},
            group=Group.species,
            min_rank_for_age_filtering=Rank.species,
        )
        hesp_id_to_name = {name.id: (name, None) for name in hesp_names}
    need_initials = mdd_diff.get_need_initials_authors(hesp_names)
    unused_hesp_ids = set(hesp_id_to_name.keys())
    counts: dict[str, int] = Counter()
    missing_in_hesp: list[tuple[str, int, dict[str, str]]] = []
    fixable_differences: list[FixableDifference] = []
    max_mdd_id = 0

    for row_idx, mdd_row in getinput.print_every_n(
        enumerate(rows, start=2), label="MDD names"
    ):
        if max_names is not None and row_idx > max_names:
            break
        mdd_id = int(mdd_row["MDD_syn_ID"])
        max_mdd_id = max(max_mdd_id, mdd_id)
        if "Hesp_id" not in mdd_row:
            missing_in_hesp.append(("missing in H", row_idx, mdd_row))
            continue
        hesp_id = resolve_hesp_id(mdd_row["Hesp_id"])
        if hesp_id is None:
            missing_in_hesp.append(("missing in H", row_idx, mdd_row))
            continue
        if hesp_id not in unused_hesp_ids and higher:
            validity = mdd_row.get("MDD_validity", "").lower()
            if validity == "synonym":
                name = Name(hesp_id)
                try:
                    rank = name.taxon.rank
                except Name.DoesNotExist:
                    rank = None
            else:
                try:
                    rank = Rank[mdd_row.get("MDD_validity", "").lower()]
                except KeyError:
                    rank = None
            if rank is not None:
                combined_id = combine_rank_and_id(rank, hesp_id)
                if combined_id in unused_hesp_ids:
                    hesp_id = combined_id
        if hesp_id not in unused_hesp_ids:
            if hesp_id in hesp_id_to_name:
                message = "already matched"
            else:
                message = "invalid Hesp id"
            missing_in_hesp.append((message, row_idx, mdd_row))
            continue
        unused_hesp_ids.remove(hesp_id)
        name, maybe_taxon = hesp_id_to_name[hesp_id]
        hesp_row = get_hesp_row(name, need_initials, hesp_id_to_mdd_id, maybe_taxon)

        for column in headings:
            if column in OMITTED_COLUMNS:
                continue
            compare_func: Callable[[str, str], object] | None
            match column:
                case "MDD_year":
                    compare_func = compare_year
                case _:
                    compare_func = None
            if diff := compare_column(
                hesp_row,
                mdd_row,
                mdd_column=column,
                counts=counts,
                row_idx=row_idx,
                col_idx=column_to_idx[column],
                compare_func=compare_func,
                hesp_name=name,
            ):
                fixable_differences.append(diff)
        if mdd_row["Hesp_id"] != str(hesp_id):
            fixable_differences.append(
                FixableDifference(
                    row_idx=row_idx,
                    col_idx=column_to_idx["Hesp_id"],
                    explanation="Hesp id format differs",
                    mdd_column="Hesp_id",
                    hesp_value=str(hesp_id),
                    mdd_value=mdd_row["Hesp_id"],
                    hesp_row=hesp_row,
                    mdd_row=mdd_row,
                    hesp_name=name,
                )
            )

        # Can happen if Hesp name got redirected
        if name.id != hesp_id and not higher:
            fixable_differences.append(
                FixableDifference(
                    row_idx=row_idx,
                    col_idx=column_to_idx["Hesp_id"],
                    explanation="Hesp id is different from name id",
                    mdd_column="Hesp_id",
                    hesp_value=str(name.id),
                    mdd_value=mdd_row["Hesp_id"],
                    hesp_row=hesp_row,
                    mdd_row=mdd_row,
                    hesp_name=name,
                )
            )

    missing_in_mdd = []
    if max_names is None:
        for hesp_id in unused_hesp_ids:
            name, maybe_taxon = hesp_id_to_name[hesp_id]
            hesp_row = get_hesp_row(name, need_initials, hesp_id_to_mdd_id, maybe_taxon)
            new_mdd_row = {"MDD_syn_ID": str(max_mdd_id + 1), "Hesp_id": str(hesp_id)}
            max_mdd_id += 1
            for mdd_column in headings:
                hesp_column = mdd_column.replace("MDD", "Hesp")
                if hesp_column in hesp_row:
                    new_mdd_row[mdd_column] = hesp_row[hesp_column]
            missing_in_mdd.append(new_mdd_row)

    with (backup_path / "summary.txt").open("w") as file:
        for f in (sys.stdout, file):
            print("Report:", file=f)
            if max_names is None:
                print(f"Total MDD names: {len(rows)}", file=f)
                print(f"Total Hesp names: {len(hesp_names)}", file=f)
                print(f"Missing in Hesp: {len(missing_in_hesp)}", file=f)
                print(f"Missing in MDD: {len(missing_in_mdd)}", file=f)
            for key, value in sorted(counts.items()):
                print(f"{key}: {value}", file=f)

    if max_names is None and missing_in_mdd:
        getinput.print_header(f"Missing in MDD {len(missing_in_mdd)}")
        for row in missing_in_mdd[:10]:
            pprint_nonempty(row)
        add_all = getinput.yes_no("Add all?")
        if add_all:
            for batch in batched(missing_in_mdd, 500):
                rows_to_add = [
                    [
                        process_value_for_sheets(row.get(column, ""))
                        for column in headings
                    ]
                    for row in batch
                ]
                if not dry_run:
                    worksheet.append_rows(rows_to_add)
        else:
            ask_individually = getinput.yes_no("Ask individually?")
            if ask_individually:
                for row in missing_in_mdd:
                    if not getinput.yes_no("Add?"):
                        continue
                    row_list = [
                        process_value_for_sheets(row.get(column, ""))
                        for column in headings
                    ]
                    if not dry_run:
                        worksheet.append_row(row_list)

        with (backup_path / "missing-in-mdd.csv").open("w") as file:
            writer = csv.writer(file)
            missing_in_mdd_headings = list(missing_in_mdd[0])
            writer.writerow(missing_in_mdd_headings)
            for row in missing_in_mdd:
                writer.writerow(
                    [row.get(column, "") for column in missing_in_mdd_headings]
                )

    fixable_differences = sorted(
        fixable_differences,
        key=lambda x: (
            x.mdd_column,
            x.kind,
            x.is_disposable_name(),
            x.hesp_value,
            x.mdd_value,
        ),
    )
    if fixable_differences:
        with (backup_path / "fixable-differences.csv").open("w") as file:
            dict_writer = csv.DictWriter(
                file,
                fieldnames=[
                    "row_idx",
                    "col_idx",
                    "explanation",
                    "mdd_column",
                    "hesp_value",
                    "mdd_value",
                    "hesp_id",
                    "mdd_id",
                    "MDD_species",
                    "MDD_original_combination",
                    "applied",
                ],
            )
            dict_writer.writeheader()
            for (mdd_column, kind, is_combination), group_iter in itertools.groupby(
                fixable_differences,
                key=lambda x: (x.mdd_column, x.kind, x.is_disposable_name()),
            ):
                group = list(group_iter)
                header = f"{kind.name} for {mdd_column} ({'name combinations; ' if is_combination else ''}{len(group)})"
                getinput.print_header(header)
                for diff in group:
                    diff.print()
                print(header)
                choice = getinput.choose_one_by_name(
                    ["mdd_edit", "ask_individually", "hesp_edit", "skip"],
                    allow_empty=False,
                    history_key="overall_choice",
                )
                updates_to_make = []
                for diff in group:
                    should_add_to_mdd = False
                    should_add_to_hesp = False
                    match choice:
                        case "mdd_edit":
                            should_add_to_mdd = True
                        case "hesp_edit":
                            should_add_to_hesp = True
                        case "ask_individually":
                            diff.hesp_name.display()
                            if diff.hesp_name.original_citation:
                                diff.hesp_name.original_citation.display()
                            diff.print()
                            if diff.mdd_value.startswith("http"):
                                subprocess.check_call(["open", diff.mdd_value])
                            individual_choice = getinput.choose_one_by_name(
                                ["mdd_edit", "hesp_edit", "skip"],
                                allow_empty=False,
                                history_key="individual_choice",
                                callbacks=diff.get_adt_callbacks(),
                            )
                            match individual_choice:
                                case "mdd_edit":
                                    should_add_to_mdd = True
                                case "hesp_edit":
                                    should_add_to_hesp = True
                    if should_add_to_mdd:
                        updates_to_make.append(
                            gspread.cell.Cell(
                                row=diff.row_idx,
                                col=diff.col_idx,
                                value=process_value_for_sheets(diff.hesp_value),
                            )
                        )
                    elif should_add_to_hesp:
                        diff.apply_to_hesp(dry_run=dry_run)
                    dict_writer.writerow(
                        {
                            "row_idx": diff.row_idx,
                            "col_idx": diff.col_idx,
                            "explanation": diff.explanation,
                            "mdd_column": diff.mdd_column,
                            "hesp_value": diff.hesp_value,
                            "mdd_value": diff.mdd_value,
                            "hesp_id": diff.hesp_name.id,
                            "mdd_id": diff.mdd_row["MDD_syn_ID"],
                            "MDD_species": diff.mdd_row.get(
                                "MDD_species", diff.mdd_row.get("MDD_taxon", "")
                            ),
                            "MDD_original_combination": diff.mdd_row[
                                "MDD_original_combination"
                            ],
                            "applied": str(int(should_add_to_hesp)),
                        }
                    )

                if dry_run:
                    print("Make change:", updates_to_make)
                else:
                    done = 0
                    print("Applying changes for column", mdd_column)
                    for batch in batched(updates_to_make, 500):
                        worksheet.update_cells(batch)
                        done += len(batch)
                        print(f"Done {done}/{len(updates_to_make)}")
                        if len(batch) == 500:
                            time.sleep(5)

    if max_names is None and missing_in_hesp:
        getinput.print_header(f"Missing in Hesp {len(missing_in_hesp)}")
        for _, _, row in missing_in_hesp[:10]:
            pprint_nonempty(row)
        with (backup_path / "missing-in-hesp.csv").open("w") as file:
            writer = csv.writer(file)
            missing_in_hesp_headings = ["match_status", *missing_in_hesp[0][2]]
            writer.writerow(missing_in_hesp_headings)
            for match_status, _, row in missing_in_hesp:
                writer.writerow(
                    [
                        match_status,
                        *[row.get(column, "") for column in missing_in_hesp[0][2]],
                    ]
                )
        for match_status, row_idx, row in sorted(
            missing_in_hesp, key=lambda triple: triple[1], reverse=True
        ):
            getinput.print_header(
                f'{row["MDD_original_combination"]} = {row.get("MDD_species", row.get("MDD_taxon", ""))}'
            )
            print(match_status)
            pprint_nonempty(row)
            if not getinput.yes_no("Remove?"):
                continue
            if not dry_run:
                worksheet.delete_rows(row_idx)

    print("Done. Data saved at", backup_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--taxon", nargs="?", default="Mammalia")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--gspread-test", action="store_true", default=False)
    parser.add_argument("--max-names", type=int, default=None)
    parser.add_argument(
        "--higher",
        action="store_true",
        default=False,
        help="Run on names above the species group",
    )
    args = parser.parse_args()
    if args.gspread_test:
        run_gspread_test()
        return
    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        print("Invalid taxon", args.taxon)
        sys.exit(1)
    run(taxon=root, dry_run=args.dry_run, max_names=args.max_names, higher=args.higher)


if __name__ == "__main__":
    main()
