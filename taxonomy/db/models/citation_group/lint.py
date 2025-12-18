"""Lint steps for citation groups."""

import functools
import re
import subprocess
from collections import Counter, defaultdict
from collections.abc import Container, Iterable
from datetime import UTC, datetime

from taxonomy import config, getinput
from taxonomy.apis import bhl
from taxonomy.db import constants, helpers, models
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint

from .cg import CitationGroup, CitationGroupStatus, CitationGroupTag

# Shared regex defaults and helper functions for validating volume/issue/series
DEFAULT_VOLUME_REGEX = r"(Suppl\. )?\d{1,4}"
DEFAULT_ISSUE_REGEX = r"\d{1,3}|\d{1,2}-\d{1,2}|Suppl\. \d{1,2}"


def get_volume_regex(cg: CitationGroup) -> str:
    tag = cg.get_tag(CitationGroupTag.VolumeRegex)
    return tag.text if tag is not None else DEFAULT_VOLUME_REGEX


def describe_volume_regex(cg: CitationGroup) -> str:
    tag = cg.get_tag(CitationGroupTag.VolumeRegex)
    return f"regex {tag.text!r}" if tag is not None else "default volume regex"


def get_issue_regex(cg: CitationGroup) -> str:
    tag = cg.get_tag(CitationGroupTag.IssueRegex)
    return tag.text if tag is not None else DEFAULT_ISSUE_REGEX


def describe_issue_regex(cg: CitationGroup) -> str:
    tag = cg.get_tag(CitationGroupTag.IssueRegex)
    return f"regex {tag.text!r}" if tag is not None else "default issue regex"


def requires_series(cg: CitationGroup) -> bool:
    return cg.get_tag(CitationGroupTag.MustHaveSeries) is not None


def get_series_regex(cg: CitationGroup) -> str | None:
    tag = cg.get_tag(CitationGroupTag.SeriesRegex)
    return tag.text if tag is not None else None


def remove_unused_ignores(cg: CitationGroup, unused: Container[str]) -> None:
    new_tags = []
    for tag in cg.tags:
        if (
            isinstance(tag, CitationGroupTag.IgnoreLintCitationGroup)
            and tag.label in unused
        ):
            print(f"{cg}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    cg.tags = new_tags  # type: ignore[assignment]


def get_ignores(cg: CitationGroup) -> Iterable[IgnoreLint]:
    return cg.get_tags(cg.tags, CitationGroupTag.IgnoreLintCitationGroup)


LINT = Lint(CitationGroup, get_ignores, remove_unused_ignores)


@functools.cache
def get_biblio_pages() -> set[str]:
    options = config.get_options()
    biblio_dir = options.taxonomy_repo / "docs" / "biblio"
    return {path.stem for path in biblio_dir.glob("*.md")}


@LINT.add("check_status")
def check_status(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    if cg.target is None:
        if cg.status not in (CitationGroupStatus.normal, CitationGroupStatus.deleted):
            yield f"CG of status {cg.status} must have target"
    elif cg.status in (CitationGroupStatus.normal, CitationGroupStatus.deleted):
        yield "CG of status normal may not have target"


@LINT.add("check_tags")
def check_tags(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    for tag in cg.tags:
        if tag is CitationGroupTag.MustHave or isinstance(
            tag, CitationGroupTag.MustHaveAfter
        ):
            if (
                not cg.archive
                and not cg.get_tag(CitationGroupTag.CitationGroupURL)
                and not cg.get_tag(CitationGroupTag.BHLBibliography)
            ):
                yield "has MustHave tag but no URL"
        if isinstance(tag, CitationGroupTag.MustHaveAfter):
            if issue := helpers.is_valid_year(tag.year):
                yield f"invalid MustHaveAfterTag {tag}: {issue}"
        if isinstance(tag, CitationGroupTag.MustHaveSeries) and not cg.get_tag(
            CitationGroupTag.SeriesRegex
        ):
            yield "MustHaveSeries tag but no SeriesRegex tag"
        if isinstance(tag, CitationGroupTag.OnlineRepository):
            yield "use of deprecated OnlineRepository tag"
        if isinstance(tag, (CitationGroupTag.ISSN, CitationGroupTag.ISSNOnline)):
            # TODO: check that the checksum digit is right
            if not re.fullmatch(r"^\d{4}-\d{3}[X\d]$", tag.text):
                yield f"invalid ISSN {tag}"
        if isinstance(tag, CitationGroupTag.BHLBibliography):
            if not tag.text.isnumeric():
                yield f"invalid BHL tag {tag}"
        if isinstance(tag, CitationGroupTag.YearRange):
            if issue := helpers.is_valid_year(tag.start):
                yield f"invalid start year in {tag}: {issue}"
            if tag.end and (issue := helpers.is_valid_year(tag.end)):
                yield f"invalid end year in {tag}: {issue}"
            if tag.start and tag.end and int(tag.start) > int(tag.end):
                yield f"{tag}: start is after end"
            if tag.end and int(tag.end) > datetime.now(tz=UTC).year:
                yield f"{tag} is predicting the future"
        if isinstance(tag, CitationGroupTag.BiblioNote):
            if tag.text not in get_biblio_pages():
                yield f"references non-existent page {tag.text!r}"
        # TODO: if there is a Predecessor, check that the YearRange tags make sense
        if isinstance(
            tag,
            (
                CitationGroupTag.SeriesRegex,
                CitationGroupTag.VolumeRegex,
                CitationGroupTag.IssueRegex,
            ),
        ):
            if issue := helpers.is_valid_regex(tag.text):
                yield f"invalid tag {tag}: {issue}"
        if isinstance(tag, CitationGroupTag.PageRegex):
            if tag.start_page_regex is not None:
                if issue := helpers.is_valid_regex(tag.start_page_regex):
                    yield f"invalid start_page_regex in tag {tag}: {issue}"
                yield "start page regex tag is deprecated"
            if tag.pages_regex is not None:
                if issue := helpers.is_valid_regex(tag.pages_regex):
                    yield f"invalid pages_regex in tag {tag}: {issue}"


@LINT.add("format_tags")
def format_tags(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    tags = sorted(set(cg.tags))
    counts = Counter(type(tag) for tag in tags)
    for tag_type, count in counts.items():
        if count > 1 and tag_type not in (
            CitationGroupTag.Predecessor,
            CitationGroupTag.CitationGroupURL,
            CitationGroupTag.ISSN,
            CitationGroupTag.ISSNOnline,
            CitationGroupTag.BHLBibliography,
            CitationGroupTag.MayHaveIdentifier,
            CitationGroupTag.MustHaveIdentifier,
            CitationGroupTag.AlternativeName,
        ):
            yield f"multiple {tag_type} tags"

    if tuple(tags) != tuple(cg.tags):
        message = "changing tags"
        getinput.print_diff(sorted(cg.tags), tags)
        if cfg.autofix:
            print(f"{cg}: {message}")
            cg.tags = tags  # type: ignore[assignment]
        else:
            yield message


@LINT.add("too_many_bhl")
def check_too_many_bhl_bibliographies(
    cg: CitationGroup, cfg: LintConfig
) -> Iterable[str]:
    num_bhl_biblios = len(cg.get_bhl_title_ids())
    if num_bhl_biblios > 5:
        yield f"has {num_bhl_biblios} BHL bibliographies"


@LINT.add("infer_bhl_from_children")
def infer_bhl_biblio_from_children(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    if cg.has_tag(CitationGroupTag.SkipExtraBHLBibliographies):
        return
    if cg.type is not constants.ArticleType.JOURNAL:
        return
    bibliographies: dict[int, list[object]] = defaultdict(list)
    for nam in cg.get_names():
        for tag in nam.get_tags(nam.type_tags, models.name.TypeTag.AuthorityPageLink):
            if biblio := bhl.get_bhl_bibliography_from_url(tag.url):
                bibliographies[biblio].append(nam)
    for art in cg.get_articles():
        if art.url:
            if biblio := bhl.get_bhl_bibliography_from_url(art.url):
                bibliographies[biblio].append(art)
    if not bibliographies:
        return
    existing = cg.get_bhl_title_ids()
    for biblio in existing:
        bibliographies.pop(biblio, None)
    if not bibliographies:
        return
    message = f"inferred BHL tags {bibliographies} from child articles and names"
    if cfg.autofix:
        print(f"{cg}: {message}")
        for biblio in bibliographies:
            cg.add_tag(CitationGroupTag.BHLBibliography(text=str(biblio)))
    else:
        yield message


@LINT.add("infer_bhl_biblio")
def infer_bhl_biblio(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    if cg.get_bhl_title_ids():
        return
    if cg.type is not constants.ArticleType.JOURNAL:
        return
    if LINT.is_ignoring_lint(cg, "infer_bhl_biblio"):
        yield "ignoring lint"
        return
    title_dict = bhl.get_title_to_data()
    name = cg.name.casefold()
    if name not in title_dict:
        return
    candidates = title_dict[name]
    if len(candidates) > 1:
        urls = [cand["TitleURL"] for cand in candidates]
        message = f"multiple possible BHL entries: {urls}"
        if cfg.manual_mode:
            getinput.print_header(cg)
            print(message)

            def open_all() -> None:
                for cand in candidates:
                    subprocess.check_call(["open", cand["TitleURL"]])

            data = getinput.choose_one(
                candidates,
                callbacks={**cg.get_wrapped_adt_callbacks(), "open_all": open_all},
                history_key=(cg, "infer_bhl_biblio"),
            )
            if data is None:
                return
            # help pyanalyze, which picks "object" as the type otherwise
            assert isinstance(data, dict)
        else:
            return
    else:
        data = candidates[0]
        active_years = cg.get_active_year_range()
        if active_years is None:
            message = f"no active years, but may match {data['TitleURL']}"
            if cfg.manual_mode:
                print(f"{cg}: {message}")
                subprocess.check_call(["open", data["TitleURL"]])
                if not getinput.yes_no(
                    "Accept anyway? ", callbacks=cg.get_wrapped_adt_callbacks()
                ):
                    return
            else:
                yield message
            return
        my_start_year, my_end_year = active_years
        if not data["StartYear"]:
            return
        if my_start_year < int(data["StartYear"]) or (
            data["EndYear"] and my_end_year > int(data["EndYear"])
        ):
            yield (
                f"active years {my_start_year}-{my_end_year} don't match"
                f" {data['TitleURL']} {data['StartYear']}-{data['EndYear']}"
            )
            return
    message = f"inferred BHL tag {data['TitleID']}"
    if cfg.autofix:
        print(f"{cg}: {message}")
        cg.add_tag(CitationGroupTag.BHLBibliography(text=str(data["TitleID"])))
    else:
        yield message


@LINT.add("bhl_year_range")
def infer_bhl_year_range(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    title_ids = cg.get_bhl_title_ids()
    if not title_ids:
        return
    if cg.get_tag(CitationGroupTag.BHLYearRange):
        return
    years: set[int] = set()
    for title_id in title_ids:
        title_metadata = bhl.get_title_metadata(title_id)
        if title_metadata is None:
            continue
        for item in title_metadata["Items"]:
            try:
                years.add(int(item["Year"]))
            except KeyError:
                pass
            except ValueError:
                print(item)
            try:
                years.add(int(item["EndYear"]))
            except KeyError:
                pass
            except ValueError:
                print(item)
    if not years:
        return
    # Add one year on either end in case the stated years are off a little
    tag = CitationGroupTag.BHLYearRange(
        start=str(min(years) - 1), end=str(max(years) + 1)
    )
    message = f"add tag {tag}"
    if cfg.autofix:
        print(f"{cg}: {message}")
        cg.add_tag(tag)
    else:
        yield message


@LINT.add("have_identifier")
def add_have_identifier_tags(cg: CitationGroup, cfg: LintConfig) -> Iterable[str]:
    if cg.type is not constants.ArticleType.JOURNAL:
        return
    # Gather article years present in this citation group
    years_all: list[int] = []
    articles = list(cg.get_articles())
    for art in articles:
        year = art.numeric_year()
        if year:
            years_all.append(year)
    if not years_all:
        return
    earliest_art_year = min(years_all)
    latest_art_year = max(years_all)

    def add_or_expand(
        tag_cls: type[CitationGroupTag],
        ident: constants.ArticleIdentifier,
        desired_min: int | None,
        desired_max: int | None,
    ) -> Iterable[str]:
        # Find existing tags of this class for the identifier
        existing: CitationGroupTag | None = None
        for tag in cg.get_tags(cg.tags, tag_cls):
            if tag.identifier == ident:
                existing = tag
                break
        if existing is None:
            # Add new tag
            new_tag = tag_cls(ident, min_year=desired_min, max_year=desired_max)
            message = f"add tag {new_tag}"
            if cfg.autofix:
                print(f"{cg}: {message}")
                cg.add_tag(new_tag)
            else:
                yield message
            return
        # Expand existing tag if possible
        existing_min = getattr(existing, "min_year", None)
        existing_max = getattr(existing, "max_year", None)
        new_min = existing_min
        new_max = existing_max
        if (
            existing_min is not None
            and desired_min is not None
            and desired_min < existing_min
        ):
            new_min = None if desired_min == earliest_art_year else desired_min
        if (
            existing_max is not None
            and desired_max is not None
            and desired_max > existing_max
        ):
            new_max = None if desired_max == latest_art_year else desired_max
        if new_min == existing_min and new_max == existing_max:
            return
        updated = tag_cls(ident, min_year=new_min, max_year=new_max)
        message = f"expand tag {existing} -> {updated}"
        if cfg.autofix:
            print(f"{cg}: {message}")
            tags = list(cg.tags or [])
            tags = [t for t in tags if t != existing]
            tags.append(updated)
            cg.tags = tags  # type: ignore[assignment]
        else:
            yield message

    for ident in constants.ArticleIdentifier:
        # Count per year and total across group for this identifier
        per_year: dict[int, int] = defaultdict(int)
        total = 0
        for art in articles:
            year = art.numeric_year()
            if not year:
                continue
            if art.get_article_identifier(ident):
                per_year[year] += 1
                total += 1
        if total == 0:
            continue
        present_years = sorted(per_year)
        min_year_present = present_years[0]
        max_year_present = present_years[-1]

        # For MayHave, add a couple of years at the margins
        desired_min: int | None = (
            None if min_year_present == earliest_art_year else min_year_present - 2
        )
        desired_max: int | None = (
            None if max_year_present == latest_art_year else max_year_present + 2
        )

        # Always manage MayHaveIdentifier over the observed presence range
        yield from add_or_expand(
            CitationGroupTag.MayHaveIdentifier, ident, desired_min, desired_max
        )

        # Independently, add/expand MustHaveIdentifier if coverage threshold is met
        years_with_at_least_5 = sorted(
            year for year, count in per_year.items() if count >= 5
        )
        if total >= 10 and len(years_with_at_least_5) >= 2:
            desired_min = (
                None
                if years_with_at_least_5[0] == earliest_art_year
                else years_with_at_least_5[0]
            )
            desired_max = (
                None
                if years_with_at_least_5[-1] == latest_art_year
                else years_with_at_least_5[-1]
            )
            yield from add_or_expand(
                CitationGroupTag.MustHaveIdentifier, ident, desired_min, desired_max
            )


@LINT.add("identifier_tag_consistency")
def check_identifier_tag_consistency(
    cg: CitationGroup, cfg: LintConfig
) -> Iterable[str]:
    """Ensure there is at most one MayHaveIdentifier and one MustHaveIdentifier per
    identifier, and that the May range contains the Must range if both exist.
    """

    def lower_value(v: int | None) -> int:
        return -(10**9) if v is None else v

    def upper_value(v: int | None) -> int:
        return 10**9 if v is None else v

    for ident in constants.ArticleIdentifier:
        may_tags = [
            t
            for t in cg.get_tags(cg.tags, CitationGroupTag.MayHaveIdentifier)
            if t.identifier == ident
        ]
        must_tags = [
            t
            for t in cg.get_tags(cg.tags, CitationGroupTag.MustHaveIdentifier)
            if t.identifier == ident
        ]

        if len(may_tags) > 1:
            yield f"multiple MayHaveIdentifier tags for {ident.name}"
        if len(must_tags) > 1:
            yield f"multiple MustHaveIdentifier tags for {ident.name}"

        if not may_tags or not must_tags:
            continue

        # Validate that each Must range is contained in each May range
        for may in may_tags:
            may_min = getattr(may, "min_year", None)
            may_max = getattr(may, "max_year", None)
            for must in must_tags:
                must_min = getattr(must, "min_year", None)
                must_max = getattr(must, "max_year", None)
                if lower_value(may_min) > lower_value(must_min) or upper_value(
                    may_max
                ) < upper_value(must_max):
                    may_str = f"{may_min or '-inf'}–{may_max or '+inf'}"
                    must_str = f"{must_min or '-inf'}–{must_max or '+inf'}"
                    yield (
                        f"identifier range mismatch for {ident.name}: MayHave {may_str} does not contain MustHave {must_str}"
                    )
