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
                callbacks={**cg.get_adt_callbacks(), "open_all": open_all},
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
                    "Accept anyway? ", callbacks=cg.get_adt_callbacks()
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
