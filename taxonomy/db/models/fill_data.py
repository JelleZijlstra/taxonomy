"""System for ensuring that names with original citations have their data filled out."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import clirm

from taxonomy import getinput
from taxonomy.command_set import CommandSet
from taxonomy.db import models
from taxonomy.db.constants import ArticleKind, NameDataLevel, OriginalCitationDataLevel

from .article import Article, ArticleTag
from .base import BaseModel
from .citation_group import CitationGroup
from .person import Person

_finished_papers: set[int] = set()


def _name_sort_key(nam: models.Name) -> tuple[str, int]:
    try:
        return ("", nam.numeric_page_described())
    except (TypeError, ValueError):
        return (nam.page_described or "", 0)


def get_names(paper: Article) -> list[models.Name]:
    return sorted(
        models.Name.select_valid().filter(models.Name.original_citation == paper),
        key=_name_sort_key,
    )


def fill_data_from_paper(
    paper: Article, *, ask_before_opening: bool = False, should_open: bool = True
) -> bool:
    if paper.id in _finished_papers:
        return True
    if paper.has_tag(ArticleTag.NeedsTranslation):
        print(f"{paper.name}: skipping because of NeedsTranslation tag")
        _finished_papers.add(paper.id)
        return True
    if paper.has_tag(ArticleTag.NonOriginal):
        print(f"{paper.name}: skipping because of NonOriginal tag")
        _finished_papers.add(paper.id)
        return True
    if paper.kind is ArticleKind.no_copy:
        print(f"{paper.name}: skipping because no copy available")
        _finished_papers.add(paper.id)
        return True

    opened = False

    nams = get_names(paper)
    nams_below_level = [
        nam
        for nam in nams
        if nam.original_citation_data_level()[0]
        is not OriginalCitationDataLevel.all_required_data
        and nam.name_data_level()[0] is not NameDataLevel.nothing_needed
    ]
    if nams_below_level:
        print(f"{paper.name}: {len(nams_below_level)} names (fill_data_from_paper)")
        if ask_before_opening:
            edit_names_interactive(paper, should_open=should_open)
            if paper.has_tag(ArticleTag.NeedsTranslation):
                print(f"{paper.name}: skipping because of NeedsTranslation tag")
                _finished_papers.add(paper.id)
                return True
            nams = get_names(paper)

        for nam in nams:
            nam.load()
            while (
                nam.original_citation_data_level()[0]
                is not OriginalCitationDataLevel.all_required_data
                and nam.name_data_level()[0] is not NameDataLevel.nothing_needed
            ):
                nam.display()
                nam.format()
                if not opened:
                    if should_open:
                        paper.openf()
                    paper.add_to_history()
                    print(f"filling data from {paper.name}")
                    paper.specify_authors()
                opened = True
                current_level, reason = nam.original_citation_data_level()
                print(f"Level: {current_level.name.upper()} ({reason})")
                if list(nam.get_empty_required_fields()):
                    print(nam, "described at", nam.page_described)
                    nam.fill_required_fields()
                else:
                    nam.fill_field("type_tags")

    if not opened:
        _finished_papers.add(paper.id)
        return True
    return False


def fill_data_from_articles(
    arts: Sequence[Article],
    *,
    ask_before_opening: bool = False,
    skip_nofile: bool = True,
    specify_authors: bool = False,
) -> None:
    total = len(arts)
    if total == 0:
        print("no articles found")
        return
    done = 0
    for i, art in enumerate(arts):
        percentage = (i / total) * 100
        print(f"{percentage:.03}% ({i}/{total}) {art.path}/{art.name}")
        getinput.flush()
        if skip_nofile and not art.isfile():
            print("skipping NOFILE article")
            continue
        if specify_authors:
            art.specify_authors()
        if fill_data_from_paper(art, ask_before_opening=ask_before_opening):
            done += 1
        else:
            # Redo this to make sure we finished the paper.
            fill_data_from_paper(art, ask_before_opening=ask_before_opening)
    print(f"{done}/{total} ({(done / total) * 100:.03}%) done")


def fill_data_for_names(
    nams: Iterable[models.Name],
    *,
    min_year: int | None = None,
    field: str | None = None,
    ask_before_opening: bool = True,
    skip_nofile: bool = True,
) -> None:
    """Calls fill_required_fields() for all names in this taxon."""

    def should_include(nam: models.Name) -> bool:
        if nam.original_citation is None:
            return False
        if nam.original_citation.lacks_full_text():
            return False
        if field is not None and (
            getattr(nam, field) is not None or field not in nam.get_required_fields()
        ):
            return False
        if min_year is not None and nam.year is not None:
            return min_year <= nam.numeric_year()
        else:
            return True

    citations = sorted(
        {
            nam.original_citation
            for nam in nams
            if should_include(nam) and nam.original_citation is not None
        },
        key=lambda art: (art.path or "NOFILE", art.name),
    )
    fill_data_from_articles(
        citations, ask_before_opening=ask_before_opening, skip_nofile=skip_nofile
    )


def edit_names_interactive(
    art: Article, field: str = "corrected_original_name", *, should_open: bool = True
) -> None:
    if should_open:
        art.openf()
    art.add_to_history()
    art.specify_authors()
    while True:
        obj = models.Name.getter(field).get_one(
            prompt=f"{field}> ",
            callbacks={
                "o": art.openf,
                "d": lambda: display_names(art),
                "f": lambda: display_names(art, full=True),
                "t": lambda: display_names(art, omit_if_done=True),
                "edit": art.edit,
            },
        )
        if obj is None:
            break
        obj.display()
        level, reason = obj.original_citation_data_level()
        print(f"Level: {level.name.upper()} ({reason})")
        obj.edit()


def display_names(
    art: Article, *, full: bool = False, omit_if_done: bool = False
) -> None:
    print(repr(art))
    new_names = get_names(art)
    if new_names:
        print(f"New names ({len(new_names)}):")
        levels = []
        for nam in new_names:
            level, reason = nam.original_citation_data_level()
            levels.append(level)
            if omit_if_done and level is OriginalCitationDataLevel.all_required_data:
                continue
            if full:
                nam.display(full=True)
                print(f"    Level: {level.name.upper()} ({reason})")
            else:
                desc = nam.get_description(include_taxon=True, full=False).rstrip()
                print(f"{desc} ({level.name.upper()}: {reason})")
        print("Current level:", min(levels).name.upper())


CS = CommandSet(
    "fill_data", "Commands for filling out data based on original citations"
)


@CS.register
def fill_data_from_paper_interactive(
    paper: Article | None = None,
    *,
    ask_before_opening: bool = True,
    should_open: bool = True,
) -> None:
    if paper is None:
        paper = BaseModel.get_value_for_foreign_class(
            "paper", Article, allow_none=False
        )
    if paper is None:
        return
    fill_data_from_paper(
        paper, ask_before_opening=ask_before_opening, should_open=should_open
    )


@CS.register
def fill_data_from_author(
    author: Person | None = None, *, skip_nofile: bool = True
) -> None:
    if author is None:
        author = Person.getter(None).get_one()
    if author is None:
        return
    arts = author.get_sorted_derived_field("articles")
    fill_data_from_articles(
        sorted(arts, key=lambda art: art.path),
        ask_before_opening=True,
        skip_nofile=skip_nofile,
    )


@CS.register
def fill_data_for_children(
    paper: Article | None = None, *, skip_nofile: bool = False
) -> None:
    if paper is None:
        paper = BaseModel.get_value_for_foreign_class(
            "paper", Article, allow_none=False
        )
    if paper is None:
        return
    children = sorted(
        Article.select_valid().filter(Article.parent == paper),
        key=lambda child: (child.numeric_start_page(), child.name),
    )
    fill_data_from_articles(children, ask_before_opening=True, skip_nofile=skip_nofile)
    fill_data_from_paper(paper)


@CS.register
def fill_data_random(batch_size: int = 20, *, ask_before_opening: bool = True) -> None:
    for count, art in enumerate(
        Article.select_valid().order_by(clirm.Func("RANDOM")).limit(batch_size)
    ):
        if count > 0:
            percentage = (count / batch_size) * 100
        else:
            percentage = 0.0
        getinput.show(f"({count}; {percentage:.03}%) {art.name}")
        try:
            fill_data_from_paper(art, ask_before_opening=ask_before_opening)
        except getinput.StopException:
            return


@CS.register
def fill_data_on_taxon() -> None:
    taxon = models.Taxon.getter(None).get_one("taxon> ")
    if taxon is None:
        return
    taxon.fill_data_for_names()


@CS.register
def fill_data_reverse_order(
    *,
    ask_before_opening: bool = True,
    max_count: int | None = 500,
    include_lint: bool = True,
) -> None:
    done = 0
    # static analysis: ignore[undefined_attribute]
    for i, art in enumerate(Article.select_valid().order_by(Article.id.desc())):
        if max_count is not None and i > max_count:
            return
        if i > 0:
            percentage = (done / i) * 100
        else:
            percentage = 0.0
        getinput.show(f"({i}; {percentage:.03}%) {art.name}")
        result = fill_data_from_paper(art, ask_before_opening=ask_before_opening)
        if include_lint:
            for nam in art.get_new_names():
                if nam.is_lint_clean():
                    continue
                nam.display()
                nam.edit_until_clean()
        if result:
            done += 1


@CS.register
def fill_data_from_folder(
    folder: str | None = None,
    *,
    ask_before_opening: bool = True,
    skip_nofile: bool = True,
) -> None:
    if folder is None:
        folder = Article.getter("path").get_one_key() or ""
    arts = Article.bfind(Article.path.startswith(folder), quiet=True)
    fill_data_from_articles(
        sorted(arts, key=lambda art: art.path),
        ask_before_opening=ask_before_opening,
        skip_nofile=skip_nofile,
    )


@CS.register
def fill_data_from_citation_group(
    cg: CitationGroup | None = None,
    *,
    ask_before_opening: bool = True,
    skip_nofile: bool = True,
) -> None:
    if cg is None:
        cg = CitationGroup.getter("name").get_one()
    if cg is None:
        return

    arts = sorted(cg.get_articles(), key=_article_sort_key)
    fill_data_from_articles(
        arts, ask_before_opening=ask_before_opening, skip_nofile=skip_nofile
    )


def _article_sort_key(art: Article) -> tuple[object, ...]:
    date = art.get_date_object()
    if art.volume:
        try:
            volume = int(art.volume)
        except ValueError:
            volume = 0
    else:
        volume = 0
    start_page = art.numeric_start_page()
    return (date, volume, start_page)


@CS.register
def edit_names_from_article(
    art: Article | None = None, field: str = "corrected_original_name"
) -> None:
    if art is None:
        art = Article.getter("name").get_one()
        if art is None:
            return
    art.display_names()
    edit_names_interactive(art, field=field)
    fill_data_from_paper(art)
