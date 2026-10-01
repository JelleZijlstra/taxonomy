"""File that gets loaded on startup of the taxonomy shell.

Contents overlap with shell.py, which defines "commands".

"""

from collections import defaultdict
from collections.abc import Container, Iterable

from taxonomy import getinput
from taxonomy.db.constants import AgeClass, Group, Rank, Status
from taxonomy.db.models import Article, Book, Name, Person, Taxon


def unrecorded_taxa(root: Taxon) -> None:
    def has_occurrence(taxon: Taxon) -> bool:
        return taxon.occurrences.count() > 0

    if root.age is AgeClass.fossil:
        return

    if root.rank is Rank.species:
        if not has_occurrence(root) and not any(
            has_occurrence(child) for child in root.children
        ):
            print(root)
    else:
        for taxon in root.children:
            unrecorded_taxa(taxon)


def locless_names(
    genus: Taxon,
    attribute: str = "type_locality",
    age: AgeClass | None = AgeClass.removed,
    min_year: int | None = None,
    exclude: Container["Taxon"] = frozenset(),
) -> list[Name]:
    if age is AgeClass.removed:
        age = genus.age
    nams = list(
        genus.names_missing_field(
            attribute, age=age, min_year=min_year, exclude=exclude
        )
    )
    for nam in nams:
        nam.display()
    return nams


def hp(
    author: str, year: int, page: int, *, uncited_only: bool = False
) -> tuple[list[Article], list[Name]]:
    return h(author, year, page, uncited_only=uncited_only)


def h(
    author: str, year: int, page: int | None = None, *, uncited_only: bool = False
) -> tuple[list[Article], list[Name]]:
    authors = Person.select_valid().filter(Person.family_name == author)
    nams = []
    arts = []
    for aut in authors:
        for art in aut.get_sorted_derived_field("articles"):
            if art.numeric_year() != year:
                continue
            if page is not None and not art.is_page_in_range(page):
                continue
            arts.append(art)
        for nam in aut.get_sorted_derived_field("names"):
            if nam.numeric_year() != year:
                continue
            if page is not None and (
                nam.page_described is None or str(page) not in nam.page_described
            ):
                continue
            if uncited_only and nam.original_citation is not None:
                continue
            nams.append(nam)
    getinput.print_header(f"Articles by {author} ({year})")
    for art in arts:
        print(repr(art))
    getinput.print_header(f"Names by {author} ({year})")
    for nam in nams:
        nam.display(full=False)
        indent = " " * 8
        if nam.verbatim_citation:
            print(f"{indent}{nam.verbatim_citation}")
        if nam.citation_group:
            print(f"{indent}{nam.citation_group}")
    return arts, nams


class _NamesGetter:
    def __init__(self, group: Group) -> None:
        self._cache: dict[str, list[Name]] | None = None
        self._group = group

    def __getattr__(self, attr: str) -> list[Name]:
        return list(
            Name.filter(
                Name.group == self._group,
                Name.status != Status.removed,
                Name.root_name == attr,
            )
        )

    def __dir__(self) -> Iterable[str]:
        self._fill_cache()
        assert self._cache is not None
        yield from self._cache.keys()
        yield from super().__dir__()

    def _fill_cache(self) -> None:
        if self._cache is not None:
            return
        self._cache = defaultdict(list)
        for nam in Name.filter(
            Name.group == self._group, Name.status != Status.removed
        ):
            self._cache[nam.root_name].append(nam)

    def clear_cache(self) -> None:
        self._cache = None


ns = _NamesGetter(Group.species)
gs = _NamesGetter(Group.genus)


def print_prefix(prefix: str) -> None:
    books = Book.select_valid().filter(Book.dewey.startswith(prefix))
    for book in sorted(
        books,
        key=lambda book: (
            book.dewey,
            tuple(
                author.get_description(family_first=True)
                for author in book.get_authors()
            ),
        ),
    ):
        print(book.dewey, repr(book))
