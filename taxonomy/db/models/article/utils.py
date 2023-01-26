from ...constants import ArticleType, DateSource
from .article import Article, ArticleTag
from ..person import Person, AuthorTag

from collections import defaultdict
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from collections.abc import Sequence

SOURCE_PRIORITY = [
    DateSource.external,
    DateSource.internal,
    DateSource.doi_published,
    DateSource.doi_published_online,
    DateSource.doi_published_print,
]


def infer_publication_date_from_tags(tags: Sequence[ArticleTag] | None) -> str | None:
    if not tags:
        return None
    by_source = defaultdict(list)
    for tag in tags:
        if isinstance(tag, ArticleTag.PublicationDate):
            by_source[tag.source].append(tag)
    for source in SOURCE_PRIORITY:
        if tags_of_source := by_source[source]:
            if len(tags_of_source) > 1:
                return None
            return tags_of_source[0].date
    return None


@dataclass
class _FakeArticle(Article):
    __data__: dict[str, Any]
    _dirty: bool = False

    def __getattribute__(self, attr: str) -> Any:
        data = object.__getattribute__(self, "__data__")
        if attr in data:
            return data[attr]
        return super().__getattribute__(attr)


@dataclass
class _FakePerson(Person):
    __data__: dict[str, Any]
    _dirty: bool = False

    def __getattribute__(self, attr: str) -> Any:
        data = object.__getattribute__(self, "__data__")
        if attr in data:
            return data[attr]
        return super().__getattribute__(attr)


def make_journal_article() -> Article:
    """Make a dummy Article for testing."""
    data = {
        "addday": "13",
        "addmonth": "1",
        "addyear": "2011",
        "authors": "Zijlstra, J.S.; Madern, P.A.; Hoek Ostende, L.W. van den",
        "doi": "10.1644/09-MAMM-A-208.1",
        "end_page": "873",
        "path": ["Cricetidae", "Oryzomyini", "Clade D"],
        "issue": "4",
        "journal": "Journal of Mammalogy",
        "name": "Agathaeromys nov.pdf",
        "start_page": "860",
        "title": (
            "New genus and two new species of Pleistocene oryzomyines (Cricetidae:"
            " Sigmodontinae) from Bonaire, Netherlands Antilles"
        ),
        "type": ArticleType.JOURNAL,
        "volume": "91",
        "year": "2010",
        "citation_group": SimpleNamespace(name="Journal of Mammalogy"),
        "author_tags": [
            AuthorTag.Author(
                _FakePerson({"family_name": "Zijlstra", "given_names": "Jelle S."})
            ),
            AuthorTag.Author(
                _FakePerson({"family_name": "Madern", "given_names": "Paulina A."})
            ),
            AuthorTag.Author(
                _FakePerson(
                    {
                        "family_name": "Hoek Ostende",
                        "given_names": "Lars W.",
                        "tussenvoegsel": "van den",
                    }
                )
            ),
        ],
    }
    return _FakeArticle(data)
