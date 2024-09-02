from types import SimpleNamespace
from typing import Any

from taxonomy.db.constants import ArticleType
from taxonomy.db.models.person import AuthorTag, Person

from .article import Article


class _FakeArticle(Article):
    for field in Article.clirm_fields:
        locals()[field] = property(lambda self, field=field: self.__dict__.get(field))  # type: ignore[misc]

    def __new__(cls, data: dict[str, Any]) -> Any:
        obj = object.__new__(cls)
        obj.__init__(None)  # type: ignore[misc]
        for k in Article.clirm_fields:
            obj.__dict__[k] = data.get(k)
        return obj


class _FakePerson(Person):
    for field in Person.clirm_fields:
        locals()[field] = property(lambda self, field=field: self.__dict__.get(field))  # type: ignore[misc]

    def __new__(cls, data: dict[str, Any]) -> Any:
        obj = object.__new__(cls)
        obj.__init__(None)  # type: ignore[misc]
        for k in Person.clirm_fields:
            obj.__dict__[k] = data.get(k)
        return obj


class _FakeCG(SimpleNamespace):
    def get_citable_name(self) -> str:
        return self.name


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
        "citation_group": _FakeCG(name="Journal of Mammalogy"),
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
