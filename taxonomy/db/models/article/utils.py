"""Helpers for creating Articles and similar objects."""

from ..constants import ArticleType
from .article import Article


def make_journal_article() -> Article:
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
    }
    return Article(data)
