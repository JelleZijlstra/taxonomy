from types import SimpleNamespace
from typing import cast

from taxonomy.db.models.article import Article, ArticleTag, add_data


def test_get_jstor_data_uses_stable_id_and_cleans_pages() -> None:
    pdfcontent = """\
FIRST RECORD OF THE WATER SHREW, SOREX PALUSTRIS, FROM GRAHAM COUNTY, NORTH CAROLINA
Author(s): Stephen B. Frantz
Source: Journal of the North Carolina Academy of Science, Vol. 126, No. 1/2 (Spring/Summer 2010), pp. 61-62 .
Published by: North Carolina Academy of Science
Stable URL: https://www.jstor.org/stable/24336329
Accessed: 1 January 2026

Your use of the JSTOR archive indicates your acceptance of JSTOR's Terms and Conditions of Use
"""
    article = cast(Article, SimpleNamespace(getpdfcontent=lambda: pdfcontent))

    data = add_data.get_jstor_data(article)

    assert "doi" not in data
    assert data["tags"] == [ArticleTag.JSTOR("24336329")]
    assert data["start_page"] == "61"
    assert data["end_page"] == "62"
