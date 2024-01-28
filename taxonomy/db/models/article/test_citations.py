from . import citations, utils


def test_citepaper() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citepaper(article)
        == "Zijlstra, J.S., Madern, P.A. and Hoek Ostende, L.W. van den. 2010. New"
        " genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles. Journal of Mammalogy"
        " 91(4):860-873. doi:10.1644/09-MAMM-A-208.1"
    )


def test_citenormal() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citenormal(article)
        == "*Zijlstra, J.S., Madern, P.A. and Hoek Ostende, L.W. van den. 2010."
        " [http://dx.doi.org/10.1644/09-MAMM-A-208.1 New genus and two new species"
        " of Pleistocene oryzomyines (Cricetidae: Sigmodontinae) from Bonaire,"
        " Netherlands Antilles] (subscription required). Journal of Mammalogy"
        " 91(4):860-873."
    )


def test_citelemurnews() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citelemurnews(article)
        == "Zijlstra, J.S.; Madern, P.A.; Hoek Ostende, L.W. van den. 2010. New genus"
        " and two new species of Pleistocene oryzomyines (Cricetidae: Sigmodontinae)"
        " from Bonaire, Netherlands Antilles. Journal of Mammalogy 91: 860–873."
    )


def test_citebzn() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citebzn(article)
        == "<b>Zijlstra, J.S., Madern, P.A. & Hoek Ostende, L.W. van den</b> 2010. New"
        " genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles. <i>Journal of"
        " Mammalogy</i>, <b>91</b>: 860-873."
    )


def test_citejhe() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citejhe(article)
        == "Zijlstra, J.S., Madern, P.A., Hoek Ostende, L.W. van den, 2010. New genus"
        " and two new species of Pleistocene oryzomyines (Cricetidae: Sigmodontinae)"
        " from Bonaire, Netherlands Antilles. Journal of Mammalogy 91, 860-873."
    )


def test_citepalaeontology() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citepalaeontology(article)
        == "ZIJLSTRA, J. S., MADERN, P. A. and HOEK OSTENDE, L. W. van den. 2010. New"
        " genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles. <i>Journal of"
        " Mammalogy</i>, <b>91</b>, 860-873."
    )


def test_citejpal() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citejpal(article)
        == "ZIJLSTRA, J. S., P. A. MADERN, and L. W. van den HOEK OSTENDE 2010. New"
        " genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles. Journal of Mammalogy,"
        " 91:860-873."
    )


def test_citepalevol() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citepalevol(article)
        == "Zijlstra, J.S., Madern, P.A., Hoek Ostende, L.W. van den, 2010. New genus"
        " and two new species of Pleistocene oryzomyines (Cricetidae: Sigmodontinae)"
        " from Bonaire, Netherlands Antilles. Journal of Mammalogy 91, 860-873."
    )


def test_citejvp() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citejvp(article)
        == "Zijlstra, J. S., P. A. Madern, and L. W. van den Hoek Ostende. 2010. New"
        " genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles. Journal of Mammalogy"
        " 91:860-873."
    )


def test_citebibtex() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citebibtex(article)
        == """@article{Zijlstra201091860,
\tauthor = "Zijlstra, J. S. and Madern, P. A. and Hoek Ostende, L. W. van den",
\tyear = "2010",
\ttitle = "{New genus and two new species of Pleistocene oryzomyines (Cricetidae: Sigmodontinae) from Bonaire, Netherlands Antilles}",
\tjournal = "Journal of Mammalogy",
\tvolume = "91",
\tnumber = "4",
\tpages = "860--873",
}"""
    )


def test_citezootaxa() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citezootaxa(article)
        == "Zijlstra, J.S., Madern, P.A. & Hoek Ostende, L.W. van den (2010) New genus"
        " and two new species of Pleistocene oryzomyines (Cricetidae: Sigmodontinae)"
        " from Bonaire, Netherlands Antilles. <i>Journal of Mammalogy</i>, 91,"
        " 860-873."
    )


def test_citewp() -> None:
    article = utils.make_journal_article()
    assert (
        citations.citewp(article)
        == "{{cite journal | last1 = Zijlstra | first1 = Jelle S. | last2 = Madern |"
        " first2 = Paulina A. | last3 = Hoek Ostende | first3 = Lars W. |"
        " year = 2010 | doi = 10.1644/09-MAMM-A-208.1 | pages = 860-873 | title ="
        " New genus and two new species of Pleistocene oryzomyines (Cricetidae:"
        " Sigmodontinae) from Bonaire, Netherlands Antilles | journal = Journal of"
        " Mammalogy | volume = 91 | issue = 4}}"
    )
