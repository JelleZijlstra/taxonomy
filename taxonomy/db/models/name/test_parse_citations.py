from taxonomy.db.models.name.parse_citations import ParsedCitation, parse_citation


def test_parse_roman_page_range() -> None:
    assert parse_citation(
        "Annuaire du Musee zoologique 21: lxxiii-lxxxii."
    ) == ParsedCitation(
        series=None, volume="21", issue=None, start_page="lxxiii", end_page="lxxxii"
    )


def test_parse_russian_volume_and_pages() -> None:
    assert parse_citation("Сб. тр. Зоол. музея МГУ. Т.18. С.5-43.") == ParsedCitation(
        series=None, volume="18", issue=None, start_page="5", end_page="43"
    )


def test_parse_semicolon_roman_page() -> None:
    assert parse_citation(
        "Bull. Biol. Dept. Sun. Yat-sen Univ. 9, i."
    ) == ParsedCitation(
        series=None, volume="9", issue=None, start_page="i", end_page=None
    )


def test_parse_appendix_roman_page() -> None:
    assert parse_citation("Faune Vert. Suisse, 1: appendix, iii.") == ParsedCitation(
        series=None, volume="1", issue=None, start_page="iii", end_page=None
    )


def test_do_not_parse_editor_initial_as_page() -> None:
    assert parse_citation(
        "New Tertiary bats. - in: V. Hanak, I. Horacek et J. Gaisler (eds.)"
    ) == ParsedCitation(None, None, None, None, None)


def test_book_preliminary_pagination_is_not_start_page() -> None:
    assert parse_citation(
        "University of Chicago Press, 1:x + 609 pp., 19 pls."
    ) == ParsedCitation(None, "1", None, None, None)
