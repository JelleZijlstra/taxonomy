from taxonomy.db.models.article.publication_date import (
    extract_publication_date_evidence,
)


def test_extracts_explicit_publication_date_without_stored_date() -> None:
    evidence = extract_publication_date_evidence(
        ["Received 13 September 1993. Accepted 4 May 1994. Issued June 17, 1994."], None
    )

    assert [item.date for item in evidence] == ["1994-06-17"]
    assert evidence[0].matched_text == "Issued June 17, 1994"
    assert evidence[0].page_number == 1


def test_extracts_unlabelled_date_from_first_page_header() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "JOURNAL OF MAMMALOGY Volume 75 June 1994\n"
                "The intended article title and subtitle"
            )
        ],
        "The intended article title and subtitle",
    )

    assert [item.date for item in evidence] == ["1994-06"]
    assert evidence[0].kind == "bibliographic header"


def test_volume_number_is_not_mistaken_for_day() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "JOURNAL OF MAMMALOGY Volume 15 June 1994\n"
                "The intended article title and subtitle"
            )
        ],
        "The intended article title and subtitle",
    )

    assert [item.date for item in evidence] == ["1994-06"]


def test_number_on_previous_line_is_not_mistaken_for_day() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "JOURNAL OF MAMMALOGY Number 5\n"
                "June 1976\n"
                "The intended article title and subtitle"
            )
        ],
        "The intended article title and subtitle",
    )

    assert [item.date for item in evidence] == ["1976-06"]


def test_unlabelled_header_without_article_title_is_not_evidence() -> None:
    evidence = extract_publication_date_evidence(
        ["JOURNAL OF MAMMALOGY Volume 75 June 1994\nA different article"],
        "The intended article title and subtitle",
    )

    assert evidence == ()


def test_date_on_separate_header_line_is_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "JOURNAL OF MAMMALOGY Volume 75\n"
                "June 1994\n"
                "The intended article title and subtitle"
            )
        ],
        "The intended article title and subtitle",
    )

    assert [item.date for item in evidence] == ["1994-06"]


def test_unlabelled_repository_date_before_title_is_not_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Uploaded by a user on 17 June 2013\n"
                "The intended article title and subtitle"
            )
        ],
        "The intended article title and subtitle",
    )

    assert evidence == ()


def test_later_unlabelled_header_requires_article_title() -> None:
    evidence = extract_publication_date_evidence(
        [
            "Repository cover sheet",
            "JOURNAL OF MAMMALOGY Volume 75 June 1994\nA different article",
        ],
        "The intended article title and subtitle",
    )

    assert evidence == ()


def test_more_precise_explicit_date_wins_over_header() -> None:
    evidence = extract_publication_date_evidence(
        ["JOURNAL June 1994. Publication date: June 17, 1994."], None
    )

    assert [item.date for item in evidence] == ["1994-06-17"]


def test_jstor_cover_date_is_not_pdf_publication_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "This content downloaded from www.jstor.org/stable/123. "
                "Journal of Mammalogy, June 1994."
            )
        ],
        None,
    )

    assert evidence == ()


def test_researchgate_cover_date_is_not_publication_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "See discussions, stats, and author profiles for this publication at: "
                "https://www.researchgate.net/publication/235806050\n"
                "Paleogene sea snakes from the Eastern part of Tethys\n"
                "Article in Russian Journal of Herpetology - January 1997\n"
                "All content following this page was uploaded in 2014."
            ),
            (
                "Russian Journal of Herpetology Vol. 4, No. 2, 1997\n"
                "Paleogene sea snakes from the Eastern part of Tethys\n"
                "Submitted September 1, 1997."
            ),
        ],
        "Paleogene sea snakes from the Eastern part of Tethys",
    )

    assert evidence == ()


def test_combined_issue_is_not_exact_month_evidence() -> None:
    evidence = extract_publication_date_evidence(
        ["BOLLETTINO. N. 5 e 6. Maggio e Giugno 1875. SOMMARIO."], None
    )

    assert evidence == ()


def test_ambiguous_numeric_date_is_not_independent_evidence() -> None:
    evidence = extract_publication_date_evidence(["Published 03/04/1994."], None)

    assert evidence == ()


def test_acceptance_for_publication_is_not_publication_date() -> None:
    evidence = extract_publication_date_evidence(
        ["Received 20 February 2019; accepted for publication 23 October 2019."], None
    )

    assert evidence == ()


def test_punctuated_received_date_is_not_publication_date() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Z. zool. Syst. Evolut.-forsch. 29 (1991) 304-311\n"
                "Received on 18. January 1991\n"
                "Adult tooth crown morphology in the Typhlonectidae"
            )
        ],
        "Adult tooth crown morphology in the Typhlonectidae",
    )

    assert evidence == ()


def test_dotted_day_in_issue_header_is_publication_date() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Zoologischer Anzeiger XXXVII. Band.\n"
                "30. Mai 1911\n"
                "Über einige Säugetiere von Celebes"
            )
        ],
        "Über einige Säugetiere von Celebes",
    )

    assert [item.date for item in evidence] == ["1911-05-30"]


def test_comma_after_day_in_issue_header_is_publication_date() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Zoologische Abhandlungen\nAusgegeben: 30, April 1980\n"
                "Example title long enough for matching"
            )
        ],
        "Example title long enough for matching",
    )

    assert [item.date for item in evidence] == ["1980-04-30"]


def test_part_number_is_not_mistaken_for_day() -> None:
    evidence = extract_publication_date_evidence(
        ["Museum Bulletin Volume 10, Part 3 February 1975\nExample article title"],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["1975-02"]


def test_issue_number_range_is_not_mistaken_for_day() -> None:
    evidence = extract_publication_date_evidence(
        ["Journal Volume 26, No. 1-4 March 2009\nExample article title"],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["2009-03"]


def test_month_range_is_not_exact_month_evidence() -> None:
    evidence = extract_publication_date_evidence(
        ["Journal Volume 19, enero-junio, 2012\nExample article title"],
        "Example article title",
    )

    assert evidence == ()


def test_body_citation_after_title_is_not_header_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Museum Bulletin\nExample article title\n"
                "In March 1930 this Bulletin published an earlier catalogue."
            )
        ],
        "Example article title",
    )

    assert evidence == ()


def test_sitzungsbericht_date_is_not_publication_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Nr. 3. Sitzungs-Bericht der Gesellschaft naturforschender Freunde "
                "zu Berlin vom 20. März 1900.\nExample article title"
            )
        ],
        "Example article title",
    )

    assert evidence == ()


def test_library_accession_stamp_is_not_publication_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "UNIV. OF IOWA LIBRARIES - SHELVE WITH BOUND VOLUMES\n"
                "JUL 12 1930\nMuseum Bulletin\nExample article title"
            )
        ],
        "Example article title",
    )

    assert evidence == ()


def test_bulletin_number_is_not_mistaken_for_day() -> None:
    evidence = extract_publication_date_evidence(
        ["Museum Bulletin 17 January 1930\nExample article title"],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["1930-01"]

    evidence = extract_publication_date_evidence(
        ["Museum Bulletin Volume 1 17 January 1930\nExample article title"],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["1930-01"]


def test_day_interrupted_by_issue_number_is_recovered() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Zoologischer Anzeiger Band 23. 19. No. 610. März 1900\n"
                "Example article title"
            )
        ],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["1900-03-19"]


def test_ocr_damaged_two_digit_days_are_recovered() -> None:
    assert [
        item.date
        for item in extract_publication_date_evidence(
            ["Senckenbergiana Band 37 I 5.April 1956\nExample article title"],
            "Example article title",
        )
    ] == ["1956-04-15"]
    assert [
        item.date
        for item in extract_publication_date_evidence(
            ["Zoologischer Anzeiger Band 21. 2 l März 1898\nExample article title"],
            "Example article title",
        )
    ] == ["1898-03-21"]


def test_day_split_from_month_by_running_title_is_recovered() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "SALAMANDRA 55(2) 15Stumpffia May 2019 ISSN 0036-3375\n"
                "Diversity of Stumpffia frogs"
            )
        ],
        "Diversity of Stumpffia frogs",
    )

    assert [item.date for item in evidence] == ["2019-05-15"]


def test_ocr_one_after_issue_number_is_day() -> None:
    evidence = extract_publication_date_evidence(
        ["Occasional Papers Number 58 I June 1979\nExample article title"],
        "Example article title",
    )

    assert [item.date for item in evidence] == ["1979-06-01"]


def test_ocr_template_noise_between_day_and_month_is_recovered() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Special Publications Number 77 xx xx31 XXXX May 2023\n"
                "Example title long enough"
            )
        ],
        "Example title long enough",
    )
    assert [item.date for item in evidence] == ["2023-05-31"]

    evidence = extract_publication_date_evidence(
        [
            (
                "Special Publications Number 80 xx 18 XXXX July2010 2024\n"
                "Example title long enough"
            )
        ],
        "Example title long enough",
    )
    assert [item.date for item in evidence] == ["2024-07-18"]


def test_typesetting_timestamp_split_across_lines_is_not_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Katalog Eck!!! 19.10.2004\n13:34 Uhr Seite 1\n"
                "Zoologische Abhandlungen\nExample article title"
            )
        ],
        "Example article title",
    )

    assert evidence == ()


def test_retrospective_online_publication_date_is_not_print_evidence() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "Transactions Volume 94 Issue 02 June 2003\n"
                "Conohyus giganteus\n"
                "Published online: 26 July 2007."
            )
        ],
        "Conohyus giganteus",
    )

    assert evidence == ()


def test_iso_day_is_not_truncated() -> None:
    evidence = extract_publication_date_evidence(["Published: 2025-09-22."], None)

    assert [item.date for item in evidence] == ["2025-09-22"]


def test_typesetting_timestamp_is_not_publication_date() -> None:
    evidence = extract_publication_date_evidence(
        [
            (
                "AMNH NOVITATES\n"
                "Wednesday Jul 25 2001 01:36 PM 2000\n"
                "Allen Press - DTPro System\n"
                "Number 3327, 15 pp., 8 figures, 1 table\n"
                "April 26, 2001\n"
                "Persistent Fontanelles in Rodent Skulls"
            )
        ],
        "Persistent Fontanelles in Rodent Skulls",
    )

    assert [item.date for item in evidence] == ["2001-04-26"]
