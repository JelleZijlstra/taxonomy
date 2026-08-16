from taxonomy.db.models.article.lsid import extract_safe_publication_lsid

LSID = "AABBCCDD-1234-4ABC-9DEF-0123456789AB"


def test_extracts_explicit_self_identified_lsid() -> None:
    pages = [
        (
            "Nomenclatural statement. The LSID for this publication is: "
            f"urn:lsid:zoobank.org:pub:{LSID}"
        )
    ]

    match = extract_safe_publication_lsid(pages, "An unrelated stored title")

    assert match is not None
    assert match.lsid == LSID
    assert match.page == 1
    assert match.evidence == "explicit self-identification"


def test_extracts_first_page_header_before_matching_title() -> None:
    title = "A revision of the exceptionally distinctive genus Example"
    pages = [
        (
            f"http://zoobank.org/urn:lsid:zoobank.org:pub:{LSID}\n"
            "A revision of the exceptionally distinctive genus Example\nAuthors"
        )
    ]

    match = extract_safe_publication_lsid(pages, title)

    assert match is not None
    assert match.lsid == LSID
    assert match.evidence == "first-page header before matching title"


def test_rejects_lsid_in_reference() -> None:
    pages = [
        (
            "A revision of the exceptionally distinctive genus Example\nAuthors\n"
            f"References: Cited article. urn:lsid:zoobank.org:pub:{LSID}"
        )
    ]

    assert (
        extract_safe_publication_lsid(
            pages, "A revision of the exceptionally distinctive genus Example"
        )
        is None
    )


def test_rejects_multiple_publication_lsids() -> None:
    other_lsid = "11223344-5678-4ABC-9DEF-0123456789AB"
    pages = [
        (
            f"The LSID for this publication is: urn:lsid:zoobank.org:pub:{LSID}\n"
            f"References: urn:lsid:zoobank.org:pub:{other_lsid}"
        )
    ]

    assert extract_safe_publication_lsid(pages, "Any title") is None


def test_rejects_header_with_mismatching_title() -> None:
    pages = [
        (
            f"urn:lsid:zoobank.org:pub:{LSID}\n"
            "The title of a different article that is long enough to match"
        )
    ]

    assert (
        extract_safe_publication_lsid(
            pages, "A revision of the exceptionally distinctive genus Example"
        )
        is None
    )


def test_accepts_line_breaks_within_lsid() -> None:
    pages = [
        (
            "ZooBank registration: urn:lsid:zoobank.org:pub:"
            "AABBCCDD-1234-4ABC-\n9DEF-0123456789AB"
        )
    ]

    match = extract_safe_publication_lsid(pages, None)

    assert match is not None
    assert match.lsid == LSID


def test_rejects_overlong_uuid_component() -> None:
    pages = [
        (
            "The LSID for this publication is: urn:lsid:zoobank.org:pub:"
            "5DE13597-E734-453B-8703-F42F84F206A38"
        )
    ]

    assert extract_safe_publication_lsid(pages, None) is None


def test_rejects_species_lsid_after_malformed_publication_lsid() -> None:
    pages = [
        (
            "The LSID for this publication is: urn:lsid:zoobank.org:pub:"
            "19A423ED8EAA-4842-9ECF-695876EC5EC0 and the LSID for the species is: "
            f"urn:lsid:zoobank.org:pub:{LSID}"
        )
    ]

    assert extract_safe_publication_lsid(pages, None) is None
