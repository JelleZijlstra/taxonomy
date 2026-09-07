from scripts import bat_literature_most_wanted as most_wanted


def make_record(**kwargs: str) -> most_wanted.SourceRecord:
    return most_wanted.SourceRecord(source="test", **kwargs)


def test_group_records_uses_strong_identifiers() -> None:
    records = [
        make_record(doi="10.1000/example", raw_reference="First rendering"),
        make_record(doi="10.1000/EXAMPLE", raw_reference="Second rendering"),
        make_record(raw_reference="An unrelated reference that is long enough to key"),
    ]
    groups = most_wanted.group_records(records)
    assert sorted(map(len, groups)) == [1, 2]


def test_group_records_does_not_merge_short_loc_cit() -> None:
    records = [
        make_record(raw_reference="loc. cit."),
        make_record(raw_reference="loc. cit."),
    ]
    assert len(most_wanted.group_records(records)) == 2


def test_classify_prefers_local_and_batlit_exclusions() -> None:
    assert (
        most_wanted.classify(
            [make_record(taxonomy_article_kind="electronic", batlit_id="ABC")], None
        )[0]
        == "excluded_local"
    )
    assert most_wanted.classify([make_record(batlit_id="ABC")], None)[0] == (
        "excluded_batlit"
    )


def test_doi_landing_page_is_not_availability_evidence() -> None:
    record = make_record(url="https://doi.org/10.1000/example")
    assert not record.has_online_evidence()
    assert most_wanted.classify([record], None)[0] == "wanted"


def test_direct_pdf_is_availability_evidence() -> None:
    record = make_record(url="https://example.org/paper.pdf")
    assert record.has_online_evidence()
    assert most_wanted.classify([record], None)[0] == "excluded_online"


def test_markdown_reference_preserves_italics() -> None:
    assert most_wanted.markdown_reference("A <i>bat</i> paper") == "A *bat* paper"


def test_markdown_reference_merges_adjacent_italic_runs() -> None:
    assert (
        most_wanted.markdown_reference("<i>longicau</i><i>datus</i>")
        == "*longicaudatus*"
    )


def test_input_source_article_is_not_an_acquisition_target() -> None:
    assert "Chiroptera (HMW)" in most_wanted.INPUT_SOURCE_ARTICLE_NAMES
