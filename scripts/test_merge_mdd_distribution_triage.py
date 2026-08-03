from scripts import merge_mdd_distribution_triage


def make_row(
    *, country: str, evidence: str, review_status: str = "", review_comment: str = ""
) -> merge_mdd_distribution_triage.Row:
    return {
        "mdd_id": "1000001",
        "species": "Example_species",
        "taxon_id": "123",
        "missing_country": country,
        "evidence_records": evidence,
        "review_status": review_status,
        "review_comment": review_comment,
    }


def test_merge_carries_only_review_fields() -> None:
    previous = make_row(
        country="Peru",
        evidence="old evidence",
        review_status="add_to_mdd",
        review_comment="Reviewed from source.",
    )
    new = make_row(country="Peru", evidence="new evidence")

    merged, summary = merge_mdd_distribution_triage.merge_rows([previous], [new])

    assert merged == [
        make_row(
            country="Peru",
            evidence="new evidence",
            review_status="add_to_mdd",
            review_comment="Reviewed from source.",
        )
    ]
    assert summary["carried_reviews"] == 1
    assert summary["untriaged_rows"] == 0


def test_merge_preserves_review_already_in_new_file() -> None:
    previous = make_row(
        country="Peru",
        evidence="old evidence",
        review_status="add_to_mdd",
        review_comment="Old review.",
    )
    new = make_row(
        country="Peru",
        evidence="new evidence",
        review_status="needs_source_review",
        review_comment="New review.",
    )

    merged, summary = merge_mdd_distribution_triage.merge_rows([previous], [new])

    assert merged == [new]
    assert summary["already_reviewed_new_rows"] == 1
    assert summary["carried_reviews"] == 0


def test_merge_counts_new_and_retired_rows() -> None:
    previous = make_row(
        country="Peru",
        evidence="old evidence",
        review_status="add_to_mdd",
        review_comment="Reviewed.",
    )
    new = make_row(country="Bolivia", evidence="new evidence")

    _, summary = merge_mdd_distribution_triage.merge_rows([previous], [new])

    assert summary["matched_rows"] == 0
    assert summary["untriaged_rows"] == 1
    assert summary["retired_reviewed_rows"] == 1
