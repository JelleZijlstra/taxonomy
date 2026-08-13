import pytest

from taxonomy.applicator import coverage as recommendations
from taxonomy.db.constants import Rank
from taxonomy.db.models import Article, ClassificationEntry


def test_coverage_includes_proposed_objects_and_reports_unexpected() -> None:
    article = Article.virtual(name="Solem.pdf")
    covered = ClassificationEntry.virtual(
        article=article, name="Endodontidae", rank=Rank.family, page="12", parent=None
    )
    unexpected = ClassificationEntry.virtual(
        article=article,
        name="Punctoidea",
        rank=Rank.superfamily,
        page="11",
        parent=None,
    )
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": "check_coverage",
            "scope": {
                "model": "ClassificationEntry",
                "where": {"article": {"model": "Article", "ref": "article"}},
            },
            "completeness": "complete",
            "covered_objects": [{"model": "ClassificationEntry", "ref": "covered"}],
            "unexpected": "report",
            "confidence": "high",
            "reason": "The source scope is complete.",
            "evidence": [{"kind": "scope", "text": "Pages 11-12."}],
        },
        1,
    )

    plan = recommendations.build_plan(
        [row],
        references={"article": article, "covered": covered, "unexpected": unexpected},
        model_registry={"ClassificationEntry": ClassificationEntry},
        query=lambda _model, _where: [],
    )

    assert plan.actions[0].unexpected == (unexpected,)


def test_review_expands_scope_guards_and_covered_objects(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": "check_coverage",
            "scope": {
                "model": "ClassificationEntry",
                "where": {"article": {"model": "Article", "ref": "article"}},
            },
            "completeness": "complete",
            "covered_objects": [{"model": "ClassificationEntry", "ref": "covered"}],
            "unexpected": "report",
            "confidence": "high",
            "reason": "The source scope is complete.",
            "evidence": [{"kind": "scope", "text": "Pages 11-12."}],
        },
        1,
    )

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "    - scope guard: article=Article(ref='article')" in output
    assert "    - covered object: ClassificationEntry(ref='covered')" in output
