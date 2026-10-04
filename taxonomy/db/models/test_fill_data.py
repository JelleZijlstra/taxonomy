from unittest.mock import Mock

import pytest

from taxonomy import getinput
from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    Group,
    NomenclatureStatus,
    Rank,
    Status,
)
from taxonomy.db.models import Article, Location, Name, Taxon, fill_data


@pytest.mark.parametrize(
    ("results", "expected_count"),
    [([True], 1), ([False, True], 1), ([False, False], 0)],
)
def test_completion_report_counts_verification_pass(
    results: list[bool],
    expected_count: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paper = Article.virtual(name="example.pdf", path="test")
    fill = Mock(side_effect=results)
    monkeypatch.setattr(fill_data, "fill_data_from_paper", fill)
    monkeypatch.setattr(getinput, "flush", lambda: None)
    fill_data.fill_data_from_articles([paper], skip_nofile=False)
    assert fill.call_count == len(results)
    assert f"{expected_count}/1" in capsys.readouterr().out.splitlines()[-1]


@pytest.mark.parametrize("has_original", [False, True])
@pytest.mark.parametrize(
    ("year", "populated", "group", "expected"),
    [
        ("1800", False, Group.species, False),
        ("2000", True, Group.species, False),
        ("2000", False, Group.species, True),
        (None, False, Group.species, True),
        ("2000", False, Group.genus, False),
    ],
)
def test_taxon_filling_applies_filters_with_and_without_original(
    *,
    has_original: bool,
    year: str | None,
    populated: bool,
    group: Group,
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    taxon = Taxon.virtual(valid_name="Example", rank=Rank.genus, age=AgeClass.extant)
    paper = Article.virtual(name="example.pdf", kind=ArticleKind.electronic, tags=())
    nam = Name.virtual(
        taxon=taxon,
        group=group,
        original_rank=Rank.species if group is Group.species else Rank.genus,
        root_name="example",
        corrected_original_name="Example example",
        status=Status.valid,
        nomenclature_status=NomenclatureStatus.available,
        year=year,
        original_citation=paper if has_original else None,
        type_locality=Location.virtual(name="Example") if populated else None,
        type_tags=(),
        tags=(),
    )
    fill_fields = Mock()
    fill_papers = Mock()
    monkeypatch.setattr(Taxon, "all_names", lambda self, **kwargs: {nam})
    monkeypatch.setattr(Name, "load", lambda self: None)
    monkeypatch.setattr(Name, "fill_required_fields", fill_fields)
    monkeypatch.setattr(fill_data, "fill_data_from_articles", fill_papers)

    taxon.fill_data_for_names(
        only_with_original=False, min_year=2000, field="type_locality"
    )

    assert fill_fields.call_count == int(expected and not has_original)
    assert fill_papers.call_args.args[0] == (
        [paper] if expected and has_original else []
    )
