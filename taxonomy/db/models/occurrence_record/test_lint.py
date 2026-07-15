from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from taxonomy.db.constants import ObservationKind, OccurrenceBasis
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location
from taxonomy.db.models.occurrence_record import (
    OccurrenceRecord,
    OccurrenceRecordTag,
    lint,
)
from taxonomy.db.models.occurrence_record.lint import (
    check_basis_tags,
    check_location,
    check_taxon,
)
from taxonomy.db.models.taxon import Taxon


def _record(**kwargs: object) -> OccurrenceRecord:
    kwargs.setdefault("tags", ())
    record = SimpleNamespace(**kwargs)
    record.has_tag = lambda tag_cls: any(
        isinstance(tag, tag_cls) for tag in record.tags
    )
    record.get_tags = lambda tags, tag_cls: (
        tag for tag in tags if isinstance(tag, tag_cls)
    )
    record.remove_tags = lambda tag_cls: setattr(
        record,
        "tags",
        tuple(tag for tag in record.tags if not isinstance(tag, tag_cls)),
    )
    return cast(OccurrenceRecord, record)


def test_taxon_lint_autofills_from_classification_entry() -> None:
    taxon = cast(Taxon, SimpleNamespace())
    taxon.resolve_redirect = lambda: taxon  # type: ignore[method-assign]
    mapped_name = SimpleNamespace(taxon=taxon)
    record = _record(
        taxon=None, classification_entry=SimpleNamespace(mapped_name=mapped_name)
    )

    assert list(check_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is taxon


def test_observation_kind_requires_observation_basis() -> None:
    record = _record(
        basis=OccurrenceBasis.listing,
        tags=(OccurrenceRecordTag.ObservationKind(ObservationKind.acoustic),),
    )

    messages = list(check_basis_tags(record, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "ObservationKind requires observation basis [basis_tags]"
    )


def test_location_lint_autofills_from_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    location = cast(Location, object())
    record = _record(
        location=None,
        locality_text="verbatim locality",
        tags=(OccurrenceRecordTag.LocationHint("canonical locality"),),
    )
    monkeypatch.setattr(
        lint,
        "_get_location_by_name",
        lambda name: location if name == "canonical locality" else None,
    )

    assert list(check_location(record, LintConfig(autofix=True))) == []
    assert record.location is location
    assert record.tags == ()
