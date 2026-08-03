import pytest

from taxonomy.applicator.proposals import (
    ProposalBuilder,
    ProposalLintResult,
    ProposedModel,
    lint_proposals,
    print_lint_results,
)
from taxonomy.db.constants import RegionKind
from taxonomy.db.models import Location, OccurrenceRecord, Region
from taxonomy.db.models.tags import LocationTag


def test_builder_composes_changes_and_contexts_on_one_copy() -> None:
    location = Location.virtual(name="Old name", latitude=None, tags=())
    builder = ProposalBuilder()

    first = builder.copy(location, context="first change")
    first.name = "New name"
    second = builder.copy(location, context="second change")
    second.latitude = "12°N"

    (proposal,) = builder.build()
    assert first is second is proposal.model
    assert proposal.model is not location
    assert proposal.model.name == "New name"
    assert proposal.model.latitude == "12°N"
    assert proposal.contexts == ("first change", "second change")
    assert location.name == "Old name"
    assert location.latitude is None


def test_builder_remaps_foreign_key_to_later_proposal() -> None:
    old_region = Region.virtual(name="Old region", kind=RegionKind.country, tags=())
    location = Location.virtual(name="Place", region=old_region, tags=())
    builder = ProposalBuilder()

    proposed_location = builder.copy(location, context="edit Location")
    proposed_region = builder.copy(old_region, context="edit Region")
    proposed_region.name = "New region"
    builder.build()

    assert proposed_location.region is proposed_region


def test_builder_remaps_model_reference_nested_in_adt_tag() -> None:
    record = OccurrenceRecord.virtual(locality_text="Precise site", tags=())
    location = Location.virtual(
        name="Precise site", tags=(LocationTag.CoordinatesFromOccurrenceRecord(record),)
    )
    builder = ProposalBuilder()

    proposed_location = builder.copy(location, context="edit Location")
    proposed_record = builder.copy(record, context="edit occurrence")
    builder.build()

    (tag,) = proposed_location.tags
    assert isinstance(tag, LocationTag.CoordinatesFromOccurrenceRecord)
    assert tag.occurrence_record is proposed_record


def test_new_proposal_has_no_origin() -> None:
    builder = ProposalBuilder()

    location = builder.create(Location, context="new Location", name="New", tags=())

    assert location.is_virtual
    assert location.virtual_origin is None
    assert builder.build() == (ProposedModel(location, ("new Location",)),)


def test_print_lint_results_marks_output_as_best_effort(
    capsys: pytest.CaptureFixture[str],
) -> None:
    location = Location.virtual(name="Place", tags=())
    proposal = ProposedModel(location, ("manifest line 1",))

    print_lint_results((ProposalLintResult(proposal, ("example issue",)),))

    output = capsys.readouterr().out
    assert "BEST_EFFORT VIRTUAL LINT" in output
    assert "example issue" in output
    assert "New virtual rows and changed scalar fields" in output


def test_lint_proposals_contains_lint_failures_in_result() -> None:
    location = Location.virtual(name="Place", tags=())
    proposal = ProposedModel(location, ("manifest line 1",))

    (result,) = lint_proposals((proposal,))

    assert result.proposal is proposal
    assert isinstance(result.messages, tuple)


def test_lint_proposals_clears_model_caches_once_per_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proposals = tuple(
        ProposedModel(Location.virtual(name=name, tags=()), (name,))
        for name in ("First", "Second")
    )
    clear_calls: list[None] = []
    monkeypatch.setattr(
        Location, "clear_lint_caches", classmethod(lambda cls: clear_calls.append(None))
    )
    monkeypatch.setattr(Location, "general_lint", lambda self, cfg: ())

    lint_proposals(proposals)

    assert len(clear_calls) == 2
