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
from taxonomy.db.models.lint import (
    Lint,
    LintWrapper,
    create_related_object_issue,
    field_issue,
)
from taxonomy.db.models.lint_types import LintIssue
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
    assert "code=uncoded count=1" in output
    assert "New virtual rows and changed scalar fields" in output


def test_print_lint_results_aggregates_simulated_fixes_by_code(
    capsys: pytest.CaptureFixture[str],
) -> None:
    location = Location.virtual(name="Place", tags=())
    proposal = ProposedModel(location, ("manifest line 1",))
    simulated = LintIssue("normalized field", code="normalize")

    print_lint_results((ProposalLintResult(proposal, (), (simulated,)),))

    output = capsys.readouterr().out
    assert "VIRTUAL_AUTOFIX_SIMULATED total=1" in output
    assert "code=normalize count=1" in output
    assert "SIMULATED_FIXES Location" in output
    assert "normalized field" in output


def test_print_lint_results_separates_deferred_autofixes_from_issues(
    capsys: pytest.CaptureFixture[str],
) -> None:
    location = Location.virtual(name="Place", tags=())
    proposal = ProposedModel(location, ("manifest line 1",))
    deferred = field_issue("derived field", location, "name", "Derived place")

    print_lint_results((ProposalLintResult(proposal, (), (), (deferred,)),))

    output = capsys.readouterr().out
    assert "VIRTUAL_LINT_AUTOFIXABLE total=1 simulated=0 deferred=1" in output
    assert "VIRTUAL_AUTOFIX_DEFERRED total=1" in output
    assert "DEFERRED_FIXES Location" in output
    assert "derived field" in output
    assert "VIRTUAL_LINT_ISSUES" not in output
    assert "should not be duplicated as manifest edits" in output


def test_print_lint_results_can_output_only_unresolved_issues(
    capsys: pytest.CaptureFixture[str],
) -> None:
    location = Location.virtual(name="Place", tags=())
    proposal = ProposedModel(location, ("manifest line 1",))
    unresolved = LintIssue("requires a manifest edit", code="manual_fix")
    simulated = LintIssue("simulated normalization", code="normalize")
    deferred = LintIssue("deferred object creation", code="create_object")

    print_lint_results(
        (ProposalLintResult(proposal, (unresolved,), (simulated,), (deferred,)),),
        issues_only=True,
    )

    output = capsys.readouterr().out
    assert "VIRTUAL_LINT_ISSUES model=Location" in output
    assert "requires a manifest edit" in output
    assert "BEST_EFFORT" not in output
    assert "simulated normalization" not in output
    assert "deferred object creation" not in output
    assert "VIRTUAL_LINT_REMAINING_BY_CODE" not in output
    assert "Best-effort virtual lint:" not in output


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


def test_lint_proposals_applies_structured_fixes_to_fixed_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    location = Location.virtual(name="Old", latitude=None, tags=())
    proposal = ProposedModel(location, ("manifest line 1",))
    registry = Lint.for_model(Location)

    def check_location(item: Location, _cfg: object) -> list[LintIssue]:
        if item.name == "Old":
            return [field_issue("normalize name", item, "name", "New")]
        if item.latitude is None:
            return [field_issue("add latitude", item, "latitude", "12°N")]
        return []

    wrapper = LintWrapper(
        linter=check_location, disabled=False, label="test_fix", lint=registry
    )
    monkeypatch.setattr(Location, "general_lint", lambda self, cfg: wrapper(self, cfg))

    (result,) = lint_proposals((proposal,))

    assert result.messages == ()
    assert [issue.code for issue in result.simulated_fixes] == ["test_fix", "test_fix"]
    assert location.name == "New"
    assert location.latitude == "12°N"


def test_lint_proposals_flags_nonvirtual_structured_fix_as_deferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = Region.virtual(name="Region", kind=RegionKind.other, tags=())
    location = Location.virtual(name="Place", region=region, parent=None, tags=())
    proposal = ProposedModel(location, ("manifest line 1",))
    registry = Lint.for_model(Location)

    def check_location(item: Location, _cfg: object) -> list[LintIssue]:
        return [
            create_related_object_issue(
                "create derived parent Location",
                item,
                "parent",
                lambda: Location.virtual(
                    name="Derived parent", region=region, parent=None, tags=()
                ),
            )
        ]

    wrapper = LintWrapper(
        linter=check_location,
        disabled=False,
        label="create_parent_location",
        lint=registry,
    )
    monkeypatch.setattr(Location, "general_lint", lambda self, cfg: wrapper(self, cfg))

    (result,) = lint_proposals((proposal,))

    assert result.messages == ()
    assert result.simulated_fixes == ()
    assert [issue.code for issue in result.deferred_fixes] == ["create_parent_location"]
    assert location.parent is None
