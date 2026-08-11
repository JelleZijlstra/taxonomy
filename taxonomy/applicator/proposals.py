"""Build and lint in-memory model proposals for recommendation applicators."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, TypeVar, cast

from clirm import substitute_virtual_models

from taxonomy import adt
from taxonomy.db.models.base import BaseModel, LintConfig
from taxonomy.db.models.lint import count_lint_codes
from taxonomy.db.models.lint_types import LintIssue, LintResult

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class ProposedModel:
    model: BaseModel
    contexts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProposalLintResult:
    proposal: ProposedModel
    messages: tuple[LintResult, ...]
    simulated_fixes: tuple[LintIssue, ...] = ()
    deferred_fixes: tuple[LintIssue, ...] = ()

    @property
    def autofixable_issues(self) -> tuple[LintIssue, ...]:
        return (*self.simulated_fixes, *self.deferred_fixes)


class ProposalBuilder:
    """Collect one final virtual model for each affected persisted object."""

    def __init__(self) -> None:
        self._by_origin_identity: dict[int, BaseModel] = {}
        self._proposals: list[BaseModel] = []
        self._contexts: dict[int, list[str]] = {}

    def _add_context(self, model: BaseModel, context: str) -> None:
        contexts = self._contexts.setdefault(id(model), [])
        if context not in contexts:
            contexts.append(context)

    def copy(self, origin: ModelT, *, context: str) -> ModelT:
        """Get the shared virtual copy for a persisted object."""
        identity = id(origin)
        try:
            proposal = self._by_origin_identity[identity]
        except KeyError:
            proposal = origin.virtual_copy()
            self._by_origin_identity[identity] = proposal
            self._proposals.append(proposal)
        self._add_context(proposal, context)
        return cast(ModelT, proposal)

    def create(
        self,
        model_cls: type[ModelT],
        *,
        context: str,
        proposal_source: BaseModel | None = None,
        **values: Any,
    ) -> ModelT:
        """Add a newly proposed model that has no persisted origin."""
        proposal = model_cls.virtual(**values)
        if proposal_source is not None:
            self._by_origin_identity[id(proposal_source)] = proposal
        self._proposals.append(proposal)
        self._add_context(proposal, context)
        return proposal

    def replacement(self, model: ModelT) -> ModelT:
        """Return an existing proposal for a model, or the persisted model itself."""
        return cast(ModelT, self._by_origin_identity.get(id(model), model))

    def _remap_value(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            return self._by_origin_identity.get(id(value), value)
        if isinstance(value, adt.ADT):
            changes = {
                attribute: self._remap_value(getattr(value, attribute))
                for attribute in value._attributes
            }
            if any(
                changes[attribute] is not getattr(value, attribute)
                for attribute in value._attributes
            ):
                return value.replace(**changes)
            return value
        if isinstance(value, tuple):
            remapped = tuple(self._remap_value(item) for item in value)
            return (
                remapped
                if any(a is not b for a, b in zip(remapped, value, strict=True))
                else value
            )
        if isinstance(value, list):
            remapped_list = [self._remap_value(item) for item in value]
            return (
                remapped_list
                if any(a is not b for a, b in zip(remapped_list, value, strict=True))
                else value
            )
        if isinstance(value, dict):
            remapped_dict = {
                key: self._remap_value(item) for key, item in value.items()
            }
            return (
                remapped_dict
                if any(remapped_dict[key] is not item for key, item in value.items())
                else value
            )
        return value

    def _remap_references(self) -> None:
        # Proposal families are built in application order. A referenced model may be
        # copied only by a later family, so update foreign keys after all proposals
        # have been collected. This includes model references nested in ADT tags.
        for proposal in self._proposals:
            for name in proposal.clirm_fields:
                if name in proposal.missing_virtual_fields:
                    continue
                value = getattr(proposal, name)
                replacement = self._remap_value(value)
                if replacement is not value:
                    setattr(proposal, name, replacement)

    def build(self) -> tuple[ProposedModel, ...]:
        self._remap_references()
        return tuple(
            ProposedModel(model, tuple(self._contexts[id(model)]))
            for model in self._proposals
        )


def lint_proposals(
    proposals: tuple[ProposedModel, ...],
    *,
    cfg: LintConfig = LintConfig(autofix=False, interactive=False),
    max_fix_rounds: int = 10,
) -> tuple[ProposalLintResult, ...]:
    """Run lint and safely simulate structured fixes against virtual proposals."""
    if max_fix_rounds < 1:
        raise ValueError("max_fix_rounds must be positive")
    base_cfg = replace(
        cfg,
        autofix=False,
        structured_autofix=False,
        fix_callback=None,
        interactive=False,
    )
    model_types = {type(proposal.model) for proposal in proposals}
    simulated_by_model: dict[int, list[LintIssue]] = {
        id(proposal.model): [] for proposal in proposals
    }
    latest_messages: dict[int, tuple[LintIssue, ...]] = {}
    convergence_error: LintIssue | None = None
    for model_type in model_types:
        model_type.clear_lint_caches()
    try:
        with (
            BaseModel.clirm.readonly(),
            substitute_virtual_models(proposal.model for proposal in proposals),
        ):
            for _round_index in range(max_fix_rounds):
                round_fix_count = 0
                for proposal in proposals:
                    model = proposal.model
                    simulated_this_pass: list[LintIssue] = []
                    run_cfg = replace(
                        base_cfg,
                        structured_autofix=True,
                        fix_callback=simulated_this_pass.append,
                    )
                    try:
                        raw_messages = tuple(model.general_lint(run_cfg))
                    except Exception as exc:
                        raw_messages = (
                            LintIssue(
                                f"error running virtual lint: {exc!r}",
                                code="virtual_lint_error",
                            ),
                        )
                    messages = tuple(
                        (
                            message
                            if isinstance(message, LintIssue)
                            else LintIssue(str(message))
                        )
                        for message in raw_messages
                    )
                    if model.virtual_origin is not None:
                        # Database-wide duplicate queries necessarily find the
                        # proposal's own persisted origin. That is an artifact of
                        # virtual lint, not a proposed duplicate.
                        messages = tuple(
                            message
                            for message in messages
                            if not str(message).endswith(" [duplicate]")
                        )
                    latest_messages[id(model)] = messages
                    simulated_by_model[id(model)].extend(simulated_this_pass)
                    round_fix_count += len(simulated_this_pass)
                if round_fix_count == 0:
                    break
                for model_type in model_types:
                    model_type.clear_lint_caches()
            else:
                convergence_error = LintIssue(
                    f"structured virtual autofix did not converge after "
                    f"{max_fix_rounds} rounds",
                    code="virtual_autofix_nonconvergent",
                )
    finally:
        for model_type in model_types:
            model_type.clear_lint_caches()
    results = []
    for index, proposal in enumerate(proposals):
        messages = latest_messages.get(id(proposal.model), ())
        if convergence_error is not None and index == 0:
            messages = (*messages, convergence_error)
        deferred_fixes = tuple(
            message for message in messages if message.fix is not None
        )
        messages = tuple(message for message in messages if message.fix is None)
        results.append(
            ProposalLintResult(
                proposal,
                messages,
                tuple(simulated_by_model[id(proposal.model)]),
                deferred_fixes,
            )
        )
    return tuple(results)


def print_lint_results(
    results: tuple[ProposalLintResult, ...], *, issues_only: bool = False
) -> None:
    """Print advisory virtual-lint results without claiming complete validation."""
    with_issues = [result for result in results if result.messages]
    if issues_only:
        _print_lint_issues(with_issues)
        return
    simulated = [issue for result in results for issue in result.simulated_fixes]
    deferred = [issue for result in results for issue in result.deferred_fixes]
    autofixable = [issue for result in results for issue in result.autofixable_issues]
    print("BEST_EFFORT VIRTUAL LINT")
    if autofixable:
        counts = count_lint_codes(autofixable)
        print(
            f"VIRTUAL_LINT_AUTOFIXABLE total={len(autofixable)} "
            f"simulated={len(simulated)} deferred={len(deferred)}"
        )
        for code, count in sorted(counts.items()):
            print(f"- code={code} count={count}")
    if simulated:
        counts = count_lint_codes(simulated)
        print(f"VIRTUAL_AUTOFIX_SIMULATED total={len(simulated)}")
        for code, count in sorted(counts.items()):
            print(f"- code={code} count={count}")
        for result in results:
            if not result.simulated_fixes:
                continue
            model = result.proposal.model
            origin = model.virtual_origin_id
            identity = (
                f"origin={origin}"
                if origin is not None
                else f"new_virtual_id={model.id!r}"
            )
            print(
                f"SIMULATED_FIXES {type(model).__name__} {identity}; "
                f"contexts={result.proposal.contexts!r}"
            )
            for issue in result.simulated_fixes:
                print(f"- {issue}")
    if deferred:
        print(f"VIRTUAL_AUTOFIX_DEFERRED total={len(deferred)}")
        for result in results:
            if not result.deferred_fixes:
                continue
            model = result.proposal.model
            origin = model.virtual_origin_id
            identity = (
                f"origin={origin}"
                if origin is not None
                else f"new_virtual_id={model.id!r}"
            )
            print(
                f"DEFERRED_FIXES {type(model).__name__} {identity}; "
                f"contexts={result.proposal.contexts!r}"
            )
            for issue in result.deferred_fixes:
                print(f"- {issue}")
    _print_lint_issues(with_issues)
    remaining = count_lint_codes(
        message for result in with_issues for message in result.messages
    )
    if remaining:
        print("VIRTUAL_LINT_REMAINING_BY_CODE")
        for code, count in sorted(remaining.items()):
            print(f"- code={code} count={count}")
    print(
        f"Best-effort virtual lint: {len(results)} object(s) checked, "
        f"{len(with_issues)} with non-autofixable issue(s), "
        f"{len(autofixable)} autofixable finding(s). Autofixable findings are "
        "reported separately and should not be duplicated as manifest edits. "
        "New virtual rows and changed scalar fields are not projected into "
        "database queries."
    )


def _print_lint_issues(results: list[ProposalLintResult]) -> None:
    for result in results:
        model = result.proposal.model
        origin = model.virtual_origin_id
        print(
            f"VIRTUAL_LINT_ISSUES model={type(model).__name__} "
            f"virtual_id={model.id!r} origin_id={origin!r} "
            f"contexts={result.proposal.contexts!r}"
        )
        for message in result.messages:
            print(f"- {message}")
