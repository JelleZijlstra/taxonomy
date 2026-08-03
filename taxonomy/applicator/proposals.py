"""Build and lint in-memory model proposals for recommendation applicators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar

from clirm import substitute_virtual_models

from taxonomy import adt
from taxonomy.db.models.base import BaseModel, LintConfig

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class ProposedModel:
    model: BaseModel
    contexts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProposalLintResult:
    proposal: ProposedModel
    messages: tuple[str, ...]


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
        return proposal

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
        return self._by_origin_identity.get(id(model), model)

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
) -> tuple[ProposalLintResult, ...]:
    """Run ordinary taxonomy lint against virtual proposals on a best-effort basis."""
    results: list[ProposalLintResult] = []
    model_types = {type(proposal.model) for proposal in proposals}
    for model_type in model_types:
        model_type.clear_lint_caches()
    try:
        with (
            BaseModel.clirm.readonly(),
            substitute_virtual_models(proposal.model for proposal in proposals),
        ):
            for proposal in proposals:
                model = proposal.model
                try:
                    messages = tuple(model.general_lint(cfg))
                except Exception as exc:
                    messages = (f"error running virtual lint: {exc!r}",)
                if model.virtual_origin is not None:
                    # Database-wide duplicate queries necessarily find the proposal's
                    # own persisted origin. That is an artifact of virtual lint, not a
                    # proposed duplicate.
                    messages = tuple(
                        message
                        for message in messages
                        if not message.endswith(" [duplicate]")
                    )
                results.append(ProposalLintResult(proposal, messages))
    finally:
        for model_type in model_types:
            model_type.clear_lint_caches()
    return tuple(results)


def print_lint_results(results: tuple[ProposalLintResult, ...]) -> None:
    """Print advisory virtual-lint results without claiming complete validation."""
    with_issues = [result for result in results if result.messages]
    print("BEST_EFFORT VIRTUAL LINT")
    for result in with_issues:
        model = result.proposal.model
        origin = model.virtual_origin_id
        print(
            f"VIRTUAL_LINT_ISSUES model={type(model).__name__} "
            f"virtual_id={model.id!r} origin_id={origin!r} "
            f"contexts={result.proposal.contexts!r}"
        )
        for message in result.messages:
            print(f"- {message}")
    print(
        f"Best-effort virtual lint: {len(results)} object(s) checked, "
        f"{len(with_issues)} with issue(s). New virtual rows and changed scalar "
        "fields are not projected into database queries."
    )
