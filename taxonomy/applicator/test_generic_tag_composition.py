"""Regression coverage for tag edits composed with other recommendation families."""

from typing import Any

import pytest

from taxonomy.applicator import generic
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.models import Location, Name, TypeTag


@pytest.mark.parametrize("schema_version", [1, 2])
@pytest.mark.parametrize("partial", [False, True])
def test_tag_edits_preserve_other_family_tags(
    schema_version: int, *, partial: bool
) -> None:
    old = TypeTag.InterpretedTypeSpecimen("old interpretation")
    new = TypeTag.InterpretedTypeSpecimen("corrected interpretation")
    name = Name.virtual(corrected_original_name="Test name", type_tags=(old,))
    common = {
        "schema_version": schema_version,
        "confidence": "high",
        "reason": "Correct the specimen interpretation.",
        "evidence": [{"kind": "source", "text": "Reviewed original."}],
        "object": {"model": "Name", "id": 37737, "label": "Test name"},
    }
    if schema_version == 1:
        data = [
            {
                **common,
                "action": "remove_tag",
                "field": "type_tags",
                "tag": old.serialize(),
            },
            {
                **common,
                "action": "add_tag",
                "field": "type_tags",
                "tag": new.serialize(),
            },
        ]
    else:
        data = [
            {
                **common,
                "action": "update_object",
                "changes": [
                    {
                        "operation": "remove",
                        "field": "type_tags",
                        "value": old.serialize(),
                    },
                    {
                        "operation": "add",
                        "field": "type_tags",
                        "value": new.serialize(),
                    },
                ],
            }
        ]
    plan = generic.build_plan(
        [generic.parse_recommendation(row, i) for i, row in enumerate(data, 1)],
        model_registry={"Name": Name},
        get_object=lambda _model, _id: name,
    )
    extra: tuple[Any, ...]
    if partial:
        extra = (
            TypeTag.PartialTypeLocality(Location.virtual(name="First site")),
            TypeTag.PartialTypeLocality(Location.virtual(name="Second site")),
        )
    else:
        extra = (TypeTag.ImpreciseLocality(comment="Only the country is known"),)
    # The type-locality family runs after planning but before generic tag edits.
    builder = ProposalBuilder()
    proposed = builder.copy(name, context="earlier type-locality family")
    proposed.type_tags = (old, *extra)
    generic.add_virtual_models(plan, builder)
    assert set(proposed.type_tags) == {new, *extra}
    assert name.type_tags == (old,)

    name.type_tags = (old, *extra)
    generic.execute_plan(plan, apply=True)
    assert set(name.type_tags) == {new, *extra}
