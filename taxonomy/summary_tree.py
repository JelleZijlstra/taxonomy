"""Generic count trees over boolean and categorical object labels."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field

type LabelValue = bool | str
type ObjectLabels = Mapping[str, LabelValue]
type Variant = bool | str


@dataclass(frozen=True, slots=True)
class SummarySplit:
    """Split objects by one field, optionally splitting selected variants further."""

    field: str
    children: Mapping[Variant | None, "SummarySplit"] = dataclass_field(
        default_factory=dict
    )
    variants: tuple[Variant, ...] | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SummaryTreeConfig:
    singular: str
    plural: str
    variant_labels: Mapping[str, Mapping[Variant, str]] = dataclass_field(
        default_factory=dict
    )
    tree: SummarySplit


def _matches(labels: ObjectLabels, field_name: str, variant: Variant) -> bool:
    try:
        actual = labels[field_name]
    except KeyError as exc:
        raise ValueError(f"missing summary field {field_name!r}") from exc
    if isinstance(variant, bool):
        if not isinstance(actual, bool):
            raise TypeError(
                f"summary field {field_name!r} must be boolean, got {actual!r}"
            )
        return actual is variant
    if not isinstance(actual, str):
        raise TypeError(f"summary field {field_name!r} must be a label, got {actual!r}")
    return actual == variant


def render_summary_tree(
    objects: Sequence[ObjectLabels], config: SummaryTreeConfig
) -> list[str]:
    """Render a strict partition tree with percentages of the complete input."""
    total = len(objects)

    def count_text(count: int) -> str:
        noun = config.singular if count == 1 else config.plural
        percentage = count / total * 100 if total else 0
        return f"{count:,} {noun} ({percentage:.1f}% of total)"

    def render_split(
        parent_objects: Sequence[ObjectLabels],
        split: SummarySplit,
        *,
        depth: int,
        path: tuple[str, ...],
    ) -> list[str]:
        labels_by_variant = config.variant_labels.get(split.field, {})
        variants = (
            tuple(labels_by_variant) if split.variants is None else split.variants
        )
        if len(set(variants)) != len(variants):
            raise ValueError(f"summary split {split.field!r} repeats a variant")
        unknown_children = set(split.children) - set(variants) - {None}
        if unknown_children:
            raise ValueError(
                f"summary split {split.field!r} has children for variants not in the "
                f"split: {unknown_children!r}"
            )

        matches_by_variant: dict[Variant, list[ObjectLabels]] = {
            variant: [] for variant in variants
        }
        for index, labels in enumerate(parent_objects):
            matching_variants = [
                variant
                for variant in variants
                if _matches(labels, split.field, variant)
            ]
            if len(matching_variants) == 1:
                matching_variant = matching_variants[0]
            elif (None in split.children or not split.children) and len(
                matching_variants
            ) == 0:
                matching_variant = labels[split.field]
            else:
                location = " > ".join(path) if path else "root"
                raise ValueError(
                    f"summary object {index} under {location!r} matches "
                    f"{len(matching_variants)} variants of {split.field!r}; "
                    "expected exactly one"
                )
            matches_by_variant.setdefault(matching_variant, []).append(labels)

        lines = []
        prefix = "  " * depth
        for variant, matching_objects in matches_by_variant.items():
            display_label = labels_by_variant.get(variant, str(variant))
            lines.append(
                f"{prefix}- {count_text(len(matching_objects))}: {display_label}"
            )
            child = split.children.get(variant)
            if child is None:
                child = split.children.get(None)
            if child is not None:
                lines.extend(
                    render_split(
                        matching_objects,
                        child,
                        depth=depth + 1,
                        path=(*path, display_label),
                    )
                )
        return lines

    return [
        f"Of {count_text(total)}:",
        *render_split(objects, config.tree, depth=0, path=()),
    ]
