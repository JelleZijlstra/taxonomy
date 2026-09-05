"""Generic count trees over boolean and categorical object labels."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field

type LabelValue = bool | Collection[str]
type ObjectLabels = Mapping[str, LabelValue]
type Variant = bool | str


@dataclass(frozen=True, slots=True)
class SummarySplit:
    """Split objects by one field, optionally splitting selected variants further."""

    field: str
    children: Mapping[Variant, "SummarySplit"] = dataclass_field(default_factory=dict)
    variants: tuple[Variant, ...] | None = None


@dataclass(frozen=True, slots=True)
class SummaryTreeConfig:
    singular: str
    plural: str
    variant_labels: Mapping[str, Mapping[Variant, str]]
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
    if isinstance(actual, (bool, str)):
        raise TypeError(
            f"summary field {field_name!r} must be a collection of labels, "
            f"got {actual!r}"
        )
    if not all(isinstance(label, str) for label in actual):
        raise TypeError(
            f"summary field {field_name!r} contains a non-string label: {actual!r}"
        )
    return variant in actual


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
        try:
            labels_by_variant = config.variant_labels[split.field]
        except KeyError as exc:
            raise ValueError(
                f"missing display labels for summary field {split.field!r}"
            ) from exc
        variants = (
            tuple(labels_by_variant) if split.variants is None else split.variants
        )
        if not variants:
            raise ValueError(f"summary split {split.field!r} has no variants")
        if len(set(variants)) != len(variants):
            raise ValueError(f"summary split {split.field!r} repeats a variant")
        missing_labels = [
            variant for variant in variants if variant not in labels_by_variant
        ]
        if missing_labels:
            raise ValueError(
                f"summary split {split.field!r} has variants without display labels: "
                f"{missing_labels!r}"
            )
        unknown_children = set(split.children) - set(variants)
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
            if len(matching_variants) != 1:
                location = " > ".join(path) if path else "root"
                raise ValueError(
                    f"summary object {index} under {location!r} matches "
                    f"{len(matching_variants)} variants of {split.field!r}; "
                    "expected exactly one"
                )
            matches_by_variant[matching_variants[0]].append(labels)

        lines = []
        prefix = "  " * depth
        for variant in variants:
            matching_objects = matches_by_variant[variant]
            display_label = labels_by_variant[variant]
            lines.append(
                f"{prefix}- {count_text(len(matching_objects))}: {display_label}"
            )
            child = split.children.get(variant)
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
