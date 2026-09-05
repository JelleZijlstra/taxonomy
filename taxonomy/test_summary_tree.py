import pytest

from taxonomy.summary_tree import (
    ObjectLabels,
    SummarySplit,
    SummaryTreeConfig,
    render_summary_tree,
)


def test_render_summary_tree_with_boolean_and_collection_labels() -> None:
    config = SummaryTreeConfig(
        singular="item",
        plural="items",
        variant_labels={
            "enabled": {True: "enabled", False: "disabled"},
            "colors": {"red": "red", "blue": "blue"},
        },
        tree=SummarySplit("enabled", children={True: SummarySplit("colors")}),
    )
    objects: list[ObjectLabels] = [
        {"enabled": True, "colors": frozenset({"red"})},
        {"enabled": True, "colors": frozenset({"blue"})},
        {"enabled": False, "colors": frozenset()},
    ]

    assert render_summary_tree(objects, config) == [
        "Of 3 items (100.0% of total):",
        "- 2 items (66.7% of total): enabled",
        "  - 1 item (33.3% of total): red",
        "  - 1 item (33.3% of total): blue",
        "- 1 item (33.3% of total): disabled",
    ]


@pytest.mark.parametrize(
    "objects", [[{"colors": frozenset()}], [{"colors": frozenset({"red", "blue"})}]]
)
def test_render_summary_tree_requires_variants_to_partition_parent(
    objects: list[ObjectLabels],
) -> None:
    config = SummaryTreeConfig(
        "item",
        "items",
        {"colors": {"red": "red", "blue": "blue"}},
        SummarySplit("colors"),
    )

    with pytest.raises(ValueError, match="expected exactly one"):
        render_summary_tree(objects, config)


def test_render_summary_tree_can_select_variants_for_a_split() -> None:
    config = SummaryTreeConfig(
        "item",
        "items",
        {"colors": {"red": "red", "blue": "blue", "green": "green"}},
        SummarySplit("colors", variants=("red", "green")),
    )

    assert render_summary_tree([{"colors": frozenset({"green"})}], config) == [
        "Of 1 item (100.0% of total):",
        "- 0 items (0.0% of total): red",
        "- 1 item (100.0% of total): green",
    ]
