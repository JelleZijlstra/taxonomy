from io import StringIO
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

from taxonomy.db.models.region import Region, RegionTag


def test_display_type_localities_uses_concise_recursive_display() -> None:
    display = Mock()
    region = cast(Region, SimpleNamespace(display=display))
    output = StringIO()

    Region.display_type_localities(region, depth=4, file=output)

    display.assert_called_once_with(
        full=False, depth=4, file=output, children=True, locations=True
    )


def test_concise_region_display_recurses_through_subregions() -> None:
    child = Mock()
    region = cast(
        Region,
        SimpleNamespace(
            comment=None,
            is_empty=Mock(return_value=False),
            sorted_locations=Mock(return_value=[]),
            sorted_children=Mock(return_value=[child]),
        ),
    )
    output = StringIO()

    Region.display(region, full=False, children=True, locations=True, file=output)

    child.display.assert_called_once_with(
        full=False, depth=4, file=output, children=True, skip_empty=True, locations=True
    )


def test_display_type_localities_is_adt_callback() -> None:
    region = object.__new__(Region)

    callbacks = region.get_adt_callbacks()

    assert callbacks["display_type_localities"] == region.display_type_localities


def test_region_has_tag() -> None:
    region = cast(Region, SimpleNamespace(tags=(RegionTag.IncompletelyDivided,)))

    assert Region.has_tag(region, RegionTag.IncompletelyDivided)
