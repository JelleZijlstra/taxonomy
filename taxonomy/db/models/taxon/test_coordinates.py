from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from taxonomy.db.models.name import Name, TypeTag
from taxonomy.db.models.occurrence_record import OccurrenceRecord, OccurrenceRecordTag
from taxonomy.db.models.taxon import Taxon
from taxonomy.svg_map import MapPoint

from . import coordinates


def _location(
    name: str, latitude: str | None, longitude: str | None, *, general: bool = False
) -> Any:
    return SimpleNamespace(
        name=name, latitude=latitude, longitude=longitude, is_general=lambda: general
    )


def _get_tags(values: object, tag_type: type[object]) -> Iterator[object]:
    return (
        value
        for value in cast(tuple[object, ...], values)
        if isinstance(value, tag_type)
    )


def _name(label: str, location: object | None, *tags: TypeTag) -> Name:
    return cast(
        Name,
        SimpleNamespace(
            corrected_original_name=label,
            original_name=None,
            root_name=label,
            type_locality=location,
            type_tags=tags,
            get_tags=_get_tags,
        ),
    )


def _record(
    label: str, location: object | None, *tags: OccurrenceRecordTag
) -> OccurrenceRecord:
    return cast(
        OccurrenceRecord,
        SimpleNamespace(
            locality_text=label, location=location, tags=tags, get_tags=_get_tags
        ),
    )


def test_get_coordinates_combines_names_occurrences_and_descendants() -> None:
    specific = _location("Walnut Creek", "37°54'N", "122°18'W")
    general = _location("California", "37°N", "120°W", general=True)
    names = [
        _name("Mus example", specific),
        _name("Mus precise", specific, TypeTag.Coordinates("38°N", "123°W")),
        _name("Mus broad", general, TypeTag.Coordinates("39°N", "121°W")),
    ]
    child = SimpleNamespace(
        occurrence_records=[
            _record(
                "Source station",
                general,
                OccurrenceRecordTag.Coordinates("10°S", "20°E"),
            ),
            _record("Mapped station", specific),
            _record("Unusable region", general),
        ],
        get_children=lambda: (),
    )
    taxon = cast(
        Taxon,
        SimpleNamespace(
            all_names_lazy=lambda: iter(names),
            occurrence_records=(),
            get_children=lambda: (child,),
        ),
    )

    assert coordinates.get_coordinates(taxon) == [
        MapPoint(-10, 20, "Source station"),
        MapPoint(39, -121, "Type locality of Mus broad"),
        MapPoint(37.9, -122.3, "Walnut Creek"),
        MapPoint(38, -123, "Walnut Creek"),
    ]


def test_get_coordinates_deduplicates_equal_records() -> None:
    location = _location("Same place", "1°N", "2°E")
    taxon = cast(
        Taxon,
        SimpleNamespace(
            all_names_lazy=lambda: iter(
                [_name("First", location), _name("Second", location)]
            ),
            occurrence_records=[_record("Same source", location)],
            get_children=lambda: (),
        ),
    )

    assert coordinates.get_coordinates(taxon) == [MapPoint(1, 2, "Same place")]


def test_write_map_delegates_to_svg_renderer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    point = MapPoint(1, 2, "Place")
    taxon = cast(
        Taxon,
        SimpleNamespace(
            valid_name="Mus example",
            all_names_lazy=lambda: (),
            get_children=lambda: (),
            occurrence_records=(),
        ),
    )
    monkeypatch.setattr(coordinates, "get_coordinates", Mock(return_value=[point]))
    write_svg_map = Mock(return_value=tmp_path / "map.svg")
    monkeypatch.setattr(coordinates, "write_svg_map", write_svg_map)

    assert coordinates.write_map(taxon, tmp_path / "map.svg") == tmp_path / "map.svg"
    write_svg_map.assert_called_once_with(
        [point], tmp_path / "map.svg", title="Coordinates for Mus example"
    )


def test_taxon_callbacks_expose_coordinate_commands() -> None:
    taxon = object.__new__(Taxon)

    callbacks = taxon.get_adt_callbacks()

    assert callbacks["display_coordinates"] == taxon.display_coordinates
    assert callbacks["plot_coordinates"] == taxon.plot_coordinates
