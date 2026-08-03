import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from taxonomy.svg_map import (
    MapPoint,
    _make_projection,
    _polygon_path_data,
    write_svg_map,
)

SVG_NAMESPACE = {"svg": "http://www.w3.org/2000/svg"}


def _write_base_map(path: Path) -> None:
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-5, -5], [5, -5], [5, 5], [-5, 5], [-5, -5]]],
                },
            }
        ],
    }
    path.write_text(json.dumps(data))


def test_write_svg_map_renders_land_points_and_labels(tmp_path: Path) -> None:
    base_map = tmp_path / "country.json"
    _write_base_map(base_map)
    output = tmp_path / "map.svg"

    result = write_svg_map(
        [MapPoint(1, 2, "A & B"), MapPoint(1, 2, "Second record")],
        output,
        title="Test & map",
        base_map_paths=[base_map],
    )

    assert result == output
    root = ET.parse(output).getroot()
    title = root.find("svg:title", SVG_NAMESPACE)
    assert title is not None
    assert title.text == "Test & map"
    assert len(root.findall(".//svg:g[@class='land']/svg:path", SVG_NAMESPACE)) == 1
    point_groups = root.findall(".//svg:g[@class='point']", SVG_NAMESPACE)
    assert len(point_groups) == 1
    assert "A & B; Second record" in point_groups[0].attrib["aria-label"]


def test_projection_uses_short_interval_across_date_line() -> None:
    projection = _make_projection([MapPoint(0, 179, "west"), MapPoint(0, -179, "east")])

    west_x, _ = projection.project(0, 179)
    east_x, _ = projection.project(0, -179)

    assert abs(west_x - east_x) < 1000


def test_polygon_does_not_wrap_across_map_at_date_line() -> None:
    projection = _make_projection([MapPoint(0, 170, "west"), MapPoint(0, -170, "east")])
    polygon = [[[175, -5], [-175, -5], [-175, 5], [175, 5], [175, -5]]]

    path_data = _polygon_path_data(polygon, projection)
    x_coordinates = [
        float(match.group(1))
        for match in re.finditer(r"(-?\d+(?:\.\d+)?),-?\d+(?:\.\d+)?", path_data)
    ]

    assert max(x_coordinates) - min(x_coordinates) < 1000


def test_write_svg_map_rejects_empty_input(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot make a map without coordinates"):
        write_svg_map([], tmp_path / "map.svg", base_map_paths=[])
