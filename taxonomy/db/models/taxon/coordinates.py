import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path

from taxonomy.coordinates import Point
from taxonomy.db import coordinate_lint, models
from taxonomy.svg_map import MapPoint, write_svg_map


def get_coordinates(taxon: models.Taxon) -> list[MapPoint]:
    """Return scalar type-locality and occurrence coordinates below a taxon."""
    points: set[MapPoint] = set()
    for name in taxon.all_names_lazy():
        points.update(_name_coordinates(name))
    for descendant in _iter_taxa(taxon):
        for record in descendant.occurrence_records:
            points.update(_occurrence_coordinates(record))
    return sorted(
        points,
        key=lambda point: (point.label.casefold(), point.latitude, point.longitude),
    )


def display_coordinates(taxon: models.Taxon) -> list[MapPoint]:
    points = get_coordinates(taxon)
    for point in points:
        print(f"{point.latitude:.6g}, {point.longitude:.6g}: {point.label}")
    print(f"{len(points)} coordinate record{'s' if len(points) != 1 else ''}")
    return points


def write_map(taxon: models.Taxon, output_path: Path) -> Path:
    return write_svg_map(
        get_coordinates(taxon), output_path, title=f"Coordinates for {taxon.valid_name}"
    )


def plot_coordinates(taxon: models.Taxon) -> Path | None:
    points = get_coordinates(taxon)
    if not points:
        print(f"No coordinates found for {taxon.valid_name}")
        return None
    with tempfile.NamedTemporaryFile(
        prefix="taxonomy-coordinate-map-", suffix=".svg", delete=False
    ) as temporary_file:
        path = Path(temporary_file.name)
    write_svg_map(points, path, title=f"Coordinates for {taxon.valid_name}")
    subprocess.check_call(["open", str(path)])
    return path


def _iter_taxa(taxon: models.Taxon) -> Iterable[models.Taxon]:
    yield taxon
    for child in taxon.get_children():
        yield from _iter_taxa(child)


def _name_coordinates(name: models.Name) -> Iterable[MapPoint]:
    location = name.type_locality
    if location is not None and not location.is_general():
        label = location.name
    else:
        label = f"Type locality of {_name_label(name)}"

    found_source_point = False
    for tag in name.get_tags(name.type_tags, models.name.TypeTag.Coordinates):
        point = coordinate_lint.make_point(tag.latitude, tag.longitude)
        if point is not None:
            found_source_point = True
            yield MapPoint(point.latitude, point.longitude, label)
    if found_source_point or location is None or location.is_general():
        return
    point = _location_point(location.latitude, location.longitude)
    if point is not None:
        yield MapPoint(point.latitude, point.longitude, label)


def _occurrence_coordinates(record: models.OccurrenceRecord) -> Iterable[MapPoint]:
    location = record.location
    if location is not None and not location.is_general():
        label = location.name
    else:
        label = record.locality_text

    found_source_point = False
    for tag in record.get_tags(
        record.tags, models.occurrence_record.OccurrenceRecordTag.Coordinates
    ):
        point = coordinate_lint.make_point(tag.latitude, tag.longitude)
        if point is not None:
            found_source_point = True
            yield MapPoint(point.latitude, point.longitude, label)
    if found_source_point or location is None or location.is_general():
        return
    point = _location_point(location.latitude, location.longitude)
    if point is not None:
        yield MapPoint(point.latitude, point.longitude, label)


def _location_point(latitude: str | None, longitude: str | None) -> Point | None:
    if latitude is None or longitude is None:
        return None
    return coordinate_lint.make_point(latitude, longitude)


def _name_label(name: models.Name) -> str:
    return name.corrected_original_name or name.original_name or name.root_name
