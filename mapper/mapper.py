import argparse
from collections.abc import Callable
from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import TypedDict
from typing_extensions import NotRequired

Mapper = Callable[[float, float], tuple[float, float]]
DEFAULT_MARKER_SIZE = 4.3493844


def square_mapper(
    top: float, bottom: float, left: float, right: float, height: float, width: float
) -> Mapper:
    """Generate a mapper function for a square, Mercator-projection map."""
    # conversion factors from degrees to pixels
    latitude_conversion = height / (top - bottom)
    longitude_conversion = width / (right - left)

    def mapper(in_lat: float, in_long: float) -> tuple[float, float]:
        # first transform latitude so that 0 = 90 N, 180 = 90 S
        in_lat = -in_lat + 90
        # transform further so that 0 = top of map
        in_lat -= 90 - top
        # then transform further so that it is in agreement with height px == (top - bottom) degrees
        # 1 deg = 91.8 deg / 91.8 = 290 px / 91.8 = (290/91.8) px
        out_lat = in_lat * latitude_conversion

        # now for longitude, first transform so that left E = 0
        in_long -= left
        # and transform into pixels
        out_long = in_long * longitude_conversion

        return (out_lat, out_long)

    return mapper


@dataclass
class Map:
    name: str
    filename: str
    converter: Mapper


def _madagascar_converter(dlat: float, dlong: float) -> tuple[float, float]:
    out_lat = 35.38609001 * dlat - 388.3345367
    out_long = 34.79835974 * dlong - 1402.352063
    return out_lat, out_long


def _europe_converter(dlat: float, dlong: float) -> tuple[float, float]:
    # formula stolen from {{tl|Location map Europe}}
    out_lat = (
        55.11
        - 153.610
        * (
            math.cos(52 * math.pi / 180) * math.sin(dlat * math.pi / 180)
            - math.sin(52 * math.pi / 180)
            * math.cos(dlat * math.pi / 180)
            * math.cos((dlong - 10) * math.pi / 180)
        )
        * pow(
            (
                1
                + math.sin(dlat * math.pi / 180) * math.sin(52 * math.pi / 180)
                + math.cos(dlat * math.pi / 180)
                * math.cos(52 * math.pi / 180)
                * math.cos((dlong - 10) * math.pi / 180)
            )
            * 0.5,
            -0.5,
        )
    ) * 11.9834
    out_long = (
        131.579
        * (math.cos(dlat * math.pi / 180) * math.sin((dlong - 10) * math.pi / 180))
        * pow(
            (
                1
                + math.sin(dlat * math.pi / 180) * math.sin(52 * math.pi / 180)
                + math.cos(dlat * math.pi / 180)
                * math.cos(52 * math.pi / 180)
                * math.cos((dlong - 10) * math.pi / 180)
            )
            * 0.5,
            -0.5,
        )
        + 36.388
    ) * 14.0134 + 4
    return out_lat, out_long


def _south_america_converter(dlat: float, dlong: float) -> tuple[float, float]:
    # formula stolen from {{tl|Location map South America}}
    out_lat = (
        (100 + -51.2026)
        - 67.3182
        * (
            math.cos(-17.5 * 0.01745329252) * math.sin(dlat * 0.01745329252)
            - math.sin(-17.5 * 0.01745329252)
            * math.cos(dlat * 0.01745329252)
            * math.cos((-dlong - (-60.0)) * 0.01745329252)
        )
        * pow(
            (
                1
                + math.sin(dlat * 0.01745329252) * math.sin(-17.5 * 0.01745329252)
                + math.cos(dlat * 0.01745329252)
                * math.cos(-17.5 * 0.01745329252)
                * math.cos((-dlong - (-60.0)) * 0.01745329252)
            )
            * 0.5,
            -0.5,
        )
    ) * 17.32
    out_long = (
        99.3492
        * (
            math.cos(dlat * 0.01745329252)
            * math.sin((-dlong - (-60.0)) * 0.01745329252)
        )
        * pow(
            (
                1
                + math.sin(dlat * 0.01745329252) * math.sin(-17.5 * 0.01745329252)
                + math.cos(dlat * 0.01745329252)
                * math.cos(-17.5 * 0.01745329252)
                * math.cos((-dlong - (-60.0)) * 0.01745329252)
            )
            * 0.5,
            -0.5,
        )
        + 50.0
    ) * 11.81 + 4
    return out_lat, out_long


MAPS = [
    Map(
        "Asia",
        "Asia_location_map2.svg",
        square_mapper(78.3, -13.5, 18, 148.5, 204, 290),
    ),
    Map(
        "Indonesia",
        "Indonesia_location_map.svg",
        square_mapper(6.5, -11.5, 94.5, 141.5, 460, 1200),
    ),
    Map(
        "World",
        "World_location_map_(equirectangular_180).svg",
        square_mapper(90, -90, -180, 180, 1260, 2521),
    ),
    Map("Madagascar", "Mada_temp.svg", _madagascar_converter),
    Map("Europe", "Euro_temp2.svg", _europe_converter),
    Map("South America", "SAm_temp.svg", _south_america_converter),
]
NAME_TO_MAP = {map.name: map for map in MAPS}


def degrees_to_decimal(degrees: str) -> float:
    parts = degrees.split()
    degrees = parts[0]
    minutes = float(parts[1] if len(parts) > 1 else 0)
    seconds = float(parts[2] if len(parts) > 2 else 0)
    if len(parts) > 3:
        raise ValueError(f"unrecognized degrees string {degrees!r}")
    if minutes < 0:
        raise ValueError(f"minutes cannot be negative: {degrees!r}")
    if seconds < 0:
        raise ValueError(f"seconds cannot be negative: {degrees!r}")

    # use character check instead of checking whether deg > 0 to catch -0^15'2" or similar
    is_negative = degrees.startswith("-")
    out = float(degrees)
    if is_negative:
        out -= minutes / 60
        out -= seconds / 3600
    else:
        out += minutes / 60
        out += seconds / 3600

    return out


class Locality(TypedDict):
    name: str
    latitude: str
    longitude: str
    # taxonomy database, either an Article name or id
    raw_source: NotRequired[str | int]
    source: NotRequired[str]


class Group(TypedDict):
    group_name: str
    color: NotRequired[str]
    localities: list[Locality]


class MapData(TypedDict):
    outfile: str
    data: list[Group]
    map: str
    marker_size: NotRequired[float]


def hex_color_of_group(group: Group) -> str:
    if "color" not in group:
        return "#40a040"  # green
    match group["color"]:
        case "darkgreen":
            return "#40a040"
        case "red":
            return "#FF0000"
        case "blue":
            return "#0000FF"
        case "green":
            return "#00FF00"
        case _:
            return group["color"]


LOCALITY_TEMPLATE = """\t\t<path
\t\t\tstyle="fill:{color};fill-opacity:1;fill-rule:evenodd;stroke:none;display:inline;enable-background:new"
\t\t\td="m {shifted_longitude},{map_latitude} a {marker_size},{marker_size} 0 1 1 -{double_size}, 0 {marker_size},{marker_size} 0 1 1 {double_size},0 z">
\t\t\t<title>{name}</title>
\t\t\t<!-- Latitude: {dlat}; longitude: {dlong}{maybe_source}-->
\t\t</path>
"""


def decode_source(raw_source: str | int) -> str:
    from taxonomy.db.models import Article

    query = Article.select_valid()
    if isinstance(raw_source, int):
        query = query.filter(Article.id == raw_source)
    else:
        query = query.filter(Article.name == raw_source)
    art: Article = query.get()
    return art.cite()


def locality_to_svg(
    locality: Locality, color: str, map: Map, marker_size: float = DEFAULT_MARKER_SIZE
) -> str:
    dlat = degrees_to_decimal(locality["latitude"])
    dlong = degrees_to_decimal(locality["longitude"])
    map_latitude, map_longitude = map.converter(dlat, dlong)
    shifted_longitude = map_longitude + marker_size / 2
    double_size = 2 * marker_size
    if "source" in locality:
        maybe_source = "; source: " + locality["source"]
    else:
        if "raw_source" in locality:
            maybe_source = "; source: " + decode_source(locality["raw_source"])
        else:
            maybe_source = ""
    return LOCALITY_TEMPLATE.format(
        color=color,
        shifted_longitude=shifted_longitude,
        map_latitude=map_latitude,
        marker_size=marker_size,
        double_size=double_size,
        name=locality["name"],
        dlat=dlat,
        dlong=dlong,
        maybe_source=maybe_source,
    )


GROUP_TEMPLATE = """
\t<g
\t\tstyle="display:inline"
\t\tinkscape:groupmode="layer"
\t\tinkscape:label="{group_name}">
{localities}
\t</g>
"""


def group_to_svg(
    group: Group, map: Map, marker_size: float = DEFAULT_MARKER_SIZE
) -> str:
    color = hex_color_of_group(group)
    localities = [
        locality_to_svg(locality, color, map, marker_size)
        for locality in group["localities"]
    ]
    return GROUP_TEMPLATE.format(
        group_name=group["group_name"], localities="".join(localities)
    )


def map_to_svg(map_data: MapData, root_dir: Path) -> None:
    base_map = NAME_TO_MAP[map_data["map"]]
    base_svg = (root_dir / "base_maps" / base_map.filename).read_text()
    pieces = [base_svg[:-7]]
    marker_size = map_data.get("marker_size", DEFAULT_MARKER_SIZE)
    pieces += [group_to_svg(group, base_map, marker_size) for group in map_data["data"]]
    pieces.append("</svg>")
    out_path = Path(map_data["outfile"])
    out_path.write_text("".join(pieces))


def main() -> None:
    parser = argparse.ArgumentParser("Add locality markers to an SVG file")
    parser.add_argument("datafile", help="Path to a datafile")
    args = parser.parse_args()
    with Path(args.datafile).open(encoding="utf-8") as f:
        map_data = json.load(f)
    root_dir = Path(__file__).parent
    map_to_svg(map_data, root_dir)


if __name__ == "__main__":
    main()
