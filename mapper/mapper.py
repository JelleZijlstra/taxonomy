import argparse
import enum
import functools
import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

from typing_extensions import NotRequired

Mapper = Callable[[float, float], tuple[float, float]]
DEFAULT_MARKER_SIZE = 4.3493844


class ReportFormat(enum.Enum):
    markdown = 1
    wiki = 2


class Locality(TypedDict):
    name: str
    latitude: str
    longitude: str
    # taxonomy database, either an Article name or id
    raw_source: NotRequired[str | int]
    source: NotRequired[str]
    comment: NotRequired[str]


class Group(TypedDict):
    group_name: str
    color: NotRequired[str]
    style: NotRequired[str]
    localities: list[Locality]


class MapData(TypedDict):
    outfile: str
    data: list[Group]
    map: str
    marker_size: NotRequired[float]


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


def hex_color_of_group(group: Group) -> str:
    if "color" not in group:
        return "#40a040"  # green
    # https://commons.wikimedia.org/wiki/Commons:Creating_accessible_illustrations
    # except for "darkgreen" and "red", retained for compatibility
    match group["color"]:
        case "darkgreen":
            return "#40a040"
        case "red":
            return "#FF0000"
        case "skyblue":
            return "#56B4E9"
        case "blue":
            return "0072B2"
        case "green":
            return "#009E73"
        case "orange":
            return "#E69F00"
        case "yellow":
            return "F0E442"
        case "vermilion":
            return "D55E00"
        case "purple":
            return "CC79A7"
        case _:
            return group["color"]


CIRCLE_TEMPLATE = """\t\t<circle
\t\t\tstyle="fill:{color};fill-opacity:1;fill-rule:evenodd;stroke:none;display:inline;enable-background:new"
\t\t\tcy="{map_latitude}" cx="{map_longitude}" r="{marker_size}">
\t\t\t<title>{name}</title>
\t\t\t<!-- Latitude: {dlat}; longitude: {dlong}{maybe_source}-->
\t\t</circle>
"""
OPEN_CIRCLE_TEMPLATE = """\t\t<circle
\t\t\tstyle="stroke:{color};stroke-width:{stroke_width};fill-opacity:0;display:inline;enable-background:new"
\t\t\tcy="{map_latitude}" cx="{map_longitude}" r="{marker_size}">
\t\t\t<title>{name}</title>
\t\t\t<!-- Latitude: {dlat}; longitude: {dlong}{maybe_source}-->
\t\t</circle>
"""
CROSS_TEMPLATE = """\t\t<path
\t\t\tstyle="fill:{color};fill-opacity:1;fill-rule:evenodd;stroke:none;display:inline;enable-background:new"
\t\t\td="M {map_longitude},{map_latitude} m {cross_size},0 h {cross_size} v -{cross_size} h {cross_size} v -{cross_size} h -{cross_size} v -{cross_size} h -{cross_size} v {cross_size} h -{cross_size} v {cross_size} h {cross_size} v {cross_size} z">
\t\t\t<title>{name}</title>
\t\t\t<!-- Latitude: {dlat}; longitude: {dlong}{maybe_source}-->
\t\t</path>
"""

STYLE_TO_TEMPLATE = {
    "circle": CIRCLE_TEMPLATE,
    "open": OPEN_CIRCLE_TEMPLATE,
    "cross": CROSS_TEMPLATE,
}


@functools.cache
def decode_source(raw_source: str | int, *, cite_style: str = "paper") -> str:
    from taxonomy.db.models import Article

    query = Article.select_valid()
    if isinstance(raw_source, int):
        query = query.filter(Article.id == raw_source)  # type: ignore
    else:
        query = query.filter(Article.name == raw_source)
    art: Article = query.get()
    return art.cite(cite_style).removeprefix("*")


def source_from_locality(locality: Locality, cite_style: str = "paper") -> str | None:
    if "source" in locality:
        return locality["source"]
    if "raw_source" in locality:
        return decode_source(locality["raw_source"], cite_style=cite_style)
    return None


def locality_to_svg(
    locality: Locality,
    color: str,
    style: str,
    map: Map,
    marker_size: float = DEFAULT_MARKER_SIZE,
) -> str:
    dlat = degrees_to_decimal(locality["latitude"])
    dlong = degrees_to_decimal(locality["longitude"])
    map_latitude, map_longitude = map.converter(dlat, dlong)
    double_size = 2 * marker_size
    source_str = source_from_locality(locality)
    cross_size = double_size / 3
    if source_str is not None:
        maybe_source = f"; source: {source_str}"
    else:
        maybe_source = ""
    return STYLE_TO_TEMPLATE[style].format(
        color=color,
        map_longitude=map_longitude,
        map_latitude=map_latitude,
        marker_size=marker_size,
        double_size=double_size,
        cross_size=cross_size,
        stroke_width=marker_size / 2,
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
    style = group.get("style", "circle")
    localities = [
        locality_to_svg(locality, color, style, map, marker_size)
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


LOCALITY_REPORT_TEMPLATE = {
    ReportFormat.markdown: "* **{name}** ({latitude}, {longitude}){maybe_comment}{maybe_source}",
    ReportFormat.wiki: "* '''{name}''' ({latitude}, {longitude}){maybe_comment}{maybe_source}",
}
FORMAT_TO_STYLE = {ReportFormat.markdown: "paper", ReportFormat.wiki: "commons"}


def print_report(map_data: MapData, format: ReportFormat, only_unsourced: bool) -> None:
    for group in map_data["data"]:
        localities = group["localities"]
        if only_unsourced:
            localities = [
                locality
                for locality in localities
                if "source" not in locality and "raw_source" not in locality
            ]
        if not localities:
            continue
        print()
        name = group["group_name"]
        match format:
            case ReportFormat.markdown:
                print(f"## {name}")
            case ReportFormat.wiki:
                print(f"=== {name} ===")
        print()
        for locality in localities:
            template = LOCALITY_REPORT_TEMPLATE[format]
            if "comment" in locality:
                maybe_comment = f". Comment: {locality['comment']}"
            else:
                maybe_comment = ""
            source_str = source_from_locality(
                locality, cite_style=FORMAT_TO_STYLE[format]
            )
            if source_str is None:
                maybe_source = ""
            else:
                maybe_source = f". Source: {source_str}"
            line = template.format(
                name=locality["name"],
                latitude=locality["latitude"],
                longitude=locality["longitude"],
                maybe_comment=maybe_comment,
                maybe_source=maybe_source,
            )
            print(line)


def main() -> None:
    parser = argparse.ArgumentParser("Add locality markers to an SVG file")
    parser.add_argument("datafile", help="Path to a datafile")
    parser.add_argument(
        "--wiki-report",
        action="store_true",
        default=False,
        help="Output a wikitext source report",
    )
    parser.add_argument(
        "--markdown-report",
        action="store_true",
        default=False,
        help="Output a markdown source report",
    )
    parser.add_argument(
        "--only-unsourced",
        action="store_true",
        default=False,
        help="Only report localities without sources",
    )
    args = parser.parse_args()
    with Path(args.datafile).open(encoding="utf-8") as f:
        map_data = json.load(f)
    if args.wiki_report:
        print_report(map_data, ReportFormat.wiki, args.only_unsourced)
    elif args.markdown_report:
        print_report(map_data, ReportFormat.markdown, args.only_unsourced)
    else:
        root_dir = Path(__file__).parent
        map_to_svg(map_data, root_dir)


if __name__ == "__main__":
    main()
