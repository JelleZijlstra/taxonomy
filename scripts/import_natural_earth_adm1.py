"""Import Natural Earth admin-1 polygons into the configured GeoJSON repo."""

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

import unidecode

from taxonomy import coordinates
from taxonomy.config import get_options
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.region import Region

NATURAL_EARTH_URL = (
    "https://naturalearth.s3.amazonaws.com/10m_cultural/"
    "ne_10m_admin_1_states_provinces.zip"
)

SOURCE_NAME = "Natural Earth 10m admin-1 states/provinces"

DEFAULT_COUNTRIES = (
    "Austria",
    "Belgium",
    "Bolivia",
    "Brazil",
    "Comoros",
    "Germany",
    "South Africa",
    "São Tomé and Príncipe",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-geojson",
        type=Path,
        help="Converted Natural Earth admin-1 GeoJSON. If omitted, download and convert.",
    )
    parser.add_argument(
        "--country",
        action="append",
        dest="countries",
        help="Country to import. May be repeated. Defaults to the conservative set.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Generated GeoJSON root. Defaults to taxonomy.ini generated_geojson_path.",
    )
    parser.add_argument(
        "--replace", action="store_true", help="Overwrite existing output files."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write files. Without this, only report planned changes.",
    )
    args = parser.parse_args()

    countries = args.countries or list(DEFAULT_COUNTRIES)
    input_geojson = args.input_geojson or download_and_convert()
    features_by_country = load_features_by_country(input_geojson)
    output_root = args.output or get_options().generated_geojson_path
    if output_root == Path():
        raise RuntimeError(
            "Set generated_geojson_path in taxonomy.ini or pass --output"
        )
    for country_name in countries:
        import_country(
            country_name,
            features_by_country,
            output_root,
            write=args.write,
            replace=args.replace,
        )


def download_and_convert() -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix="natural-earth-adm1-"))
    zip_path = tmpdir / "natural_earth_adm1.zip"
    urlretrieve(NATURAL_EARTH_URL, zip_path)  # noqa: S310
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmpdir)
    shapefile_path = tmpdir / "ne_10m_admin_1_states_provinces.shp"
    if not shapefile_path.exists():
        raise RuntimeError(f"Natural Earth shapefile missing from {zip_path}")
    geojson_path = tmpdir / "admin1.json"
    ogr2ogr = shutil.which("ogr2ogr")
    if ogr2ogr is None:
        raise RuntimeError("ogr2ogr is required to convert the Natural Earth shapefile")
    subprocess.check_call(
        [ogr2ogr, "-f", "GeoJSON", str(geojson_path), str(shapefile_path)]
    )
    return geojson_path


def load_features_by_country(path: Path) -> dict[str, list[dict[str, Any]]]:
    with path.open() as f:
        data = json.load(f)
    features_by_country: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in data["features"]:
        country_name = normalize_name(feature["properties"]["admin"])
        features_by_country[country_name].append(feature)
    return features_by_country


def import_country(
    country_name: str,
    features_by_country: dict[str, list[dict[str, Any]]],
    output_root: Path,
    *,
    write: bool,
    replace: bool,
) -> None:
    country = Region.get(Region.name == country_name, Region.kind == RegionKind.country)
    country_key = normalize_name(country.name)
    features = features_by_country[country_key]
    matched = match_country_features(country, features)
    output_dir = output_root / "states" / get_path_name(country.name)
    print(f"{country.name}: {len(matched)} regions -> {output_dir}")
    for region, feature in matched.items():
        output_path = output_dir / f"{get_path_name(region.name)}.json"
        if output_path.exists() and not replace:
            raise RuntimeError(f"{output_path} already exists; pass --replace")
        print(
            f"  {region.name} <- {feature['properties']['name']}"
            f" ({feature['properties'].get('iso_3166_2') or 'no ISO code'})"
        )
        if write:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(make_feature_collection(feature), ensure_ascii=False) + "\n"
            )


def match_country_features(
    country: Region, features: list[dict[str, Any]]
) -> dict[Region, dict[str, Any]]:
    features_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in features:
        for name in get_feature_names(feature):
            features_by_name[normalize_name(name)].append(feature)

    matches: dict[Region, dict[str, Any]] = {}
    unmatched_regions = []
    used_feature_ids: set[int] = set()
    for region in sorted(country.children, key=lambda region: region.name):
        candidates = []
        for name in get_region_names(region):
            candidates.extend(features_by_name.get(normalize_name(name), []))
        unique_candidates = {id(feature): feature for feature in candidates}
        if len(unique_candidates) != 1:
            unmatched_regions.append(
                (
                    region.name,
                    sorted({feature["properties"]["name"] for feature in candidates}),
                )
            )
            continue
        feature = next(iter(unique_candidates.values()))
        if id(feature) in used_feature_ids:
            raise RuntimeError(f"{country.name}: feature matched twice: {feature}")
        used_feature_ids.add(id(feature))
        matches[region] = feature

    unused_features = [
        get_feature_label(feature)
        for feature in features
        if id(feature) not in used_feature_ids
    ]
    if unmatched_regions or unused_features:
        raise RuntimeError(
            f"{country.name}: cannot match cleanly; unmatched regions "
            f"{unmatched_regions}; unused features {sorted(unused_features)}"
        )
    return matches


def get_feature_label(feature: dict[str, Any]) -> str:
    props = feature["properties"]
    return props.get("name") or props.get("iso_3166_2") or props["adm1_code"]


def get_feature_names(feature: dict[str, Any]) -> set[str]:
    props = feature["properties"]
    names = {
        value
        for key in ("name", "name_en", "gn_name", "gns_name", "woe_name")
        if (value := props.get(key))
    }
    if name_alt := props.get("name_alt"):
        names.update(name_alt.split("|"))
    return names


def get_region_names(region: Region) -> set[str]:
    names = {region.name}
    names.add(re.sub(r" \([^)]+\)$", "", region.name))
    for suffix in (" Department", " Province", " Region"):
        if region.name.endswith(suffix):
            names.add(region.name.removesuffix(suffix))
    return names


def make_feature_collection(feature: dict[str, Any]) -> dict[str, Any]:
    props = feature["properties"]
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": feature["geometry"],
                "properties": {
                    "source": SOURCE_NAME,
                    "name": props.get("name"),
                    "name_en": props.get("name_en"),
                    "iso_3166_2": props.get("iso_3166_2"),
                    "natural_earth_adm1_code": props.get("adm1_code"),
                },
            }
        ],
    }


def normalize_name(name: str) -> str:
    name = unidecode.unidecode(name).lower()
    return re.sub(r"[^a-z0-9]+", " ", name).strip()


def get_path_name(name: str) -> str:
    return coordinates._transform_name(name)


if __name__ == "__main__":
    main()
