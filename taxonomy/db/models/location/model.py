from __future__ import annotations

import enum
import re
import sqlite3
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import IO, TYPE_CHECKING, Any, ClassVar, NotRequired, Self

from clirm import Field

from taxonomy import adt, events, getinput
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import Managed, Markdown
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig, TextField
from taxonomy.db.models.period import Period, period_sort_key
from taxonomy.db.models.region import Region
from taxonomy.db.models.stratigraphic_unit import StratigraphicUnit

if TYPE_CHECKING:
    from taxonomy.db.models.name import Name

    _CoordinatesFromNameTarget = Name
else:
    # Replaced with Name after all models have loaded; see models.__init__. This
    # breaks the Location <-> Name import cycle while retaining the normal
    # model-valued ADT behavior at runtime.
    _CoordinatesFromNameTarget = Managed


class LocationStatus(enum.IntEnum):
    valid = 0
    deleted = 1
    alias = 2


def _format_coordinate_evidence(
    latitude: str | None, longitude: str | None, *, reference_extent: Any | None = None
) -> str:
    if latitude is None or longitude is None:
        return f"{latitude or '(missing)'}, {longitude or '(missing)'} (incomplete)"
    parsed = coordinate_lint.standardize_coordinate_pair(latitude, longitude)
    if parsed is None:
        return f"{latitude}, {longitude} (invalid)"
    standardized_latitude, standardized_longitude, extent = parsed
    output = f"{standardized_latitude}, {standardized_longitude}"
    if (standardized_latitude, standardized_longitude) != (latitude, longitude):
        output += f" (raw: {latitude!r}, {longitude!r})"
    if reference_extent is not None:
        distance = coordinate_lint.extent_distance_km(reference_extent, extent)
        output += f"; {distance:.1f} km from Location coordinates"
    return output


@dataclass(slots=True)
class _CoordinateChoice:
    latitude: str
    longitude: str
    extent: coordinate_lint.CoordinateExtent
    sources: list[str]
    provenance_tags: list[adt.ADT] = dataclass_field(default_factory=list)

    def describe(self) -> str:
        return f"{self.latitude}, {self.longitude} — {'; '.join(self.sources)}"


def _is_empty_text(value: str | None) -> bool:
    return value is None or not value.strip() or value == "None"


def _merge_text_field(source: Location, target: Location, field: str) -> None:
    source_text = getattr(source, field)
    if _is_empty_text(source_text):
        return
    target_text = getattr(target, field)
    if _is_empty_text(target_text):
        print(f"setting {field} from {source}")
        setattr(target, field, source_text)
        return
    if source_text == target_text:
        return
    merged_section = f"Merged from {source.name} (L#{source.id}):\n{source_text}"
    if merged_section in target_text:
        return
    print(f"appending {field} from {source}")
    setattr(target, field, f"{target_text.rstrip()}\n\n{merged_section}")


def _merge_coordinates(source: Location, target: Location) -> bool:
    source_pair = (source.latitude, source.longitude)
    target_pair = (target.latitude, target.longitude)
    if source_pair in ((None, None), target_pair):
        return source_pair == target_pair and source_pair != (None, None)
    if target_pair == (None, None):
        print(f"setting coordinates from {source}: {source_pair}")
        target.latitude, target.longitude = source_pair
        return True

    compatible = all(
        source_value is None or target_value is None or source_value == target_value
        for source_value, target_value in zip(source_pair, target_pair, strict=True)
    )
    if compatible:
        merged_pair = tuple(
            target_value if target_value is not None else source_value
            for source_value, target_value in zip(source_pair, target_pair, strict=True)
        )
        if merged_pair != target_pair:
            print(f"completing coordinates from {source}: {merged_pair}")
            target.latitude, target.longitude = merged_pair
        return True
    print(
        f"warning: keeping coordinates on {target}: {target_pair}; "
        f"source {source} has {source_pair}"
    )
    return False


def _merge_location_data(source: Location, target: Location) -> None:
    if source.region != target.region:
        print(
            f"warning: keeping region on {target}: {target.region}; "
            f"source {source} has {source.region}"
        )
    for field in (
        "min_period",
        "max_period",
        "stratigraphic_unit",
        "min_age",
        "max_age",
        "source",
    ):
        source_value = getattr(source, field)
        if source_value is None:
            continue
        target_value = getattr(target, field)
        if target_value is None:
            print(f"setting {field} from {source}: {source_value}")
            setattr(target, field, source_value)
        elif source_value != target_value:
            print(
                f"warning: keeping {field} on {target}: {target_value}; "
                f"source {source} has {source_value}"
            )

    coordinates_merged = _merge_coordinates(source, target)
    for field in ("comment", "location_detail", "age_detail"):
        _merge_text_field(source, target, field)

    target_tags = tuple(target.tags or ())
    merged_tags = (
        *target_tags,
        *(
            tag
            for tag in source.tags or ()
            if tag not in target_tags
            and (coordinates_merged or not is_coordinate_provenance_tag(tag))
        ),
    )
    if merged_tags != target_tags:
        print(f"adding {len(merged_tags) - len(target_tags)} tag(s) from {source}")
        target.tags = merged_tags  # type: ignore[assignment]


class Location(BaseModel):
    creation_event = events.Event["Location"]()
    save_event = events.Event["Location"]()
    label_field = "name"
    grouping_field = "min_period"
    call_sign = "L"
    clirm_table_name = "location"

    name = Field[str]()
    min_period = Field[Period | None]("min_period_id", related_name="locations_min")
    max_period = Field[Period | None]("max_period_id", related_name="locations_max")
    min_age = Field[int | None]()
    max_age = Field[int | None]()
    stratigraphic_unit = Field[StratigraphicUnit | None](
        "stratigraphic_unit_id", related_name="locations"
    )
    region = Field[Region]("region_id", related_name="locations")
    comment = Field[str | None]()
    latitude = Field[str | None]()
    longitude = Field[str | None]()
    location_detail = TextField()
    age_detail = TextField()
    source = Field[Article | None]("source_id", related_name="locations")
    deleted = Field[LocationStatus]()
    tags = ADTField["LocationTag"](is_ordered=False)
    parent = Field[Self | None]("parent_id", related_name="aliases")

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Location.deleted == LocationStatus.valid)

    def should_skip(self) -> bool:
        return self.deleted is not LocationStatus.valid

    def get_redirect_target(self) -> Location | None:
        if self.deleted is LocationStatus.alias:
            return self.parent
        return None

    def is_invalid(self) -> bool:
        return self.deleted is not LocationStatus.valid

    @classmethod
    def make(
        cls,
        name: str,
        region: Region,
        period: Period,
        comment: str | None = None,
        stratigraphic_unit: StratigraphicUnit | None = None,
    ) -> Location:
        return cls.create(
            name=name,
            min_period=period,
            max_period=period,
            region=region,
            comment=comment,
            stratigraphic_unit=stratigraphic_unit,
        )

    @classmethod
    def _get_unique_name(cls, name: str | None = None) -> str | None:
        while True:
            if name is None:
                name = getinput.get_line("name> ")
            if not name:
                return None
            try:
                existing = cls.select().filter(cls.name == name).get()
            except cls.DoesNotExist:
                return name
            print(f"A Location named {name!r} already exists; choose a different name.")
            existing.display()
            name = None

    @classmethod
    def _make_with_unique_name(
        cls,
        *,
        name: str,
        region: Region,
        period: Period,
        comment: str | None = None,
        **kwargs: Any,
    ) -> Location | None:
        while True:
            make_kwargs: dict[str, Any] = {
                "name": name,
                "region": region,
                "period": period,
                **kwargs,
            }
            if comment is not None:
                make_kwargs["comment"] = comment
            try:
                return cls.make(**make_kwargs)
            except sqlite3.IntegrityError:
                print(
                    f"Could not create Location {name!r} because that name is now "
                    "in use; choose a different name."
                )
                maybe_name = cls._get_unique_name()
                if maybe_name is None:
                    return None
                name = maybe_name

    @classmethod
    def create_interactively(
        cls,
        name: str | None = None,
        region: Region | None = None,
        period: Period | None = None,
        comment: str | None = None,
        **kwargs: Any,
    ) -> Location | None:
        name = cls._get_unique_name(name)
        if name is None:
            return None
        while region is None:
            region = cls.get_value_for_foreign_key_field_on_class(
                "region", allow_none=False
            )
        assert region is not None
        if period is None:
            period = cls.get_value_for_foreign_key_field_on_class("min_period")
        result = cls._make_with_unique_name(
            name=name, region=region, period=period, comment=comment, **kwargs
        )
        if result is not None:
            result.fill_required_fields()
        return result

    @classmethod
    def create_recent_interactively(cls) -> Location | None:
        recent = Period.filter(Period.name == "Recent").get()
        name = cls._get_unique_name()
        if name is None:
            return None
        region = cls.get_value_for_foreign_key_field_on_class(
            "region", allow_none=False
        )
        assert region is not None
        latitude = getinput.get_line("latitude> ") or None
        longitude = getinput.get_line("longitude> ") or None
        result = cls._make_with_unique_name(name=name, region=region, period=recent)
        if result is None:
            return None
        result.latitude = latitude
        result.longitude = longitude
        result.format()
        result.edit()
        return result

    def __repr__(self) -> str:
        parts = []
        if self.deleted is LocationStatus.alias:
            parts.append(f"alias for {self.parent}")
        elif self.deleted is LocationStatus.deleted:
            parts.append("deleted")
        if self.stratigraphic_unit is not None:
            parts.append(self.stratigraphic_unit.name)
        if self.max_period is not None:
            age_str = self.max_period.name
            if self.min_period is not None and self.min_period != self.max_period:
                age_str += f"–{self.min_period.name}"
            parts.append(age_str)
        if self.min_age is not None and self.max_age is not None:
            parts.append(f"{self.max_age}–{self.min_age}")
        if self.tags:
            parts.append(", ".join(repr(tag) for tag in self.tags))
        return f"{self.name} ({'; '.join(parts)}), {self.region.name}"

    def sort_key(self) -> Any:
        return (
            period_sort_key(self.min_period),
            period_sort_key(self.max_period),
            self.name,
        )

    def display(
        self,
        *,
        full: bool = False,
        organized: bool = False,
        include_occurrences: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
    ) -> None:
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if self.comment:
            space = " " * (depth + 12)
            file.write(f"{space}Comment: {self.comment}\n")
        type_locs = list(self.type_localities)
        models.name.name.write_names(
            type_locs, depth=depth, full=full, organized=organized, file=file
        )
        if include_occurrences or full:
            taxa = list(self.taxa)
            records = list(self.occurrence_records)
            if not taxa and not records:
                return
            file.write("{}Occurrences:\n".format(" " * (depth + 8)))
            if organized:
                models.taxon.display_organized(
                    [(str(occ), occ.taxon) for occ in taxa], depth=depth, file=file
                )
            else:
                for occurrence in sorted(taxa, key=lambda occ: occ.taxon.valid_name):
                    file.write("{}{}\n".format(" " * (depth + 12), occurrence))
            for record in sorted(
                records,
                key=lambda record: (
                    record.taxon.valid_name if record.taxon is not None else "",
                    record.locality_text,
                ),
            ):
                file.write("{}{}\n".format(" " * (depth + 12), record))

    def merge(self, other: Location | None = None) -> None:
        if other is None:
            other = self.getter(None).get_one()
            if other is None:
                return
        if other == self:
            raise ValueError("cannot merge a Location into itself")
        _merge_location_data(self, other)
        self.reassign_references(other)
        self.deleted = LocationStatus.alias
        self.parent = other

    def add_alias(self) -> Location | None:
        name = self.getter("name").get_one()
        if name is None:
            return None
        return Location.create(
            parent=self, deleted=LocationStatus.alias, name=name, region=self.region
        )

    def display_occurrences(
        self, *, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        type_localities = list(self.type_localities)
        records = list(self.occurrence_records)
        if type_localities:
            file.write("{}Type localities:\n".format(" " * (depth + 4)))
            for occurrence in sorted(
                type_localities, key=lambda occ: occ.taxon.valid_name
            ):
                file.write("{}{}\n".format(" " * (depth + 8), occurrence))
                file.write("{}{}\n".format(" " * (depth + 8), occurrence))
        if records:
            file.write("{}Occurrence records:\n".format(" " * (depth + 4)))
            for record in sorted(
                records,
                key=lambda record: (
                    record.taxon.valid_name if record.taxon is not None else "",
                    record.locality_text,
                ),
            ):
                file.write("{}{}\n".format(" " * (depth + 12), record))

    def open_coordinates(self) -> None:
        if self.latitude is None or self.longitude is None:
            return
        extent = coordinate_lint.make_extent(self.latitude, self.longitude)
        if extent is not None:
            subprocess.check_call(["open", *extent.openstreetmap_urls])

    def coordinate_evidence(self) -> None:
        from taxonomy.apis import geonames, nominatim, plss
        from taxonomy.db.models.name import TypeTag
        from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
        from taxonomy.db.models.occurrence_record.lint import parse_verbatim_coordinates

        from . import lint as location_lint

        print(f"Coordinate evidence for {self}:")
        location_parsed = None
        if self.latitude is None and self.longitude is None:
            print("  Location coordinates: none")
        else:
            location_parsed = (
                coordinate_lint.standardize_coordinate_pair(
                    self.latitude, self.longitude
                )
                if self.latitude is not None and self.longitude is not None
                else None
            )
            print(
                "  Location coordinates: "
                + _format_coordinate_evidence(self.latitude, self.longitude)
            )
        reference_extent = location_parsed[2] if location_parsed is not None else None

        print("\n  PLSS evidence:")
        plss_tags = list(self.get_tags(self.tags, LocationTag.PLSS))
        accepted_plss = None
        for tag in plss_tags:
            print(f"    Location tag: {tag!r}")
            plss_description = plss.parse_canonical(tag.text)
            if len(plss_tags) == 1 and plss_description is not None:
                accepted_plss = plss_description
            extent, issue = location_lint._get_plss_provenance_extent(
                self, LocationTag.CoordinatesFromPLSS(tag.plss_id)
            )
            if extent is not None:
                print(
                    "      Resolved coordinates: "
                    + _format_coordinate_evidence(
                        extent.latitude.standardized_text,
                        extent.longitude.standardized_text,
                        reference_extent=reference_extent,
                    )
                )
            elif issue is not None:
                print(f"      Resolution issue: {issue}")
        linked_plss = location_lint._get_linked_plss_evidence(self)
        for item in linked_plss:
            if item.has_alternative_section:
                status = "alternative sections"
            elif accepted_plss is None:
                status = "unreviewed"
            elif accepted_plss.is_compatible_with(item.description):
                status = "compatible with Location PLSS"
            else:
                status = "conflicts with Location PLSS"
            print(
                f"    [{status}] {item.description.canonical_text} "
                f"(from {item.source})"
            )
        if not plss_tags and not linked_plss:
            print("    none")

        print("\n  Nominatim candidates:")
        search_plan = location_lint.get_nominatim_search_plan(self)
        query = location_lint.get_nominatim_query(self)
        print(f"    Query: {query}")
        nominatim_results = None
        if not search_plan.coordinates_can_be_inferred:
            if self.name != search_plan.standardized_name:
                print(
                    "    Lookup skipped: rename noncanonical offset locality to "
                    f"{search_plan.standardized_name!r} first"
                )
            elif location_lint._get_coordinate_modifier_plan(self.name) is not None:
                print(
                    "    Lookup skipped: modifier supplies explicit locality "
                    "coordinates"
                )
            else:
                print(
                    "    Lookup skipped: modifier is not a fully parsed "
                    "distance offset"
                )
        else:
            try:
                nominatim_results = nominatim.search(query)
            except Exception as exc:
                print(f"    Lookup failed: {exc}")
        if nominatim_results is not None:
            if not nominatim_results:
                print("    none")
            for index, result in enumerate(nominatim_results, start=1):
                assessment = location_lint.assess_nominatim_result(self, result)
                status = "accepted" if assessment.is_accepted else "rejected"
                print(
                    f"    {index}. [{status}] {result.category}/"
                    f"{result.feature_type}: {result.display_name}"
                )
                if assessment.issues:
                    print("       Rejection reasons:")
                    for issue in assessment.issues:
                        print(f"         - {issue}")
                if assessment.notes:
                    print("       Match notes:")
                    for note in assessment.notes:
                        print(f"         - {note}")
                print(
                    "       Coordinates: "
                    + _format_coordinate_evidence(
                        result.latitude,
                        result.longitude,
                        reference_extent=reference_extent,
                    )
                )
                address = "; ".join(
                    f"{key}={value}"
                    for key, value in result.address.items()
                    if key != "country_code" and not key.startswith("ISO3166")
                )
                if address:
                    print(f"       Address: {address}")
                if search_plan.offset_description is not None:
                    offset_coordinates = location_lint.get_nominatim_result_coordinates(
                        result, offsets=search_plan.offsets
                    )
                    if offset_coordinates is not None:
                        print(
                            f"       After {search_plan.offset_description}: "
                            + _format_coordinate_evidence(
                                offset_coordinates[0],
                                offset_coordinates[1],
                                reference_extent=reference_extent,
                            )
                        )

        print("\n  GeoNames candidates:")
        geonames_country = location_lint._get_region_country_name(self.region)
        geonames_country_code = (
            None
            if geonames_country is None
            else location_lint._get_geonames_country_code(geonames_country)
        )
        print(
            f"    Exact-name query: {search_plan.locality_name!r}; "
            f"country={geonames_country_code or '(unresolved)'}"
        )
        geonames_candidates = location_lint._get_geonames_coordinate_matches(self)
        if not geonames_candidates:
            print("    none")
        for index, candidate in enumerate(geonames_candidates, start=1):
            if not candidate.is_accepted:
                status = "rejected by region checks"
            elif location_lint._is_geonames_point_candidate(candidate):
                status = "accepted for point-coordinate checks"
            else:
                status = "evidence only; feature is not point-like"
            record = candidate.match.record
            matched_as = (
                ""
                if candidate.match.match_kind == "name"
                else f"; matched {candidate.match.match_kind.replace('_', ' ')} "
                f"{candidate.match.matched_name!r}"
            )
            print(
                f"    {index}. [{status}] {record.feature_class}/"
                f"{record.feature_code}: {record.name} "
                f"(GeoNames ID {record.geoname_id}{matched_as})"
            )
            print(
                "       Coordinates: "
                + _format_coordinate_evidence(
                    candidate.latitude,
                    candidate.longitude,
                    reference_extent=reference_extent,
                )
            )
            administrative_codes = "; ".join(
                f"{label}={value}"
                for label, value in (
                    ("country", record.country_code),
                    ("admin1", record.admin1_code),
                    ("admin2", record.admin2_code),
                    ("admin3", record.admin3_code),
                    ("admin4", record.admin4_code),
                )
                if value
            )
            if administrative_codes:
                print(f"       Administrative codes: {administrative_codes}")
            try:
                administrative_hierarchy = geonames.get_administrative_hierarchy(record)
            except RuntimeError:
                administrative_hierarchy = []
            if administrative_hierarchy:
                hierarchy = "; ".join(
                    f"{item.feature_code}={item.name}"
                    for item in administrative_hierarchy
                )
                print(f"       Administrative hierarchy: {hierarchy}")
            if candidate.region_issues:
                print(f"       Region issues: {'; '.join(candidate.region_issues)}")

        print("\n  Type-locality Names:")
        found_type_locality_evidence = False
        for name in self.type_localities:
            coordinates = list(name.get_tags(name.type_tags, TypeTag.Coordinates))
            location_details = []
            for tag in location_lint._get_applicable_location_detail_tags(name):
                extracted = helpers.extract_coordinates(tag.text)
                if extracted is not None:
                    location_details.append(tag)
            if not coordinates and not location_details:
                continue
            found_type_locality_evidence = True
            print(f"    Name {name.id}: {name}")
            for tag in coordinates:
                print(
                    "      Coordinates tag: "
                    + _format_coordinate_evidence(
                        tag.latitude, tag.longitude, reference_extent=reference_extent
                    )
                )
            for tag in location_details:
                source = f"{tag.source}"
                if tag.page is not None:
                    source += f", page {tag.page}"
                print(f"      LocationDetail: {tag.text} (from {source})")
        if not found_type_locality_evidence:
            print("    none")

        print("\n  OccurrenceRecords:")
        found_occurrence_evidence = False
        for record in self.occurrence_records:
            coordinates = list(
                record.get_tags(record.tags, OccurrenceRecordTag.Coordinates)
            )
            verbatim = list(
                record.get_tags(record.tags, OccurrenceRecordTag.VerbatimCoordinates)
            )
            uncertainties = list(
                record.get_tags(
                    record.tags, OccurrenceRecordTag.CoordinateUncertaintyFromSource
                )
            )
            if not coordinates and not verbatim and not uncertainties:
                continue
            found_occurrence_evidence = True
            print(f"    OccurrenceRecord {record.id}: {record}")
            for tag in coordinates:
                print(
                    "      Coordinates tag: "
                    + _format_coordinate_evidence(
                        tag.latitude, tag.longitude, reference_extent=reference_extent
                    )
                )
            for tag in verbatim:
                verbatim_parsed = parse_verbatim_coordinates(tag.text)
                if verbatim_parsed is None:
                    print(f"      Verbatim coordinates: {tag.text!r} (unparsed)")
                else:
                    print(
                        f"      Verbatim coordinates: {tag.text!r} -> "
                        + _format_coordinate_evidence(
                            *verbatim_parsed, reference_extent=reference_extent
                        )
                    )
            for tag in uncertainties:
                print(f"      Coordinate uncertainty: {tag.text}")
        if not found_occurrence_evidence:
            print("    none")

    def _get_coordinate_choices(self) -> list[_CoordinateChoice]:
        from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
        from taxonomy.db.models.occurrence_record.lint import parse_verbatim_coordinates

        from . import lint as location_lint

        choices: dict[tuple[str, str], _CoordinateChoice] = {}

        def add_choice(
            latitude: str,
            longitude: str,
            source: str,
            provenance: adt.ADT | None = None,
        ) -> None:
            parsed = coordinate_lint.standardize_coordinate_pair(latitude, longitude)
            if parsed is None:
                return
            standardized_latitude, standardized_longitude, extent = parsed
            key = (standardized_latitude, standardized_longitude)
            if key in choices:
                choices[key].sources.append(source)
                if (
                    provenance is not None
                    and provenance not in choices[key].provenance_tags
                ):
                    choices[key].provenance_tags.append(provenance)
            else:
                choices[key] = _CoordinateChoice(
                    standardized_latitude,
                    standardized_longitude,
                    extent,
                    [source],
                    [] if provenance is None else [provenance],
                )

        for evidence in location_lint._get_linked_coordinate_evidence(self):
            add_choice(
                evidence.latitude,
                evidence.longitude,
                evidence.source,
                evidence.provenance,
            )

        for name in self.type_localities:
            for tag in location_lint._get_applicable_location_detail_tags(name):
                extracted = helpers.extract_coordinates(tag.text)
                if extracted is not None:
                    add_choice(
                        *extracted,
                        f"Name {name.id} LocationDetail from {tag.source}",
                        LocationTag.CoordinatesFromName(name),
                    )

        for record in self.occurrence_records:
            for tag in record.get_tags(
                record.tags, OccurrenceRecordTag.VerbatimCoordinates
            ):
                parsed = parse_verbatim_coordinates(tag.text)
                if parsed is not None:
                    add_choice(
                        *parsed,
                        f"OccurrenceRecord {record.id} verbatim coordinates",
                        LocationTag.CoordinatesFromOccurrenceRecord(record.id),
                    )

        for candidate in location_lint._get_accepted_geonames_coordinate_candidates(
            self
        ):
            add_choice(
                candidate.latitude,
                candidate.longitude,
                location_lint._describe_geonames_match(candidate.match),
                LocationTag.CoordinatesFromGeoNames(candidate.match.record.geoname_id),
            )

        try:
            if self.is_general():
                nominatim_candidates = (
                    location_lint._get_nominatim_bounding_box_candidates(self)
                )
                nominatim_source_type = "bounding box"
            else:
                nominatim_candidates = (
                    location_lint._get_nominatim_coordinate_candidates(self)
                )
                nominatim_source_type = "coordinates"
        except Exception as exc:
            print(f"Nominatim lookup failed: {exc}")
        else:
            for result, (latitude, longitude, _) in nominatim_candidates:
                provenance = location_lint._nominatim_provenance_tag(
                    result, use_bounding_box=self.is_general()
                )
                add_choice(
                    latitude,
                    longitude,
                    f"Nominatim {result.category}/{result.feature_type} "
                    f"{nominatim_source_type} for {result.display_name!r}",
                    provenance,
                )

        return list(choices.values())

    def pick_coordinates(self) -> None:
        if self.latitude is not None or self.longitude is not None:
            print(
                "Existing Location coordinates: "
                + _format_coordinate_evidence(self.latitude, self.longitude)
            )

        choices = self._get_coordinate_choices()
        if not choices:
            print("No coordinate choices found")
            return

        class CoordinatesCombined(Exception):
            pass

        def combine() -> None:
            combined_extent = choices[0].extent
            for choice in choices[1:]:
                combined_extent = combined_extent.union(choice.extent)
            self.latitude = combined_extent.latitude.standardized_text
            self.longitude = combined_extent.longitude.standardized_text
            for provenance in dict.fromkeys(
                tag for choice in choices for tag in choice.provenance_tags
            ):
                self.add_tag(provenance)
            print(f"Combined coordinates: {self.latitude}, {self.longitude}")
            raise CoordinatesCombined

        print("Type 'combine' to use a range encompassing every option.")
        try:
            choice = getinput.choose_one(
                choices,
                message="Pick coordinates (Enter to cancel)> ",
                display_fn=_CoordinateChoice.describe,
                history_key=("Location.pick_coordinates", self.id),
                callbacks={"combine": combine},
            )
        except CoordinatesCombined:
            return
        if choice is None:
            return
        self.latitude = choice.latitude
        self.longitude = choice.longitude
        for provenance in choice.provenance_tags:
            self.add_tag(provenance)
        print(f"Selected coordinates: {self.latitude}, {self.longitude}")

    def infer_coordinates(self) -> None:
        if self.latitude is not None or self.longitude is not None:
            print(f"{self}: already has coordinates")
            return
        from . import lint as location_lint

        cfg = LintConfig(autofix=True, interactive=True, manual_mode=True)
        messages = list(location_lint.check_linked_coordinates(self, cfg))
        if self.latitude is None and self.longitude is None:
            geonames_messages = list(
                location_lint.check_geonames_coordinates(self, cfg)
            )
            messages.extend(geonames_messages)
            if (
                not geonames_messages
                and self.latitude is None
                and self.longitude is None
            ):
                messages.extend(location_lint.check_nominatim_coordinates(self, cfg))
        for message in messages:
            print(message)

    def generalize(self) -> None:
        if not self.has_tag(LocationTag.General):
            self.add_tag(LocationTag.General)
        if self.latitude is not None and self.longitude is not None:
            extent = coordinate_lint.make_extent(self.latitude, self.longitude)
            if extent is not None and extent.point is not None:
                print(
                    f"Removing exact coordinates from general Location: {self.latitude} {self.longitude}"
                )
                self.latitude = None
                self.longitude = None
                self.tags = tuple(  # type: ignore[assignment]
                    tag
                    for tag in getattr(self, "tags", ()) or ()
                    if not is_coordinate_provenance_tag(tag)
                )
        self.format()

    def edit_imprecise_localities(self) -> None:
        nams = list(
            models.Name.add_validity_check(self.type_localities).filter(
                ~models.Name.type_tags.contains(
                    f"[{models.name.TypeTag.ImpreciseLocality._tag},"
                )
            )
        )
        if not nams:
            print(f"{self}: no type-locality Names to edit")
            return
        print(f"{self}: editing {len(nams)} type-locality Names")
        for nam in nams:
            if nam.type_locality != self or nam.has_type_tag(
                models.name.TypeTag.ImpreciseLocality
            ):
                continue
            nam.display()
            nam.edit()

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        article_callbacks = (
            self.source.get_shareable_adt_callbacks() if self.source is not None else {}
        )
        return {
            **callbacks,
            **article_callbacks,
            "add_alias": self.add_alias,
            "merge": self.merge,
            "display_occurrences": self.display_occurrences,
            "coordinate_evidence": self.coordinate_evidence,
            "pick_coordinates": self.pick_coordinates,
            "infer_coordinates": self.infer_coordinates,
            "generalize": self.generalize,
            "open_coordinates": self.open_coordinates,
            "edit_imprecise_localities": self.edit_imprecise_localities,
        }

    def edit(self) -> None:
        self.fill_field("tags")

    def reassign_references(self, other: Location) -> None:
        print(f"{self}: reassign references to {other}")
        for taxon in self.type_localities:
            taxon.type_locality = other
        for occ in self.taxa:
            occ.location = other
        for record in self.occurrence_records:
            record.location = other

    def set_period(self, period: Period | None) -> None:
        self.min_period = self.max_period = period

    def fill_field(self, field: str) -> None:
        if field == "period":
            period = self.get_value_for_foreign_class(
                "period",
                Period,
                default_obj=self.min_period,
                callbacks=self.get_wrapped_adt_callbacks(),
            )
            self.set_period(period)
        else:
            super().fill_field(field)

    def get_required_fields(self) -> Iterable[str]:
        yield "name"
        yield "max_period"
        yield "min_period"
        yield "stratigraphic_unit"
        yield "region"

    def has_tag(self, tag_cls: adt.ADT | type[adt.ADT]) -> bool:
        tag_id = tag_cls._tag
        return any(tag[0] == tag_id for tag in self.get_raw_tags_field("tags"))

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = (tag,)
        else:
            self.tags = (*self.tags, tag)  # type: ignore[assignment]

    def is_in_region(self, query: Region) -> bool:
        region = self.region
        while region is not None:
            if region == query:
                return True
            region = region.parent
        return False

    def is_empty(self) -> bool:
        if self.taxa.count():
            return False
        if self.occurrence_records.count():
            return False
        if self.type_localities.count():
            return False
        if self.specimen_set.count():
            return False
        return True

    def lint_invalid(self, cfg: LintConfig) -> Iterable[str]:
        if self.deleted is LocationStatus.alias and self.parent is None:
            yield "alias location has no parent"

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        from . import lint as location_lint

        yield from location_lint.LINT.run(self, cfg)

    @classmethod
    def clear_lint_caches(cls) -> None:
        from . import lint as location_lint

        location_lint.LINT.clear_caches()

    def is_general(self) -> bool:
        if self.has_tag(LocationTag.General):
            return True
        if (
            self.min_period == self.max_period
            and self.min_period is not None
            and get_expected_general_name(self.region, self.min_period) == self.name
        ):
            return True
        if (
            self.stratigraphic_unit is not None
            and self.name == f"{self.stratigraphic_unit.name} ({self.region.name})"
        ):
            return True
        return False

    # TODO: remove in favor of is_general()
    def should_be_specified(self) -> bool:
        if self.region.has_children() or self.is_general():
            return True
        if (
            self.min_period == self.max_period
            and self.min_period is not None
            and self.min_period.name != "Recent"
            and get_expected_general_name(self.region, self.min_period) == self.name
        ):
            return True
        return False

    @classmethod
    def fix_references(cls) -> None:
        for alias in cls.select_valid().filter(cls.deleted == LocationStatus.alias):
            if not alias.is_empty() and alias.parent:
                alias.reassign_references(alias.parent)

    @classmethod
    def get_or_create_general(cls, region: Region, period: Period) -> Location:
        name = get_expected_general_name(region, period)
        objs = list(Location.select_valid().filter(Location.name == name))
        if objs:
            return objs[0]  # should only be one

        objs = list(
            Location.select().filter(
                Location.name == name, Location.deleted == LocationStatus.deleted
            )
        )
        if objs:
            obj = objs[0]
            obj.deleted = LocationStatus.valid
            print(f"Resurrected {obj}")
            return obj

        else:
            obj = cls.make(name=name, region=region, period=period)
            if not (period.name == "Recent" and region.children.count() == 0):
                obj.tags = [LocationTag.General]  # type: ignore[assignment]
            print(f"Created {obj}")
            return obj

    @classmethod
    def autodelete(cls, *, dry_run: bool = False) -> None:
        for loc in cls.select_valid():
            loc.maybe_autodelete(dry_run=dry_run)

    def maybe_autodelete(self, *, dry_run: bool = True) -> None:
        if self.deleted is LocationStatus.alias or not self.is_empty():
            return
        print(f"Autodeleting {self!r}")
        if not dry_run:
            self.deleted = LocationStatus.deleted

    @classmethod
    def get_interactive_creators(cls) -> dict[str, Callable[[], Any]]:
        def callback() -> Location | None:
            region = models.Region.getter(None).get_one("region> ")
            if region is None:
                return None
            period = models.Period.getter(None).get_one("period> ")
            if period is None:
                return None
            return cls.get_or_create_general(region, period)

        return {
            **super().get_interactive_creators(),
            "r": cls.create_recent_interactively,
            "u": callback,
        }

    def most_common_words(self) -> Counter[str]:
        words: Counter[str] = Counter()
        for nam in self.type_localities:
            for tag in nam.type_tags:
                if isinstance(tag, models.name.TypeTag.LocationDetail):
                    print(tag.text)
                    text = re.sub(r"[^a-z ]", "", tag.text.lower())
                    for word in text.split():
                        words[word] += 1
        for word, count in words.most_common(100):
            print(count, word)
        return words

    search_fields: ClassVar[Sequence[SearchField]] = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.text, "comment", highlight_enabled=True),
        SearchField(SearchFieldType.text, "location_detail", highlight_enabled=True),
        SearchField(SearchFieldType.text, "age_detail", highlight_enabled=True),
        SearchField(SearchFieldType.text_array, "tags", highlight_enabled=True),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        data: dict[str, Any] = {
            "name": self.name,
            "comment": self.comment,
            "location_detail": self.location_detail,
            "age_detail": self.age_detail,
        }
        tags = []
        for tag in self.tags or ():
            if isinstance(tag, LocationTag.PBDB):
                tags.append(f"PBDB {tag.id}")
            elif isinstance(tag, LocationTag.ETMNA):
                tags.append(f"ETMNA {tag.id}")
            elif isinstance(tag, LocationTag.NOW):
                tags.append(f"NOW {tag.id}")
            elif isinstance(tag, LocationTag.PLSS):
                tags.append(f"PLSS {tag.plss_id} {tag.text}")
            elif isinstance(tag, LocationTag.CoordinatesFromPLSS):
                tags.append(f"coordinate provenance PLSS {tag.plss_id}")
            elif isinstance(tag, LocationTag.CoordinatesFromGeoNames):
                tags.append(f"coordinate provenance GeoNames {tag.geoname_id}")
            elif isinstance(tag, LocationTag.CoordinatesFromNominatim):
                tags.append(
                    "coordinate provenance OpenStreetMap "
                    f"{tag.osm_type} {tag.osm_id} {tag.category}"
                )
            elif isinstance(tag, LocationTag.CoordinatesFromName):
                tags.append(f"coordinate provenance Name {tag.name.id}")
            elif isinstance(tag, LocationTag.CoordinatesFromOccurrenceRecord):
                tags.append(
                    "coordinate provenance OccurrenceRecord "
                    f"{tag.occurrence_record_id}"
                )
            elif tag is LocationTag.CoordinatesFromLocationName:
                tags.append("coordinate provenance Location name")
            elif isinstance(tag, LocationTag.CoordinatesManual):
                tags.append(f"coordinate provenance manual {tag.comment}")
        if tags:
            data["tags"] = tags
        return [data]


class LocationTag(adt.ADT):
    # General locality; should be simplified if possible.
    General(tag=1)  # type: ignore[name-defined]

    # Locality identifiers in other databases

    # Paleobiology Database
    PBDB(id=Managed, tag=2)  # type: ignore[name-defined]

    # {North America Tertiary-localities.pdf}, appendix to
    # Evolution of Tertiary Mammals of North America
    ETMNA(id=Managed, tag=3)  # type: ignore[name-defined]

    # Neogene of the Old World database
    NOW(id=Managed, tag=4)  # type: ignore[name-defined]

    IgnoreLintLocation(  # type: ignore[name-defined]
        label=Managed, comment=NotRequired[Markdown], tag=5
    )

    # Indicate that after some research, it is unclear where this place is
    Unplaced(comment=NotRequired[Markdown], tag=6)  # type: ignore[name-defined]

    # Region that is nearby and used as a base for a disambiguator
    NearbyRegion(region=Region, tag=7)  # type: ignore[name-defined]

    # Reviewed Public Land Survey System description. The text is a canonical,
    # human-readable land description; plss_id is the township-level CadNSDI
    # PLSSID and therefore also records the resolved principal meridian.
    PLSS(  # type: ignore[name-defined]
        text=Managed, plss_id=Managed, comment=NotRequired[Markdown], tag=8
    )

    # Evidence supporting the Location's latitude and longitude fields. External
    # identifiers are snapshots of the object used, not cached coordinate values.
    CoordinatesFromPLSS(plss_id=Managed, tag=9)  # type: ignore[name-defined]
    CoordinatesFromGeoNames(geoname_id=Managed, tag=10)  # type: ignore[name-defined]
    CoordinatesFromNominatim(  # type: ignore[name-defined]
        osm_type=Managed,
        osm_id=Managed,
        category=Managed,
        use_bounding_box=Managed,
        tag=11,
    )
    CoordinatesFromName(  # type: ignore[name-defined]
        name=_CoordinatesFromNameTarget, text=NotRequired[Markdown], tag=12
    )
    CoordinatesFromOccurrenceRecord(  # type: ignore[name-defined]
        occurrence_record_id=Managed, tag=13
    )
    CoordinatesFromLocationName(tag=14)  # type: ignore[name-defined]
    CoordinatesManual(comment=Markdown, tag=15)  # type: ignore[name-defined]


COORDINATE_PROVENANCE_TAG_TYPES = (
    LocationTag.CoordinatesFromPLSS,
    LocationTag.CoordinatesFromGeoNames,
    LocationTag.CoordinatesFromNominatim,
    LocationTag.CoordinatesFromName,
    LocationTag.CoordinatesFromOccurrenceRecord,
    LocationTag.CoordinatesManual,
)


def is_coordinate_provenance_tag(tag: adt.ADT) -> bool:
    return tag is LocationTag.CoordinatesFromLocationName or isinstance(
        tag, COORDINATE_PROVENANCE_TAG_TYPES
    )


def get_expected_general_name(region: Region, period: Period) -> str:
    if period.name == "Recent":
        return region.name
    elif period.name == "Phanerozoic":
        return f"{region.name} fossil"
    elif period.name == "Pleistocene":
        return f"{region.name} Pleistocene"
    else:
        return f"{period.name} ({region.name})"
