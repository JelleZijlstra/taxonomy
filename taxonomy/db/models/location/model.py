from __future__ import annotations

import enum
import re
import sqlite3
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from typing import IO, Any, ClassVar, NotRequired, Self

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
        if include_occurrences:
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
            subprocess.check_call(["open", extent.openstreetmap_url])

    def coordinate_evidence(self) -> None:
        from taxonomy.apis import geonames, nominatim
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
                status = (
                    "accepted"
                    if location_lint.is_sane_nominatim_result(self, result)
                    else "rejected by locality/region checks"
                )
                print(
                    f"    {index}. [{status}] {result.category}/"
                    f"{result.feature_type}: {result.display_name}"
                )
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
            for tag in name.get_tags(name.type_tags, TypeTag.LocationDetail):
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
                parsed = parse_verbatim_coordinates(tag.text)
                if parsed is None:
                    print(f"      Verbatim coordinates: {tag.text!r} (unparsed)")
                else:
                    print(
                        f"      Verbatim coordinates: {tag.text!r} -> "
                        + _format_coordinate_evidence(
                            *parsed, reference_extent=reference_extent
                        )
                    )
            for tag in uncertainties:
                print(f"      Coordinate uncertainty: {tag.text}")
        if not found_occurrence_evidence:
            print("    none")

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
                self.latitude = None
                self.longitude = None
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
        return self.has_tag(LocationTag.General) or self.name in (
            self.region.name,
            f"{self.region.name} fossil",
            f"{self.region.name} Pleistocene",
        )

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
        data = {
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


def get_expected_general_name(region: Region, period: Period) -> str:
    if period.name == "Recent":
        return region.name
    elif period.name == "Phanerozoic":
        return f"{region.name} fossil"
    elif period.name == "Pleistocene":
        return f"{region.name} Pleistocene"
    else:
        return f"{period.name} ({region.name})"
