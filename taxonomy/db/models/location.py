from __future__ import annotations

import enum
import re
import sqlite3
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from typing import IO, Any, ClassVar, Self

from clirm import Field

from taxonomy import adt, events, getinput
from taxonomy.apis import nominatim
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.config import is_network_available
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import Managed, RegionKind

from .article import Article
from .base import ADTField, BaseModel, LintConfig, TextField
from .period import Period, period_sort_key
from .region import Region
from .stratigraphic_unit import StratigraphicUnit


class LocationStatus(enum.IntEnum):
    valid = 0
    deleted = 1
    alias = 2


_GEOCODABLE_OSM_CATEGORIES = {"boundary", "natural", "place", "waterway"}
_OSM_CATEGORY_PRIORITY = {"place": 0, "natural": 1, "waterway": 2, "boundary": 3}
_ADDRESS_REGION_KINDS = {
    RegionKind.country,
    RegionKind.subnational,
    RegionKind.county,
    RegionKind.state,
    RegionKind.province,
    RegionKind.department,
    RegionKind.region,
    RegionKind.canton,
    RegionKind.prefecture,
    RegionKind.territory,
}
_SEARCH_REGION_KINDS = _ADDRESS_REGION_KINDS | {RegionKind.other, RegionKind.island}


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
        point = coordinate_lint.make_point(self.latitude, self.longitude)
        if point is not None:
            subprocess.check_call(["open", point.openstreetmap_url])

    def infer_coordinates(self) -> None:
        if self.latitude is not None or self.longitude is not None:
            print(f"{self}: already has coordinates")
            return
        messages = list(
            self.lint_missing_coordinates(
                LintConfig(autofix=True, interactive=True, manual_mode=True)
            )
        )
        for message in messages:
            print(message)

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
            "infer_coordinates": self.infer_coordinates,
            "open_coordinates": self.open_coordinates,
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
        if self.deleted is LocationStatus.alias:
            if not self.parent:
                yield f"{self}: alias location has no parent"
            if not self.is_empty():
                yield f"{self}: alias location has references"

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        if self.min_period is None and self.max_period is not None:
            yield f"{self}: missing min_period"
        if self.max_period is None and self.min_period is not None:
            yield f"{self}: missing max_period"
        if self.latitude is None and self.longitude is None:
            yield from self.lint_missing_coordinates(cfg)
            return
        if self.latitude is None:
            yield f"{self}: missing latitude"
            return
        if self.longitude is None:
            yield f"{self}: missing longitude"
            return
        if self.is_general():
            yield f"{self}: general location should not have coordinates"
            return
        try:
            latitude, _ = coordinate_lint.standardize_coordinate(
                self.latitude, is_latitude=True
            )
            longitude, _ = coordinate_lint.standardize_coordinate(
                self.longitude, is_latitude=False
            )
        except helpers.InvalidCoordinates:
            yield f"{self}: invalid coordinates {self.latitude}, {self.longitude}"
            return
        if (latitude, longitude) != (self.latitude, self.longitude):
            message = (
                f"{self}: coordinates should be {latitude}, {longitude}, not "
                f"{self.latitude}, {self.longitude}"
            )
            if cfg.autofix:
                print(message)
                self.latitude = latitude
                self.longitude = longitude
            else:
                yield message
        point = coordinate_lint.make_point(self.latitude, self.longitude)
        assert point is not None
        for message in coordinate_lint.check_point_in_region(point, self.region):
            yield f"{self}: {message}"

    def is_general(self) -> bool:
        return self.has_tag(LocationTag.General) or self.name in (
            self.region.name,
            f"{self.region.name} fossil",
            f"{self.region.name} Pleistocene",
        )

    def lint_missing_coordinates(self, cfg: LintConfig) -> Iterable[str]:
        # General Recent locations such as "Ecuador" are deliberately not points.
        # Avoid loading all their reverse relationships as well as avoiding a noisy
        # and misleading lint.
        if self.is_general():
            return
        if self.stratigraphic_unit is not None and re.fullmatch(
            rf"{self.stratigraphic_unit.name} \(.*\)", self.name
        ):
            return

        from .name import TypeTag
        from .occurrence_record import OccurrenceRecordTag

        candidates: list[tuple[str, str, Any, str]] = []
        for name in self.type_localities:
            for tag in name.get_tags(name.type_tags, TypeTag.Coordinates):
                parsed = coordinate_lint.standardize_coordinate_pair(
                    tag.latitude, tag.longitude
                )
                if parsed is not None:
                    latitude, longitude, point = parsed
                    candidates.append((latitude, longitude, point, f"Name {name}"))
        for record in self.occurrence_records:
            for tag in record.get_tags(record.tags, OccurrenceRecordTag.Coordinates):
                parsed = coordinate_lint.standardize_coordinate_pair(
                    tag.latitude, tag.longitude
                )
                if parsed is not None:
                    latitude, longitude, point = parsed
                    candidates.append(
                        (latitude, longitude, point, f"OccurrenceRecord {record}")
                    )
        if not candidates:
            yield from self.lint_nominatim_coordinates(cfg)
            return

        latitude, longitude, point, source = candidates[0]
        for _, _, other_point, other_source in candidates[1:]:
            distance = coordinate_lint.distance_km(point, other_point)
            if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
                yield (
                    f"{self}: cannot infer coordinates because {point} (from {source}) and "
                    f"{other_point} (from {other_source}) differ by {distance:.1f} km "
                    "[linked_coordinates]"
                )
                return

        message = (
            f"{self}: coordinates should be {latitude}, {longitude}, inferred from "
            f"{source} [linked_coordinates]"
        )
        if cfg.autofix:
            print(message)
            self.latitude = latitude
            self.longitude = longitude
        else:
            yield message

    def lint_nominatim_coordinates(self, cfg: LintConfig) -> Iterable[str]:
        """Infer coordinates from an exact, geographically consistent OSM match."""
        if not is_network_available():
            return

        query = self.get_nominatim_query()
        results = nominatim.search(query)
        candidates: list[tuple[nominatim.SearchResult, tuple[str, str, Any]]] = []
        for result in results:
            if not self.is_sane_nominatim_result(result):
                continue
            parsed = coordinate_lint.standardize_coordinate_pair(
                result.latitude, result.longitude
            )
            if parsed is not None:
                candidates.append((result, parsed))

        if not candidates:
            return

        candidates.sort(
            key=lambda candidate: _OSM_CATEGORY_PRIORITY[candidate[0].category]
        )
        result, (latitude, longitude, point) = candidates[0]
        has_conflict = any(
            coordinate_lint.distance_km(point, other_point)
            > coordinate_lint.COORDINATE_TOLERANCE_KM
            for _, (_, _, other_point) in candidates[1:]
        )
        if has_conflict:
            matches = "".join(
                f"- {candidate.display_name!r} "
                f"({candidate.category}/{candidate.feature_type}, "
                f"{candidate_latitude}, {candidate_longitude})\n"
                for candidate, (
                    candidate_latitude,
                    candidate_longitude,
                    _,
                ) in candidates
            )
            yield (
                f"{self}: Nominatim returned {len(candidates)} conflicting exact "
                f"matches:\n{matches}[nominatim_coordinates]"
            )
            return

        region_issues = list(coordinate_lint.check_point_in_region(point, self.region))
        if region_issues:
            yield (
                f"{self}: Nominatim result {result.display_name!r} failed the region "
                f"check: {'; '.join(region_issues)} [nominatim_coordinates]"
            )
            return

        message = (
            f"{self}: coordinates should be {latitude}, {longitude}, inferred from "
            f"OpenStreetMap Nominatim {result.category}/{result.feature_type} result "
            f"{result.display_name!r} [nominatim_coordinates]"
        )
        if cfg.autofix:
            print(message)
            self.latitude = latitude
            self.longitude = longitude
        else:
            yield message

    def get_nominatim_query(self) -> str:
        components = [self.name]
        seen = {helpers.simplify_string(self.name, clean_words=False)}
        for region in (self.region, *self.region.all_parents()):
            if region.kind not in _SEARCH_REGION_KINDS:
                continue
            name = _get_unqualified_region_name(region)
            simplified = helpers.simplify_string(name, clean_words=False)
            if simplified not in seen:
                components.append(name)
                seen.add(simplified)
        return ", ".join(components)

    def is_sane_nominatim_result(self, result: nominatim.SearchResult) -> bool:
        if result.category not in _GEOCODABLE_OSM_CATEGORIES:
            return False
        if helpers.simplify_string(
            result.name, clean_words=False
        ) != helpers.simplify_string(self.name, clean_words=False):
            return False

        address_names = {
            helpers.simplify_string(value, clean_words=False)
            for key, value in result.address.items()
            if key not in {"country_code", "postcode"} and not key.startswith("ISO3166")
        }
        checked_region = False
        for region in (self.region, *self.region.all_parents()):
            if region.kind not in _ADDRESS_REGION_KINDS:
                continue
            checked_region = True
            expected_names = {
                helpers.simplify_string(name, clean_words=False)
                for name in _get_region_name_aliases(region)
            }
            if address_names.isdisjoint(expected_names):
                return False
        return checked_region

    def should_be_specified(self) -> bool:
        if self.region.has_children():
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


def _get_unqualified_region_name(region: Region) -> str:
    if region.parent is not None:
        parent_suffix = f", {region.parent.name}"
        if region.name.endswith(parent_suffix):
            return region.name.removesuffix(parent_suffix)
    return region.name


def _get_region_name_aliases(region: Region) -> set[str]:
    names = {region.name, _get_unqualified_region_name(region)}
    if region.kind is RegionKind.country:
        names.add(nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name))
    return names


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


def get_expected_general_name(region: Region, period: Period) -> str:
    if period.name == "Recent":
        return region.name
    elif period.name == "Phanerozoic":
        return f"{region.name} fossil"
    elif period.name == "Pleistocene":
        return f"{region.name} Pleistocene"
    else:
        return f"{period.name} ({region.name})"
