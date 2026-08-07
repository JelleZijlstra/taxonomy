import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pytest

from taxonomy.applicator import type_locality as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import DistributionOrigin, OccurrenceValidity, RegionKind
from taxonomy.db.models import Location, Name, Period, Region, TypeTag
from taxonomy.db.models.location import LocationStatus
from taxonomy.db.models.tags import LocationTag, TaxonTag


def _row(
    *,
    name_id: int = 1,
    action: str = recommendations.ADD_IMPRECISE_LOCALITY,
    target: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "name_id": name_id,
        "name": f"Name {name_id}",
        "current_location_id": 1176,
        "current_location_name": "Africa",
        "action": action,
        "confidence": "high",
        "reason": "Recorded only as Africa.",
        "tag_comment": (
            "Type locality recorded only as Africa."
            if action == recommendations.ADD_IMPRECISE_LOCALITY
            else None
        ),
        "target": target,
        "evidence": [{"source_id": 10, "source_name": "Source", "text": "Africa"}],
    }


def _existing_target() -> dict[str, object]:
    return {
        "location_id": 3684,
        "location_name": "Western Africa",
        "region_id": 795,
        "region_name": "Western Africa",
        "latitude": None,
        "longitude": None,
        "coordinate_source": None,
        "coordinate_note": None,
    }


def _new_target() -> dict[str, object]:
    return {
        "location_id": None,
        "location_name": "Farajala",
        "region_id": 163,
        "region_name": "South Sudan",
        "latitude": "5.17814°N",
        "longitude": "31.76002°E",
        "coordinate_source": "https://example.test/farajala",
        "coordinate_note": "Mapped forest matching the historical locality.",
    }


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


@dataclass
class FakeRegion:
    id: int
    name: str


@dataclass
class FakeLocation:
    id: int
    name: str
    region: recommendations.RegionLike
    latitude: str | None = None
    longitude: str | None = None
    min_period: recommendations.NamedLike | None = None
    max_period: recommendations.NamedLike | None = None
    min_age: int | None = None
    max_age: int | None = None
    stratigraphic_unit: recommendations.NamedLike | None = None
    deleted: LocationStatus = LocationStatus.valid
    tags: Sequence[LocationTag] | None = field(default_factory=tuple)

    def is_invalid(self) -> bool:
        return self.deleted is not LocationStatus.valid

    def has_tag(self, tag_type: object) -> bool:
        tags = self.tags or ()
        if tag_type is LocationTag.General:
            return any(tag is LocationTag.General for tag in tags)
        return any(isinstance(tag, tag_type) for tag in tags)  # type: ignore[arg-type]

    def add_tag(self, tag: LocationTag) -> None:
        tags = self.tags or ()
        if tag not in tags:
            self.tags = (*tags, tag)


@dataclass
class FakeArticle:
    id: int
    name: str


@dataclass
class FakeTaxon:
    id: int = 100
    tags: Sequence[TaxonTag] | None = field(default_factory=list)

    def add_tag(self, tag: TaxonTag) -> None:
        if self.tags is None:
            self.tags = [tag]
        elif tag not in self.tags:
            self.tags = (*self.tags, tag)


@dataclass
class FakeName:
    id: int
    label: str
    type_locality: recommendations.LocationLike | None
    taxon: recommendations.TaxonLike = field(default_factory=FakeTaxon)
    type_tags: Sequence[TypeTag] | None = field(default_factory=list)

    def add_type_tag(self, tag: TypeTag) -> None:
        if self.type_tags is None:
            self.type_tags = [tag]
        elif tag not in self.type_tags:
            self.type_tags = (*self.type_tags, tag)


def test_reads_jsonl_and_standardizes_coordinates(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row(action=recommendations.CREATE_LOCATION, target=_new_target())
    row["tag_comment"] = None
    _write_rows(path, [row])

    parsed = recommendations.read_recommendations(path)

    assert len(parsed) == 1
    assert parsed[0].target is not None
    assert parsed[0].target.latitude == "5.17814°N"
    assert parsed[0].target.longitude == "31.76002°E"


def test_reads_optional_actionable_review_note(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row(action=recommendations.CREATE_LOCATION, target=_new_target())
    row["tag_comment"] = None
    row["review_note"] = "Review the historical epoch terminology before applying."
    _write_rows(path, [row])

    parsed = recommendations.read_recommendations(path)

    assert parsed[0].review_note == (
        "Review the historical epoch terminology before applying."
    )


def test_virtual_proposal_links_name_to_new_virtual_location() -> None:
    row = recommendations.parse_recommendation(
        _row(action=recommendations.CREATE_LOCATION, target=_new_target()), 1
    )
    assert row.target is not None
    region = Region.virtual(name="South Sudan", kind=RegionKind.country, tags=())
    period = Period.virtual(name="Recent")
    current = Location.virtual(name="Africa", region=region, tags=())
    name = Name.virtual(type_locality=current, type_tags=())
    plan = recommendations.RecommendationPlan(
        updates=(
            recommendations.PlannedUpdate(
                row,
                cast(recommendations.NameLike, name),
                target=None,
                new_location_name="Farajala",
                already_applied=False,
            ),
        ),
        new_locations=(
            recommendations.NewLocationDefinition(
                row.target,
                region,
                min_period=period,
                max_period=period,
                stratigraphic_unit=None,
            ),
        ),
        location_tag_updates=(),
        serialized_location_tag_updates=(),
        type_locality_validity_updates=(),
        regional_origin_updates=(),
        action_counts=Counter({recommendations.CREATE_LOCATION: 1}),
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposals = builder.build()
    proposed_location = next(
        proposal.model
        for proposal in proposals
        if isinstance(proposal.model, Location)
        and proposal.model.virtual_origin is None
    )
    proposed_name = next(
        proposal.model for proposal in proposals if isinstance(proposal.model, Name)
    )
    assert proposed_location.name == "Farajala"
    assert proposed_location.virtual_origin_id is None
    assert proposed_name.type_locality is proposed_location
    assert name.type_locality is current


def test_rejects_duplicate_name_ids(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write_rows(path, [_row(), _row()])

    with pytest.raises(recommendations.RecommendationError, match="duplicate name_id"):
        recommendations.read_recommendations(path)


def test_review_table_is_concise_and_can_filter_actions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    move_row = _row(
        name_id=2,
        action=recommendations.MOVE_EXISTING_LOCATION,
        target=_existing_target(),
    )
    move_row["tag_comment"] = None
    _write_rows(path, [_row(), move_row])
    parsed = recommendations.read_recommendations(path)

    recommendations.print_review_table(
        parsed, actions={recommendations.MOVE_EXISTING_LOCATION}
    )

    output = capsys.readouterr().out
    assert "Western Africa [existing #3684]" in output
    assert "Name 2" in output
    assert "Name 1" not in output
    assert "1 recommendation(s)" in output


def test_dry_run_adds_tag_and_moves_without_mutating(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    move_row = _row(
        name_id=2,
        action=recommendations.MOVE_EXISTING_LOCATION,
        target=_existing_target(),
    )
    move_row["tag_comment"] = None
    _write_rows(path, [_row(), move_row])
    parsed = recommendations.read_recommendations(path)
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    western = FakeLocation(3684, "Western Africa", FakeRegion(795, "Western Africa"))
    names = {1: FakeName(1, "Name 1", africa), 2: FakeName(2, "Name 2", africa)}
    locations = {1176: africa, 3684: western}
    plan = recommendations.build_plan(
        parsed,
        get_name=names.__getitem__,
        get_location=locations.__getitem__,
        label_name=lambda name: name.label,  # type: ignore[attr-defined]
    )

    recommendations.execute_plan(plan, apply=False)

    assert not names[1].type_tags
    assert names[2].type_locality is africa
    output = capsys.readouterr().out
    assert "WOULD_ADD_IMPRECISE_LOCALITY name_id=1" in output
    assert "WOULD_MOVE name_id=2" in output
    assert "No database changes made" in output


def test_move_accepts_separately_planned_target_name() -> None:
    row = recommendations.parse_recommendation(
        _row(action=recommendations.MOVE_EXISTING_LOCATION, target=_existing_target()),
        1,
    )
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    western = FakeLocation(3684, "West Africa", FakeRegion(795, "Western Africa"))
    name = FakeName(1, "Name 1", western)

    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={1176: africa, 3684: western}.__getitem__,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
        allowed_target_names={3684: {"West Africa"}},
    )

    assert plan.updates[0].already_applied


def test_apply_adds_tag_and_moves(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    move_row = _row(
        name_id=2,
        action=recommendations.MOVE_EXISTING_LOCATION,
        target=_existing_target(),
    )
    move_row["tag_comment"] = None
    _write_rows(path, [_row(), move_row])
    parsed = recommendations.read_recommendations(path)
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    western = FakeLocation(3684, "Western Africa", FakeRegion(795, "Western Africa"))
    names = {1: FakeName(1, "Name 1", africa), 2: FakeName(2, "Name 2", africa)}
    locations = {1176: africa, 3684: western}
    plan = recommendations.build_plan(
        parsed,
        get_name=names.__getitem__,
        get_location=locations.__getitem__,
        label_name=lambda name: name.label,  # type: ignore[attr-defined]
    )

    recommendations.execute_plan(plan, apply=True)

    assert any(
        isinstance(tag, TypeTag.ImpreciseLocality) for tag in names[1].type_tags or ()
    )
    assert names[2].type_locality is western
    assert "ADD_IMPRECISE_LOCALITY name_id=1" in capsys.readouterr().out


def test_groups_shared_new_location(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    rows = []
    for name_id in (1, 2):
        row = _row(
            name_id=name_id,
            action=recommendations.CREATE_LOCATION,
            target=_new_target(),
        )
        row["tag_comment"] = None
        rows.append(row)
    _write_rows(path, rows)
    parsed = recommendations.read_recommendations(path)
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    names = {
        name_id: FakeName(name_id, f"Name {name_id}", africa) for name_id in (1, 2)
    }
    regions = {163: FakeRegion(163, "South Sudan")}

    plan = recommendations.build_plan(
        parsed,
        get_name=names.__getitem__,
        get_location=lambda _: africa,
        get_region=regions.__getitem__,
        find_location=lambda _: None,
        label_name=lambda name: name.label,  # type: ignore[attr-defined]
    )

    assert len(plan.new_locations) == 1
    assert len(plan.updates) == 2


def test_schema_two_adds_general_and_unplaced_location_tags(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    target = _existing_target()
    target["location_tags"] = [{"tag": "Unplaced", "comment": "Placement unresolved."}]
    row = _row(action=recommendations.MOVE_EXISTING_LOCATION, target=target)
    row["schema_version"] = 2
    row["tag_comment"] = None
    row["current_location_tags"] = [{"tag": "General", "comment": None}]
    _write_rows(path, [row])

    parsed = recommendations.read_recommendations(path)
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    western = FakeLocation(3684, "Western Africa", FakeRegion(795, "Western Africa"))
    name = FakeName(1, "Name 1", africa)
    plan = recommendations.build_plan(
        parsed,
        get_name=lambda _: name,
        get_location={1176: africa, 3684: western}.__getitem__,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    assert len(plan.location_tag_updates) == 2
    recommendations.execute_plan(plan, apply=True)

    assert africa.has_tag(LocationTag.General)
    assert western.has_tag(LocationTag.Unplaced)
    output = capsys.readouterr().out
    assert "ADD_LOCATION_TAGS location_id=1176" in output
    assert "ADD_LOCATION_TAGS location_id=3684" in output


def test_schema_two_requires_location_tag_fields(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row()
    row["schema_version"] = 2
    _write_rows(path, [row])

    with pytest.raises(recommendations.RecommendationError, match="location tags"):
        recommendations.read_recommendations(path)


def test_schema_three_preserves_fossil_context_and_serialized_tags(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recommendations.jsonl"
    target = _new_target()
    target.update(
        {
            "location_tags": [],
            "min_period_id": 1324,
            "min_period_name": "Bl5",
            "max_period_id": 1324,
            "max_period_name": "Bl5",
            "min_age": None,
            "max_age": None,
            "stratigraphic_unit_id": 327,
            "stratigraphic_unit_name": "Crooked Creek Formation",
            "serialized_location_tags": [
                LocationTag.PLSS(
                    "T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0"
                ).serialize()
            ],
        }
    )
    row = _row(action=recommendations.CREATE_LOCATION, target=target)
    row["schema_version"] = 3
    row["tag_comment"] = None
    row["current_location_tags"] = []
    _write_rows(path, [row])
    parsed = recommendations.read_recommendations(path)
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    name = FakeName(1, "Name 1", current)
    period = FakeRegion(1324, "Bl5")
    unit = FakeRegion(327, "Crooked Creek Formation")

    plan = recommendations.build_plan(
        parsed,
        get_name=lambda _: name,
        get_location=lambda _: current,
        get_region=lambda _: FakeRegion(163, "South Sudan"),
        get_period=lambda _: period,
        get_stratigraphic_unit=lambda _: unit,
        find_location=lambda _: None,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    assert plan.new_locations[0].min_period is period
    assert plan.new_locations[0].max_period is period
    assert plan.new_locations[0].stratigraphic_unit is unit
    assert plan.new_locations[0].target.serialized_location_tags == (
        LocationTag.PLSS("T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0"),
    )


def test_applies_existing_incidental_and_introduced_tags(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row(action=recommendations.MANUAL_REVIEW)
    row["tag_comment"] = None
    row["type_locality_validity"] = {
        "validity": "incidental",
        "comment": "Obtained through a dealer.",
    }
    row["regional_origins"] = [
        {
            "region_id": 240,
            "region_name": "Réunion",
            "origin": "introduced",
            "source_id": 10,
            "source_name": "Source",
            "comment": "The source identifies an introduced population.",
        }
    ]
    _write_rows(path, [row])

    parsed = recommendations.read_recommendations(path)
    africa = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    reunion = FakeRegion(240, "Réunion")
    article = FakeArticle(10, "Source")
    taxon = FakeTaxon()
    name = FakeName(1, "Name 1", africa, taxon)
    plan = recommendations.build_plan(
        parsed,
        get_name=lambda _: name,
        get_location=lambda _: africa,
        get_region=lambda _: reunion,
        get_article=lambda _: article,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    recommendations.execute_plan(plan, apply=True)

    assert any(
        isinstance(tag, TypeTag.TypeLocalityValidity)
        and tag.validity is OccurrenceValidity.incidental
        for tag in name.type_tags or ()
    )
    assert any(
        isinstance(tag, TaxonTag.RegionalOrigin)
        and tag.origin is DistributionOrigin.introduced
        for tag in taxon.tags or ()
    )
    output = capsys.readouterr().out
    assert "ADD_TYPE_LOCALITY_VALIDITY" in output
    assert "ADD_REGIONAL_ORIGIN" in output
