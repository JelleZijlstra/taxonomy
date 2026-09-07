import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pytest

from taxonomy.applicator import type_locality as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import (
    DistributionOrigin,
    OccurrenceValidity,
    RegionKind,
    SpeciesGroupType,
)
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


def _schema_four_target(
    *,
    location_id: int | None,
    location_name: str,
    region_id: int,
    region_name: str,
    fossil: bool = True,
) -> dict[str, object]:
    period_id = 10 if fossil else 11
    period_name = "Pleistocene" if fossil else "Recent"
    return {
        "location_id": location_id,
        "location_name": location_name,
        "region_id": region_id,
        "region_name": region_name,
        "latitude": None,
        "longitude": None,
        "coordinate_source": None,
        "coordinate_note": None,
        "location_tags": [],
        "min_period_id": period_id,
        "min_period_name": period_name,
        "max_period_id": period_id,
        "max_period_name": period_name,
        "min_age": None,
        "max_age": None,
        "stratigraphic_unit_id": None,
        "stratigraphic_unit_name": None,
        "serialized_location_tags": [],
    }


def _partial_row() -> dict[str, object]:
    row = _row(action=recommendations.SET_PARTIAL_TYPE_LOCALITIES)
    row.update(
        {
            "schema_version": 4,
            "tag_comment": None,
            "current_location_tags": [],
            "target": _schema_four_target(
                location_id=200,
                location_name="Puy-de-Dôme fossil",
                region_id=20,
                region_name="Puy-de-Dôme",
            ),
            "partial_targets": [
                _schema_four_target(
                    location_id=201,
                    location_name="Coudes",
                    region_id=20,
                    region_name="Puy-de-Dôme",
                ),
                _schema_four_target(
                    location_id=None,
                    location_name="Neschers",
                    region_id=20,
                    region_name="Puy-de-Dôme",
                ),
            ],
            "current_partial_type_localities": [],
        }
    )
    return row


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


@dataclass
class FakeRegion:
    id: int
    name: str
    parent: recommendations.RegionLike | None = None


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
    species_type_kind: SpeciesGroupType | None = None

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
                cast(recommendations.RegionLike, region),
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


def test_review_table_expands_changes_and_can_filter_actions(
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
    assert "Western Africa [existing]" in output
    assert "Name 2" in output
    assert "Name 1" not in output
    assert "    - current location: Africa" in output
    assert "    - change: type_locality: Africa -> Western Africa" in output
    assert "    - current location tags:" not in output
    assert "    - target:" not in output
    assert "#3684" not in output
    assert "1 recommendation(s)" in output


def test_review_table_collapses_named_target_fields_and_omits_empty_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row(
        name_id=73453,
        action=recommendations.MOVE_EXISTING_LOCATION,
        target=_schema_four_target(
            location_id=19426,
            location_name="Vaupés Department",
            region_id=4305,
            region_name="Vaupés Department",
            fossil=False,
        ),
    )
    row.update(
        {
            "schema_version": 4,
            "name": "Caiman sclerops apaporiensis",
            "current_location_id": 19397,
            "current_location_name": "Amazonas Department (Colombia)",
            "current_location_tags": [],
            "tag_comment": None,
        }
    )
    _write_rows(path, [row])

    recommendations.print_review_table(recommendations.read_recommendations(path))

    output = capsys.readouterr().out
    assert "Caiman sclerops apaporiensis" in output
    assert "Vaupés Department [existing]" in output
    assert "    - current location: Amazonas Department (Colombia)" in output
    assert (
        "    - change: type_locality: Amazonas Department (Colombia) -> "
        "Vaupés Department" in output
    )
    assert "    - target: period='Recent'" in output
    assert "73453" not in output
    assert "19397" not in output
    assert "19426" not in output
    assert "4305" not in output
    assert "None" not in output
    assert "()" not in output
    assert "_id=" not in output


def test_review_prints_manual_evidence_without_truncation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _row(action=recommendations.MANUAL_REVIEW)
    row["tag_comment"] = None
    row["evidence"] = [
        {
            "source_id": 10,
            "source_name": "Source",
            "text": "Exact evidence " + "that must remain visible " * 10,
        }
    ]
    _write_rows(path, [row])

    recommendations.print_review_table(recommendations.read_recommendations(path))

    output = capsys.readouterr().out
    assert "    - evidence (Source): Exact evidence" in output
    assert "that must remain visible " * 10 in output


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


def test_schema_four_requires_multiple_partial_targets() -> None:
    row = _partial_row()
    row["partial_targets"] = cast(list[object], row["partial_targets"])[:1]

    with pytest.raises(
        recommendations.RecommendationError, match="at least two targets"
    ):
        recommendations.parse_recommendation(row, 1)


def test_builds_partial_type_locality_plan_with_new_location(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(_partial_row(), 1)
    puy = FakeRegion(20, "Puy-de-Dôme")
    fossil_period = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    broad = FakeLocation(
        200,
        "Puy-de-Dôme fossil",
        puy,
        min_period=fossil_period,
        max_period=fossil_period,
    )
    coudes = FakeLocation(
        201, "Coudes", puy, min_period=fossil_period, max_period=fossil_period
    )
    name = FakeName(1, "Name 1", current)

    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={1176: current, 200: broad, 201: coudes}.__getitem__,
        get_region=lambda _: puy,
        get_period=lambda _: fossil_period,
        find_location=lambda _: None,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    assert [definition.target.location_name for definition in plan.new_locations] == [
        "Neschers"
    ]
    assert plan.updates[0].target is broad
    assert len(plan.updates[0].partial_targets) == 2
    recommendations.execute_plan(plan, apply=False)
    output = capsys.readouterr().out
    assert "WOULD_CREATE_LOCATION name='Neschers'" in output
    assert "WOULD_SET_PARTIAL_TYPE_LOCALITIES name_id=1" in output
    assert "partials=Coudes,Neschers" in output
    assert name.type_locality is current


def test_partial_type_locality_plan_gets_or_creates_general_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _partial_row()
    target = cast(dict[str, object], data["target"])
    target.update(
        {
            "location_id": None,
            "location_name": "Puy-de-Dôme fossil",
            "min_period_id": 1,
            "min_period_name": "Phanerozoic",
            "max_period_id": 1,
            "max_period_name": "Phanerozoic",
            "location_tags": [{"tag": "General", "comment": None}],
        }
    )
    partial_targets = cast(list[dict[str, object]], data["partial_targets"])
    partial_targets[1]["location_id"] = 202
    row = recommendations.parse_recommendation(data, 1)
    region = FakeRegion(20, "Puy-de-Dôme")
    phanerozoic = FakeRegion(1, "Phanerozoic")
    pleistocene = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    coudes = FakeLocation(
        201, "Coudes", region, min_period=pleistocene, max_period=pleistocene
    )
    neschers = FakeLocation(
        202, "Neschers", region, min_period=pleistocene, max_period=pleistocene
    )
    name = FakeName(1, "Name 1", current)
    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={1176: current, 201: coudes, 202: neschers}.__getitem__,
        get_region=lambda _: region,
        get_period=lambda period_id: (phanerozoic if period_id == 1 else pleistocene),
        find_location=lambda _: None,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    definition = plan.new_locations[0]
    assert definition.target.location_name == "Puy-de-Dôme fossil"
    assert definition.use_general_factory
    assert plan.updates[0].target is None
    assert plan.updates[0].new_location_name == "Puy-de-Dôme fossil"

    broad = FakeLocation(
        200,
        "Puy-de-Dôme fossil",
        region,
        min_period=phanerozoic,
        max_period=phanerozoic,
    )
    monkeypatch.setattr(
        Location,
        "get_or_create_general",
        classmethod(lambda cls, requested_region, period: cast(Location, broad)),
    )
    recommendations.execute_plan(plan, apply=True)

    assert name.type_locality is broad
    assert {
        tag.location.id
        for tag in name.type_tags or ()
        if isinstance(tag, TypeTag.PartialTypeLocality)
    } == {201, 202}


def test_partial_type_locality_plan_can_revive_deleted_general_container() -> None:
    data = _partial_row()
    target = cast(dict[str, object], data["target"])
    target.update(
        {
            "location_id": None,
            "min_period_id": 1,
            "min_period_name": "Phanerozoic",
            "max_period_id": 1,
            "max_period_name": "Phanerozoic",
        }
    )
    row = recommendations.parse_recommendation(data, 1)
    region = FakeRegion(20, "Puy-de-Dôme")
    phanerozoic = FakeRegion(1, "Phanerozoic")
    pleistocene = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    deleted = FakeLocation(
        200,
        "Puy-de-Dôme fossil",
        region,
        min_period=phanerozoic,
        max_period=phanerozoic,
        deleted=LocationStatus.deleted,
    )
    coudes = FakeLocation(
        201, "Coudes", region, min_period=pleistocene, max_period=pleistocene
    )
    name = FakeName(1, "Name 1", current)

    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={1176: current, 201: coudes}.__getitem__,
        get_region=lambda _: region,
        get_period=lambda period_id: (phanerozoic if period_id == 1 else pleistocene),
        find_location=lambda location_name: (
            deleted if location_name == "Puy-de-Dôme fossil" else None
        ),
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    definition = next(
        item
        for item in plan.new_locations
        if item.target.location_name == "Puy-de-Dôme fossil"
    )
    assert definition.use_general_factory
    assert definition.deleted_location is deleted


def test_partial_type_locality_plan_rejects_stale_tags() -> None:
    row = recommendations.parse_recommendation(_partial_row(), 1)
    puy = FakeRegion(20, "Puy-de-Dôme")
    period = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    broad = FakeLocation(
        200, "Puy-de-Dôme fossil", puy, min_period=period, max_period=period
    )
    coudes = FakeLocation(201, "Coudes", puy, min_period=period, max_period=period)
    unexpected = FakeLocation(203, "Aubière", puy, min_period=period, max_period=period)
    name = FakeName(
        1,
        "Name 1",
        current,
        type_tags=(TypeTag.PartialTypeLocality(cast(Location, unexpected)),),
    )

    with pytest.raises(
        recommendations.RecommendationError,
        match="current PartialTypeLocality tags differ",
    ):
        recommendations.build_plan(
            [row],
            get_name=lambda _: name,
            get_location={1176: current, 200: broad, 201: coudes}.__getitem__,
            get_region=lambda _: puy,
            get_period=lambda _: period,
            find_location=lambda _: None,
            label_name=lambda item: item.label,  # type: ignore[attr-defined]
        )


def test_partial_type_locality_plan_rejects_holotype() -> None:
    row = recommendations.parse_recommendation(_partial_row(), 1)
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    name = FakeName(1, "Name 1", current, species_type_kind=SpeciesGroupType.holotype)

    with pytest.raises(recommendations.RecommendationError, match="syntypes or unset"):
        recommendations.build_plan(
            [row],
            get_name=lambda _: name,
            get_location=lambda _: current,
            label_name=lambda item: item.label,  # type: ignore[attr-defined]
        )


def test_partial_type_locality_plan_is_idempotent() -> None:
    data = _partial_row()
    partial_targets = cast(list[dict[str, object]], data["partial_targets"])
    partial_targets[1]["location_id"] = 202
    row = recommendations.parse_recommendation(data, 1)
    puy = FakeRegion(20, "Puy-de-Dôme")
    period = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    broad = FakeLocation(
        200, "Puy-de-Dôme fossil", puy, min_period=period, max_period=period
    )
    coudes = FakeLocation(201, "Coudes", puy, min_period=period, max_period=period)
    neschers = FakeLocation(202, "Neschers", puy, min_period=period, max_period=period)
    name = FakeName(
        1,
        "Name 1",
        broad,
        type_tags=(
            TypeTag.PartialTypeLocality(cast(Location, coudes)),
            TypeTag.PartialTypeLocality(cast(Location, neschers)),
        ),
    )

    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={
            1176: current,
            200: broad,
            201: coudes,
            202: neschers,
        }.__getitem__,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    assert plan.updates[0].already_applied


def test_applies_partial_type_localities_to_existing_locations() -> None:
    data = _partial_row()
    partial_targets = cast(list[dict[str, object]], data["partial_targets"])
    partial_targets[1]["location_id"] = 202
    row = recommendations.parse_recommendation(data, 1)
    puy = FakeRegion(20, "Puy-de-Dôme")
    period = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    broad = FakeLocation(
        200, "Puy-de-Dôme fossil", puy, min_period=period, max_period=period
    )
    coudes = FakeLocation(201, "Coudes", puy, min_period=period, max_period=period)
    neschers = FakeLocation(202, "Neschers", puy, min_period=period, max_period=period)
    name = FakeName(1, "Name 1", current)
    plan = recommendations.build_plan(
        [row],
        get_name=lambda _: name,
        get_location={
            1176: current,
            200: broad,
            201: coudes,
            202: neschers,
        }.__getitem__,
        label_name=lambda item: item.label,  # type: ignore[attr-defined]
    )

    recommendations.execute_plan(plan, apply=True)

    assert name.type_locality is broad
    assert {
        tag.location.id
        for tag in name.type_tags or ()
        if isinstance(tag, TypeTag.PartialTypeLocality)
    } == {coudes.id, neschers.id}


def test_virtual_proposal_links_partial_type_localities() -> None:
    row = recommendations.parse_recommendation(_partial_row(), 1)
    region = Region.virtual(name="Puy-de-Dôme", kind=RegionKind.other, tags=())
    period = Period.virtual(name="Pleistocene")
    current = Location.virtual(
        name="Brèche de Coudes",
        region=region,
        min_period=period,
        max_period=period,
        tags=(),
    )
    broad = Location.virtual(
        name="Puy-de-Dôme fossil",
        region=region,
        min_period=period,
        max_period=period,
        tags=(LocationTag.General,),
    )
    neschers = Location.virtual(
        name="Neschers", region=region, min_period=period, max_period=period, tags=()
    )
    name = Name.virtual(type_locality=current, type_tags=())
    plan = recommendations.RecommendationPlan(
        updates=(
            recommendations.PlannedUpdate(
                row,
                cast(recommendations.NameLike, name),
                target=cast(recommendations.LocationLike, broad),
                new_location_name=None,
                already_applied=False,
                partial_targets=(
                    recommendations.PlannedPartialTarget(
                        cast(recommendations.LocationLike, current), None
                    ),
                    recommendations.PlannedPartialTarget(
                        cast(recommendations.LocationLike, neschers), None
                    ),
                ),
            ),
        ),
        new_locations=(),
        location_tag_updates=(),
        serialized_location_tag_updates=(),
        type_locality_validity_updates=(),
        regional_origin_updates=(),
        action_counts=Counter({recommendations.SET_PARTIAL_TYPE_LOCALITIES: 1}),
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposed_name = next(
        proposal.model
        for proposal in builder.build()
        if isinstance(proposal.model, Name)
    )
    assert proposed_name.type_locality is broad
    partials = [
        tag.location
        for tag in proposed_name.type_tags
        if isinstance(tag, TypeTag.PartialTypeLocality)
    ]
    assert set(partials) == {current, neschers}


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


def test_explicitly_restore_deleted_named_location() -> None:
    target = _schema_four_target(
        location_id=None,
        location_name="Neschers",
        region_id=20,
        region_name="Puy-de-Dôme",
    )
    target["restore_deleted_location_id"] = 202
    row = _row(action=recommendations.CREATE_LOCATION, target=target)
    row.update(schema_version=4, current_location_tags=[])
    region = FakeRegion(20, "Puy-de-Dôme")
    period = FakeRegion(10, "Pleistocene")
    current = FakeLocation(1176, "Africa", FakeRegion(629, "Africa"))
    deleted = FakeLocation(
        202,
        "Neschers",
        region,
        min_period=period,
        max_period=period,
        deleted=LocationStatus.deleted,
    )
    name = FakeName(1, "Name 1", current)

    def build(data: dict[str, object]) -> recommendations.RecommendationPlan:
        return recommendations.build_plan(
            [recommendations.parse_recommendation(data, 1)],
            get_name=lambda _: name,
            get_location=lambda _: current,
            get_region=lambda _: region,
            get_period=lambda _: period,
            find_location=lambda _: deleted,
            label_name=lambda item: item.label,  # type: ignore[attr-defined]
        )

    plan = build(row)
    assert plan.new_locations[0].deleted_location is deleted
    recommendations.execute_plan(plan, apply=False)
    assert deleted.deleted is LocationStatus.deleted
    recommendations.execute_plan(plan, apply=True)
    assert name.type_locality is deleted
    assert int(deleted.deleted) == LocationStatus.valid
    assert deleted.id == 202
    # A matching record restored by an earlier run is reused.
    name.type_locality = current
    assert not build(row).new_locations
    target["location_id"] = 202
    row["action"] = recommendations.MOVE_EXISTING_LOCATION
    with pytest.raises(
        recommendations.RecommendationError, match="requires location_id=null"
    ):
        build(row)
    target["location_id"] = None
    row["action"] = recommendations.CREATE_LOCATION

    deleted.deleted = LocationStatus.deleted
    name.type_locality = current
    target["restore_deleted_location_id"] = 999
    with pytest.raises(
        recommendations.RecommendationError, match="expected Location 999"
    ):
        build(row)
    del target["restore_deleted_location_id"]
    with pytest.raises(recommendations.RecommendationError, match="is invalid"):
        build(row)
    target["restore_deleted_location_id"] = 202
    deleted.deleted = LocationStatus.alias
    with pytest.raises(recommendations.RecommendationError, match="is invalid"):
        build(row)


def test_virtual_restore_preserves_existing_location_data() -> None:
    target = _schema_four_target(
        location_id=None,
        location_name="Neschers",
        region_id=20,
        region_name="Puy-de-Dôme",
    )
    target["restore_deleted_location_id"] = 202
    data = _row(action=recommendations.CREATE_LOCATION, target=target)
    data.update(schema_version=4, current_location_tags=[])
    row = recommendations.parse_recommendation(data, 1)
    assert row.target is not None
    region = Region.virtual(name="Puy-de-Dôme", kind=RegionKind.other, tags=())
    period = Period.virtual(name="Pleistocene")
    deleted = Location.virtual(
        name="Neschers",
        region=region,
        min_period=period,
        max_period=period,
        deleted=LocationStatus.deleted,
        comment="Existing source note",
        min_age=100,
        max_age=200,
        latitude="45°N",
        longitude="3°E",
        tags=(LocationTag.General,),
    )
    current = Location.virtual(name="Africa", region=region, tags=())
    name = Name.virtual(type_locality=current, type_tags=())
    plan = recommendations.RecommendationPlan(
        updates=(
            recommendations.PlannedUpdate(
                row,
                cast(recommendations.NameLike, name),
                target=None,
                new_location_name="Neschers",
                already_applied=False,
            ),
        ),
        new_locations=(
            recommendations.NewLocationDefinition(
                row.target,
                cast(recommendations.RegionLike, region),
                min_period=period,
                max_period=period,
                stratigraphic_unit=None,
                deleted_location=cast(recommendations.LocationLike, deleted),
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
    restored = builder.replacement(deleted)
    assert restored is not deleted
    assert restored.deleted is LocationStatus.valid
    assert (restored.min_age, restored.max_age) == (100, 200)
    assert restored.comment == deleted.comment
    assert (restored.latitude, restored.longitude) == ("45°N", "3°E")
    assert restored.tags == deleted.tags
    assert builder.replacement(name).type_locality is restored
    assert deleted.deleted is LocationStatus.deleted
    assert name.type_locality is current
