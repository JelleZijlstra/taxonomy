import csv
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import gspread
import pytest

from scripts import mdd_taxa
from taxonomy import getinput
from taxonomy.db.constants import (
    AgeClass,
    DistributionOrigin,
    DistributionPresence,
    OccurrenceBasis,
    OccurrenceValidity,
)
from taxonomy.db.models.name import Name, TypeTag
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
from taxonomy.db.models.tags import TaxonTag
from taxonomy.db.models.taxon import Taxon


def _location(country: str, name: str = "precise locality") -> SimpleNamespace:
    return _location_with_regions(name, country)


def _location_with_regions(*region_names: str) -> SimpleNamespace:
    regions = [SimpleNamespace(name=name) for name in region_names]
    for index, region in enumerate(regions):
        region.all_parents = lambda index=index: iter(regions[index + 1 :])
    return SimpleNamespace(
        name=region_names[0], region=regions[0], min_age=None, min_period=None
    )


def _taxon_with_occurrence_record() -> Taxon:
    location = _location("Venezuela")
    article = SimpleNamespace(name="source.pdf")
    record = SimpleNamespace(
        id=12,
        location=location,
        tags=(),
        classification_entry=SimpleNamespace(article=article),
        basis=OccurrenceBasis.voucher,
        locality_text="locality from source",
        get_page=lambda: "3",
        get_absolute_url=lambda: "https://hesperomys.com/or/12",
    )
    return cast(
        Taxon,
        SimpleNamespace(
            id=13, age=AgeClass.extant, tags=(), occurrence_records=[record]
        ),
    )


def test_taxon_distribution_evidence_includes_occurrence_records() -> None:
    evidence = mdd_taxa.get_taxon_distribution_evidence(_taxon_with_occurrence_record())

    assert [(item.kind, item.country) for item in evidence] == [
        ("OccurrenceRecord", "Venezuela")
    ]
    assert evidence[0].record == "OR:12"
    assert evidence[0].source == "source.pdf"


def test_rejected_occurrence_record_is_not_distribution_evidence() -> None:
    taxon = _taxon_with_occurrence_record()
    record = cast(Any, taxon).occurrence_records[0]
    record.tags = (OccurrenceRecordTag.ValidityAssessment(OccurrenceValidity.rejected),)

    assert mdd_taxa.get_taxon_distribution_evidence(taxon) == []


@pytest.mark.parametrize(
    "tag",
    [
        OccurrenceRecordTag.ValidityAssessment(OccurrenceValidity.rejected),
        OccurrenceRecordTag.ValidityFromSource(OccurrenceValidity.occurrence_dubious),
        OccurrenceRecordTag.ValidityAssessment(OccurrenceValidity.incidental),
    ],
)
def test_status_tag_excludes_occurrence_record_from_distribution(tag: object) -> None:
    taxon = _taxon_with_occurrence_record()
    record = cast(Any, taxon).occurrence_records[0]
    record.tags = (tag,)

    assert mdd_taxa.get_taxon_distribution_evidence(taxon) == []


@pytest.mark.parametrize(
    "tag",
    [
        OccurrenceRecordTag.OriginFromSource(DistributionOrigin.introduced),
        OccurrenceRecordTag.PresenceFromSource(DistributionPresence.vagrant),
    ],
)
def test_nonnative_and_vagrant_records_are_distribution_evidence(tag: object) -> None:
    taxon = _taxon_with_occurrence_record()
    record = cast(Any, taxon).occurrence_records[0]
    record.tags = (tag,)

    assert len(mdd_taxa.get_taxon_distribution_evidence(taxon)) == 1


def test_regional_extirpation_excludes_living_taxon() -> None:
    taxon = _taxon_with_occurrence_record()
    record = cast(Any, taxon).occurrence_records[0]
    region = record.location.region
    cast(Any, taxon).tags = (
        TaxonTag.RegionalPresence(
            region, DistributionPresence.extirpated, SimpleNamespace()
        ),
    )

    assert mdd_taxa.get_taxon_distribution_evidence(taxon) == []


def test_regional_extirpation_keeps_globally_extinct_taxon() -> None:
    taxon = _taxon_with_occurrence_record()
    taxon.age = AgeClass.recently_extinct
    record = cast(Any, taxon).occurrence_records[0]
    region = record.location.region
    cast(Any, taxon).tags = (
        TaxonTag.RegionalPresence(
            region, DistributionPresence.extirpated, SimpleNamespace()
        ),
    )

    assert len(mdd_taxa.get_taxon_distribution_evidence(taxon)) == 1


@pytest.mark.parametrize(
    ("regions", "expected"),
    [
        (("Galápagos Islands", "Ecuador", "South America"), "Galápagos Islands"),
        (("Christmas Island", "Australia", "Oceania"), "Christmas Island"),
        (("Bolívar", "Venezuela", "South America"), "Venezuela"),
    ],
)
def test_country_for_location_uses_most_specific_mdd_unit(
    regions: tuple[str, ...], expected: str
) -> None:
    assert mdd_taxa._country_for_location(_location_with_regions(*regions)) == expected


@pytest.mark.parametrize(
    ("regions", "mdd_country", "expected"),
    [
        (("Galápagos Islands", "Ecuador"), "Galápagos Islands", []),
        (("Ecuador",), "Galápagos Islands", ["Ecuador"]),
        (("Galápagos Islands", "Ecuador"), "Ecuador", ["Galápagos Islands"]),
        (("Christmas Island", "Australia"), "Christmas Island", []),
        (("Australia",), "Christmas Island", ["Australia"]),
    ],
)
def test_type_locality_country_comparison_keeps_mdd_units_independent(
    monkeypatch: pytest.MonkeyPatch,
    regions: tuple[str, ...],
    mdd_country: str,
    expected: list[str],
) -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Aegialomys_galapagoensis",
                "countryDistribution": mdd_country,
            },
        ),
    )
    syn = {
        "MDD_type_country": "Ecuador",
        "MDD_syn_ID": "1",
        "MDD_original_type_locality": regions[0],
        "MDD_authority_citation": "source.pdf",
        "MDD_root_name": "galapagoensis",
        "MDD_authority_parentheses": "0",
        "MDD_year": "1839",
        "MDD_nomenclature_status": "available",
        "MDD_author": "Waterhouse",
    }
    species_with_syns = mdd_taxa.SpeciesWithSyns(species, {}, [syn])
    name = cast(
        Name,
        SimpleNamespace(
            id=1,
            taxon=SimpleNamespace(age=AgeClass.extant, tags=()),
            type_locality=_location_with_regions(*regions),
            type_tags=(),
            get_absolute_url=lambda: "https://hesperomys.com/n/1",
        ),
    )
    monkeypatch.setattr(mdd_taxa, "_get_hesp_name", lambda syn: name)

    evidence = species_with_syns.get_distribution_evidence(None, {mdd_country})

    assert [item.country for item in evidence] == expected


@pytest.mark.parametrize(
    ("age", "tags", "youngest_age", "expected"),
    [
        (AgeClass.extant, (), None, 1),
        (AgeClass.extant, (), 0, 1),
        (AgeClass.extant, (), 11_700, 0),
        (AgeClass.fossil, (), None, 0),
        (
            AgeClass.extant,
            (TypeTag.TypeLocalityValidity(OccurrenceValidity.occurrence_dubious),),
            None,
            0,
        ),
        (AgeClass.recently_extinct, (), None, 1),
    ],
)
def test_name_type_locality_distribution_evidence(
    age: AgeClass, tags: tuple[object, ...], youngest_age: int | None, expected: int
) -> None:
    period = (
        None
        if youngest_age is None
        else SimpleNamespace(
            name="Recent" if youngest_age == 0 else "Pleistocene", min_age=youngest_age
        )
    )
    location = SimpleNamespace(min_age=None, min_period=period, max_period=period)
    name = SimpleNamespace(
        taxon=SimpleNamespace(age=age, tags=()), type_locality=location, type_tags=tags
    )

    assert mdd_taxa._name_type_locality_is_distribution_evidence(
        cast(Name, name)
    ) is bool(expected)


def test_distribution_lists_are_sorted_with_uncertain_countries_last() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_species",
                "subregionDistribution": "Wyoming|Alabama|California",
                "countryDistribution": "Venezuela|Argentina?|Brazil|Bolivia?",
                "continentDistribution": "South America|Africa|Asia",
                "biogeographicRealm": "Neotropic|Afrotropic|Nearctic",
            },
        ),
    )

    issues = {
        issue.mdd_column: issue.suggested_change
        for issue in species.lint_distribution_order()
    }

    assert issues == {
        "subregionDistribution": "Alabama|California|Wyoming",
        "countryDistribution": "Brazil|Venezuela|Argentina?|Bolivia?",
        "continentDistribution": "Africa|Asia|South America",
        "biogeographicRealm": "Afrotropic|Nearctic|Neotropic",
    }


def test_subregions_within_geographic_range_are_sorted() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_species",
                "subregionDistribution": (
                    "USA(KY,AL,MS,MD,FL,GA,WV,IL,AR,VA,LA,NC,IN,SC,MO,OK,TN)"
                ),
            },
        ),
    )

    issues = list(species.lint_distribution_order())

    assert len(issues) == 1
    assert issues[0].mdd_column == "subregionDistribution"
    assert issues[0].suggested_change == (
        "USA(AL,AR,FL,GA,IL,IN,KY,LA,MD,MO,MS,NC,OK,SC,TN,VA,WV)"
    )


def test_uncertain_subregions_are_sorted_last() -> None:
    assert mdd_taxa._sort_subregions("USA(MS?,AL,KY?)?") == "USA(AL,KY?,MS?)?"


@pytest.mark.parametrize(
    ("column", "value", "expected"),
    [
        (
            "taxonomyNotes",
            "split from A.  source;  recently described",
            "split from A. source; recently described",
        ),
        (
            "distributionNotes",
            "Known from the north.   Also recorded in the south.",
            "Known from the north. Also recorded in the south.",
        ),
        ("typeLocality", "East of  Lake Example", "East of Lake Example"),
        ("taxonomyNotesCitation", "Example  2026. Title.", "Example 2026. Title."),
    ],
)
def test_repeated_spaces_in_free_form_text_are_fixable(
    column: str, value: str, expected: str
) -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {"id": "1234", "sciName": "Example_species", column: value},
        ),
    )

    issues = list(species.lint_repeated_spaces())

    assert len(issues) == 1
    assert issues[0].mdd_column == column
    assert issues[0].suggested_change == expected


def test_repeated_spaces_outside_free_form_text_are_not_changed() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_species",
                "countryDistribution": "United  States",
            },
        ),
    )

    assert list(species.lint_repeated_spaces()) == []


@pytest.mark.parametrize("taxonomy_notes", ["", "NA"])
def test_new_since_msw3_requires_taxonomy_notes(taxonomy_notes: str) -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_species",
                "taxonomyNotes": taxonomy_notes,
                "MSW3_sciName": "NA",
                "diffSinceMSW3": "1",
            },
        ),
    )

    issues = list(species.lint_msw3_taxonomy_notes())

    assert len(issues) == 1
    assert issues[0].mdd_column == "taxonomyNotes"
    assert issues[0].suggested_change is None
    assert "marked as new since MSW3" in issues[0].description


def test_changed_name_since_msw3_requires_taxonomy_notes() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_emended",
                "taxonomyNotes": "NA",
                "MSW3_sciName": "Example_original",
                "diffSinceMSW3": "0",
            },
        ),
    )

    issues = list(species.lint_msw3_taxonomy_notes())

    assert len(issues) == 1
    assert "Example_original" in issues[0].description


@pytest.mark.parametrize(
    ("sci_name", "msw3_sci_name", "diff_since_msw3", "taxonomy_notes"),
    [
        ("Example_species", "Example_species", "0", "NA"),
        ("Example_species", "Example_original", "0", "spelling corrected"),
        ("Example_species", "NA", "1", "recently described"),
    ],
)
def test_msw3_taxonomy_note_requirement_is_satisfied(
    sci_name: str, msw3_sci_name: str, diff_since_msw3: str, taxonomy_notes: str
) -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": sci_name,
                "taxonomyNotes": taxonomy_notes,
                "MSW3_sciName": msw3_sci_name,
                "diffSinceMSW3": diff_since_msw3,
            },
        ),
    )

    assert list(species.lint_msw3_taxonomy_notes()) == []


def test_msw3_source_check_fixes_wrong_name_when_current_name_is_in_msw3() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1001254",
                "sciName": "Erethizon_dorsatum",
                "MSW3_sciName": "Erethizon_dorsata",
                "MSW3_matchtype": "manual",
            },
        ),
    )

    issues = {
        issue.mdd_column: issue
        for issue in mdd_taxa.lint_msw3_classification(
            [species], {"Erethizon_dorsatum"}
        )
    }

    assert issues["MSW3_sciName"].suggested_change == "Erethizon_dorsatum"
    assert "MSW3_matchtype" not in issues


def test_msw3_source_check_establishes_new_exact_match() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Exact_species",
                "MSW3_sciName": "NA",
                "MSW3_matchtype": "unmatched",
            },
        ),
    )

    issues = {
        issue.mdd_column: issue
        for issue in mdd_taxa.lint_msw3_classification([species], {"Exact_species"})
    }

    assert issues["MSW3_sciName"].suggested_change == "Exact_species"
    assert issues["MSW3_matchtype"].suggested_change == "sciname match"


def test_msw3_source_check_accepts_valid_manual_mapping() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Current_species",
                "MSW3_sciName": "Former_species",
                "MSW3_matchtype": "manual",
            },
        ),
    )

    assert (
        list(
            mdd_taxa.lint_msw3_classification(
                [species], {"Former_species", "Other_species"}
            )
        )
        == []
    )


def test_msw3_source_check_preserves_historical_direct_match_provenance() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Current_species",
                "MSW3_sciName": "Former_species",
                "MSW3_matchtype": "sciname match",
            },
        ),
    )

    assert (
        list(
            mdd_taxa.lint_msw3_classification(
                [species], {"Former_species", "Other_species"}
            )
        )
        == []
    )


def test_msw3_source_check_flags_name_absent_from_msw3() -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Current_species",
                "MSW3_sciName": "Misspelled_species",
                "MSW3_matchtype": "manual",
            },
        ),
    )

    issues = list(mdd_taxa.lint_msw3_classification([species], {"Actual_species"}))

    assert len(issues) == 1
    assert issues[0].mdd_column == "MSW3_sciName"
    assert issues[0].suggested_change is None
    assert "not found" in issues[0].description


@pytest.mark.parametrize(
    ("msw3_name", "match_type", "description_fragment"),
    [
        ("Former_species", "unmatched", "name is recorded"),
        ("NA", "manual", "no scientific name is recorded"),
    ],
)
def test_msw3_source_check_flags_inconsistent_match_type(
    msw3_name: str, match_type: str, description_fragment: str
) -> None:
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Current_species",
                "MSW3_sciName": msw3_name,
                "MSW3_matchtype": match_type,
            },
        ),
    )

    issues = list(mdd_taxa.lint_msw3_classification([species], {"Former_species"}))

    assert len(issues) == 1
    assert issues[0].mdd_column == "MSW3_matchtype"
    assert issues[0].suggested_change is None
    assert description_fragment in issues[0].description


def test_iso_country_conversion_sorts_uncertain_countries_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mdd_taxa, "USE_ISO_3166", True)
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Example_species",
                "countryDistribution": "Venezuela|Argentina?|Brazil",
            },
        ),
    )

    issues = [
        issue
        for issue in species.lint_distribution_standalone()
        if issue.description.startswith("convert to ISO")
    ]

    assert [issue.suggested_change for issue in issues] == ["BR|VE|AR?"]


def test_write_distribution_problems_one_row_per_country(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    taxon = _taxon_with_occurrence_record()
    species = mdd_taxa.MDDSpecies(
        2,
        cast(
            mdd_taxa.MDDSpeciesRow,
            {
                "id": "1234",
                "sciName": "Echimys_chrysurus",
                "countryDistribution": "Guyana",
            },
        ),
    )
    species_with_syns = mdd_taxa.SpeciesWithSyns(species, {}, [])
    monkeypatch.setattr(mdd_taxa.SpeciesWithSyns, "get_hesp_taxon", lambda self: taxon)
    output_path = tmp_path / "distribution_problems.csv"

    problems = mdd_taxa.write_distribution_problems(output_path, [species_with_syns])

    assert len(problems) == 1
    with output_path.open() as file:
        rows = list(csv.DictReader(file))
    assert len(rows) == 1
    assert rows[0]["species"] == "Echimys_chrysurus"
    assert rows[0]["missing_country"] == "Venezuela"
    assert rows[0]["suggested_country_distribution"] == "Guyana|Venezuela"
    assert rows[0]["evidence_count"] == "1"
    assert rows[0]["evidence_types"] == "OccurrenceRecord"


def test_parse_changefile_with_comments_and_manual_review(tmp_path: Path) -> None:
    path = tmp_path / "changes.jsonl"
    path.write_text(
        '{"taxon_id": 1234, "column": "taxonomyNotes", "old_text": "old", '
        '"action": "replace", '
        '"new_text": "new, text", "comment": "Fix typo #1"}\n'
        '{"taxon_id": "5678", "column": "distributionNotes", '
        '"old_text": "unclear", "action": "manual_review", '
        '"comment": "The intended wording is unclear"}\n',
        encoding="utf-8",
    )

    entries = mdd_taxa.parse_changefile(path)

    assert entries == [
        mdd_taxa.ChangefileEntry(
            1, "1234", "taxonomyNotes", "old", "new, text", "Fix typo #1"
        ),
        mdd_taxa.ChangefileEntry(
            2,
            "5678",
            "distributionNotes",
            "unclear",
            None,
            "The intended wording is unclear",
        ),
    ]


def test_parse_changefile_rejects_duplicate_target(tmp_path: Path) -> None:
    path = tmp_path / "changes.jsonl"
    path.write_text(
        '{"taxon_id": 1234, "column": "taxonomyNotes", "old_text": "old", '
        '"action": "replace", '
        '"new_text": "first"}\n'
        '{"taxon_id": 1234, "column": "taxonomyNotes", "old_text": "old", '
        '"action": "replace", '
        '"new_text": "second"}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate change"):
        mdd_taxa.parse_changefile(path)


def test_parse_changefile_requires_old_text(tmp_path: Path) -> None:
    path = tmp_path / "changes.jsonl"
    path.write_text(
        '{"taxon_id": 1234, "column": "taxonomyNotes", "action": "replace", '
        '"new_text": "new"}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required fields: old_text"):
        mdd_taxa.parse_changefile(path)


def test_parse_changefile_rejects_new_text_for_manual_review(tmp_path: Path) -> None:
    path = tmp_path / "changes.jsonl"
    path.write_text(
        '{"taxon_id": 1234, "column": "taxonomyNotes", '
        '"old_text": "old", "action": "manual_review", '
        '"new_text": "ambiguous"}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must not include new_text"):
        mdd_taxa.parse_changefile(path)


def test_apply_changefile_shows_comment_and_applies_confirmed_change(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "old text",
                    "distributionNotes": "unclear text",
                },
            ),
        )
    ]
    entries = [
        mdd_taxa.ChangefileEntry(
            1, "1234", "taxonomyNotes", "old text", "new text", "Correct a typo"
        ),
        mdd_taxa.ChangefileEntry(
            2,
            "1234",
            "distributionNotes",
            "unclear text",
            None,
            "Check the cited source before editing",
        ),
    ]
    choices = iter(["apply", "enter new text"])
    monkeypatch.setattr(
        getinput, "choose_one_by_name", lambda *args, **kwargs: next(choices)
    )
    monkeypatch.setattr(
        getinput, "get_line", lambda *args, **kwargs: "reviewed distribution text"
    )
    diffs: list[tuple[str, str]] = []
    monkeypatch.setattr(
        getinput, "diff_strings", lambda old, new: diffs.append((old, new))
    )
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )

    updates = mdd_taxa.apply_changefile(
        entries,
        species,
        ["id", "sciName", "taxonomyNotes", "distributionNotes"],
        worksheet,
        dry_run=False,
    )

    assert len(updates) == 2
    assert (updates[0].row, updates[0].col, updates[0].value) == (7, 3, "new text")
    assert (updates[1].row, updates[1].col, updates[1].value) == (
        7,
        4,
        "reviewed distribution text",
    )
    assert updated_batches == [[updates[0]], [updates[1]]]
    assert diffs == [("old text", "new text")]
    output = capsys.readouterr().out
    assert "Comment:\nCorrect a typo" in output
    assert "Old text:\nold text" in output
    assert "New text:\nnew text" in output
    assert "Check the cited source before editing" in output
    assert "[manual review required; no suggested replacement]" in output
    assert "Character diff (^ replace, - delete, + insert):" in output
    assert "2 changes applied (1 manually edited), 0 skipped" in output


def test_apply_changefile_allows_editing_a_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "old text",
                },
            ),
        )
    ]
    monkeypatch.setattr(getinput, "choose_one_by_name", lambda *args, **kwargs: "edit")

    def edit_suggestion(*args: object, **kwargs: object) -> str:
        assert kwargs["default"] == "suggested text"
        return "suggested text plus another correction"

    monkeypatch.setattr(getinput, "get_line", edit_suggestion)
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )

    updates = mdd_taxa.apply_changefile(
        [
            mdd_taxa.ChangefileEntry(
                1, "1234", "taxonomyNotes", "old text", "suggested text"
            )
        ],
        species,
        ["id", "sciName", "taxonomyNotes"],
        worksheet,
        dry_run=False,
    )

    assert len(updates) == 1
    assert updates[0].value == "suggested text plus another correction"
    assert updated_batches == [updates]


def test_apply_changefile_writes_each_change_before_next_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "old taxonomy text",
                    "distributionNotes": "old distribution text",
                },
            ),
        )
    ]
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )
    prompt_count = 0

    def choose_change(*args: object, **kwargs: object) -> str:
        nonlocal prompt_count
        prompt_count += 1
        if prompt_count == 1:
            return "apply"
        assert len(updated_batches) == 1
        first_cell = updated_batches[0][0]
        assert isinstance(first_cell, gspread.cell.Cell)
        assert first_cell.value == "new taxonomy text"
        raise KeyboardInterrupt

    monkeypatch.setattr(getinput, "choose_one_by_name", choose_change)

    with pytest.raises(KeyboardInterrupt):
        mdd_taxa.apply_changefile(
            [
                mdd_taxa.ChangefileEntry(
                    1, "1234", "taxonomyNotes", "old taxonomy text", "new taxonomy text"
                ),
                mdd_taxa.ChangefileEntry(
                    2,
                    "1234",
                    "distributionNotes",
                    "old distribution text",
                    "new distribution text",
                ),
            ],
            species,
            ["id", "sciName", "taxonomyNotes", "distributionNotes"],
            worksheet,
            dry_run=False,
        )

    assert len(updated_batches) == 1


def test_apply_changefile_requires_manual_edit_for_stale_suggestion(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "intervening live text",
                },
            ),
        )
    ]

    def choose_manual(options: list[str], **kwargs: object) -> str:
        assert options == ["enter new text", "skip", "quit"]
        return "enter new text"

    monkeypatch.setattr(getinput, "choose_one_by_name", choose_manual)

    def edit_live_text(*args: object, **kwargs: object) -> str:
        assert kwargs["default"] == "intervening live text"
        return "manually reconciled text"

    monkeypatch.setattr(getinput, "get_line", edit_live_text)
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )

    updates = mdd_taxa.apply_changefile(
        [
            mdd_taxa.ChangefileEntry(
                1, "1234", "taxonomyNotes", "expected old text", "suggested text"
            )
        ],
        species,
        ["id", "sciName", "taxonomyNotes"],
        worksheet,
        dry_run=False,
    )

    assert len(updates) == 1
    assert updates[0].value == "manually reconciled text"
    assert updated_batches == [updates]
    output = capsys.readouterr().out
    assert "Expected old text:\nexpected old text" in output
    assert "Current live text:\nintervening live text" in output
    assert "stored suggestion cannot be applied directly" in output
    assert "1 stale" in output


def test_apply_changefile_drops_already_applied_suggestion(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "new text",
                },
            ),
        )
    ]

    def fail_if_called(*args: object, **kwargs: object) -> None:
        raise AssertionError("an already-applied suggestion must be dropped")

    monkeypatch.setattr(getinput, "choose_one_by_name", fail_if_called)
    monkeypatch.setattr(getinput, "get_line", fail_if_called)
    monkeypatch.setattr(getinput, "diff_strings", fail_if_called)
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )

    updates = mdd_taxa.apply_changefile(
        [mdd_taxa.ChangefileEntry(1, "1234", "taxonomyNotes", "old text", "new text")],
        species,
        ["id", "sciName", "taxonomyNotes"],
        worksheet,
        dry_run=False,
    )

    assert updates == []
    assert updated_batches == []
    output = capsys.readouterr().out
    assert "1 already applied" in output
    assert "Example_species" not in output


def test_apply_changefile_can_skip_suggested_and_manual_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            7,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "old taxonomy text",
                    "distributionNotes": "old distribution text",
                },
            ),
        )
    ]
    choices = iter(["skip", "skip"])
    monkeypatch.setattr(
        getinput, "choose_one_by_name", lambda *args, **kwargs: next(choices)
    )

    def fail_if_text_requested(*args: object, **kwargs: object) -> None:
        raise AssertionError("manual text must not be requested after skip")

    monkeypatch.setattr(getinput, "get_line", fail_if_text_requested)
    updated_batches: list[list[object]] = []
    worksheet = SimpleNamespace(
        update_cells=lambda cells: updated_batches.append(list(cells))
    )

    updates = mdd_taxa.apply_changefile(
        [
            mdd_taxa.ChangefileEntry(
                1,
                "1234",
                "taxonomyNotes",
                "old taxonomy text",
                "suggested taxonomy text",
            ),
            mdd_taxa.ChangefileEntry(
                2,
                "1234",
                "distributionNotes",
                "old distribution text",
                None,
                "Needs source review",
            ),
        ],
        species,
        ["id", "sciName", "taxonomyNotes", "distributionNotes"],
        worksheet,
        dry_run=False,
    )

    assert updates == []
    assert updated_batches == []


def test_apply_changefile_dry_run_does_not_require_worksheet(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    species = [
        mdd_taxa.MDDSpecies(
            2,
            cast(
                mdd_taxa.MDDSpeciesRow,
                {
                    "id": "1234",
                    "sciName": "Example_species",
                    "taxonomyNotes": "old text",
                },
            ),
        )
    ]

    def fail_if_prompted(*args: object, **kwargs: object) -> None:
        raise AssertionError("dry run must not prompt")

    monkeypatch.setattr(getinput, "choose_one_by_name", fail_if_prompted)
    monkeypatch.setattr(getinput, "get_line", fail_if_prompted)

    updates = mdd_taxa.apply_changefile(
        [
            mdd_taxa.ChangefileEntry(
                1, "1234", "taxonomyNotes", "old text", "new text"
            ),
            mdd_taxa.ChangefileEntry(
                2, "1234", "taxonomyNotes", "old text", None, "Needs source review"
            ),
        ],
        species,
        ["id", "sciName", "taxonomyNotes"],
        None,
        dry_run=True,
    )

    assert updates == []
    output = capsys.readouterr().out
    assert "Old text:\nold text" in output
    assert "New text:\nnew text" in output
    assert "Needs source review" in output
    assert "[manual review required; no suggested replacement]" in output
    assert "printed 1 applicable suggested changes, 1 manual reviews" in output
    assert "no changes applied" in output
