import pytest

from taxonomy.applicator import generic
from taxonomy.applicator.classification import (
    ClassificationBundleBuilder,
    ClassificationDraftError,
    DraftEvidence,
)
from taxonomy.db.constants import OccurrenceBasis, Rank
from taxonomy.db.models import Article, ClassificationEntry
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag


def test_bundle_builder_lowers_entries_occurrences_and_coverage() -> None:
    bundle = ClassificationBundleBuilder()
    evidence = [DraftEvidence("source_page", "PDF page 12")]
    with bundle.article(
        "solem", article_ref="article:solem", evidence=evidence
    ) as source:
        family = source.add_entry("Endodontidae", Rank.family, page="12")
        genus = source.add_entry("Endodonta", Rank.genus, page="40", parent=family)
        species = source.add_entry(
            "Endodonta christenseni", Rank.species, page="44", parent=genus
        )
        species.add_occurrence(
            "Rurutu",
            OccurrenceBasis.listing,
            location={"model": "Location", "ref": "location:rurutu"},
        )
    bundle.assert_coverage(
        "solem",
        [species],
        parent=genus,
        reason="The account is complete.",
        evidence=evidence,
    )

    rows = bundle.to_rows()

    assert [row["action"] for row in rows] == [
        "create_object",
        "create_object",
        "create_object",
        "create_object",
        "check_coverage",
    ]
    assert rows[2]["values"]["parent"]["ref"] == genus.ref
    assert rows[3]["values"]["classification_entry"]["ref"] == species.ref
    assert "mapped_name" not in rows[2]["values"]["raw_data"]


def test_bundle_builder_rejects_page_ranges() -> None:
    bundle = ClassificationBundleBuilder()
    with bundle.article(
        "source", evidence=[DraftEvidence("source_page", "PDF page 12")]
    ) as source:
        source.add_entry("Endodontidae", Rank.family, page="12-13")

    with pytest.raises(ClassificationDraftError, match="one page number"):
        bundle.to_rows()


def test_bundle_builder_references_existing_article() -> None:
    bundle = ClassificationBundleBuilder()
    with bundle.article(
        "revision",
        article_id=37697,
        article_label="Nesorohyla nov.pdf",
        evidence=[DraftEvidence("source_page", "Genus account on page 231")],
    ) as source:
        source.add_entry("Nesorohyla", Rank.genus, page="231")

    (row,) = bundle.to_rows()
    assert row["values"]["article"] == {
        "model": "Article",
        "id": 37697,
        "label": "Nesorohyla nov.pdf",
    }


def test_generated_entries_plan_with_manifest_article_and_parent_refs() -> None:
    bundle = ClassificationBundleBuilder()
    with bundle.article(
        "source",
        article_ref="article:source",
        evidence=[DraftEvidence("source_page", "PDF page 12")],
    ) as source:
        family = source.add_entry("Endodontidae", Rank.family, page="12")
        source.add_entry("Endodonta", Rank.genus, page="40", parent=family)
    parsed = [
        generic.parse_recommendation(row, index)
        for index, row in enumerate(bundle.to_rows(), start=1)
    ]
    article = Article.virtual(name="Solem.pdf")

    plan = generic.build_plan(
        parsed,
        model_registry={"ClassificationEntry": ClassificationEntry},
        initial_references={"article:source": article},
        find_objects_by_match=lambda _model, _match: [],
    )

    family_action, genus_action = plan.actions
    assert family_action.new_value["article"] is article
    assert genus_action.new_value["parent"] is family_action.object


def test_builder_lowers_materialization_and_source_quotes() -> None:
    bundle = ClassificationBundleBuilder()
    with bundle.article(
        "monograph",
        evidence=[DraftEvidence("source_page", "Family account on page 12")],
    ) as source:
        source.add_entry(
            "Endodontidae",
            Rank.family,
            page="12",
            materialize=True,
            materialize_parent_taxon_id=123,
            type_locality_quote="Valley below the inward-facing cliffs.",
            type_specimen_data_quote="Holotype: USNM 123, adult male.",
            etymology_detail_quote="Named from the inward-facing teeth.",
            original_citation=True,
        )

    (row,) = bundle.to_rows()
    assert row["values"]["type_locality"] == "Valley below the inward-facing cliffs."
    assert row["values"]["tags"] == [
        {
            "tag": "TypeSpecimenData",
            "arguments": {"text": "Holotype: USNM 123, adult male."},
        },
        {"tag": "Materialize", "arguments": {"parent_taxon_id": 123}},
        {
            "tag": "EtymologyDetail",
            "arguments": {"text": "Named from the inward-facing teeth."},
        },
        {"tag": "OriginalCitation", "arguments": {}},
    ]
    recommendation = generic.parse_recommendation(row, 1)
    article = Article.virtual(name="Monograph.pdf")
    plan = generic.build_plan(
        [recommendation],
        model_registry={"ClassificationEntry": ClassificationEntry},
        initial_references={"monograph": article},
        find_objects_by_match=lambda _model, _match: [],
    )
    tags = plan.actions[0].new_value["tags"]
    assert (
        ClassificationEntryTag.TypeSpecimenData("Holotype: USNM 123, adult male.")
        in tags
    )
    assert ClassificationEntryTag.Materialize(parent_taxon_id=123) in tags
    assert (
        ClassificationEntryTag.EtymologyDetail("Named from the inward-facing teeth.")
        in tags
    )
    assert ClassificationEntryTag.OriginalCitation in tags


def test_builder_rejects_materialization_parent_without_instruction() -> None:
    bundle = ClassificationBundleBuilder()
    with (
        bundle.article(
            "monograph",
            evidence=[DraftEvidence("source_page", "Family account on page 12")],
        ) as source,
        pytest.raises(
            ClassificationDraftError,
            match="materialize_parent_taxon_id requires materialize=True",
        ),
    ):
        source.add_entry(
            "Endodontidae", Rank.family, page="12", materialize_parent_taxon_id=123
        )


def test_builder_lowers_materialize_base_name() -> None:
    bundle = ClassificationBundleBuilder()
    with bundle.article(
        "revision",
        article_ref="article:revision",
        evidence=[DraftEvidence("source_page", "Genus account on page 231")],
    ) as source:
        source.add_entry(
            "Nesorohyla kanaima",
            Rank.species,
            page="231",
            materialize=True,
            materialize_parent_taxon_id=123,
            materialize_base_name={
                "original_name": "Hyla kanaima",
                "page": "135",
                "verbatim_citation": (
                    "Goin, C. J. & Woodley, J. D. 1969. A new tree-frog from Guyana. Zoological Journal of the Linnean Society 48: 135–140."
                ),
                "citation_group": {
                    "model": "CitationGroup",
                    "id": 432,
                    "label": "Zoological Journal of the Linnean Society",
                },
                "authors": [
                    {"model": "Person", "id": 51902, "label": "Goin"},
                    {"model": "Person", "ref": "person:woodley", "label": "Woodley"},
                ],
            },
        )

    (row,) = bundle.to_rows()
    assert row["values"]["tags"][-3:] == [
        {
            "tag": "MaterializeBaseName",
            "arguments": {
                "original_name": "Hyla kanaima",
                "page": "135",
                "verbatim_citation": (
                    "Goin, C. J. & Woodley, J. D. 1969. A new tree-frog from Guyana. Zoological Journal of the Linnean Society 48: 135–140."
                ),
                "citation_group": {
                    "model": "CitationGroup",
                    "id": 432,
                    "label": "Zoological Journal of the Linnean Society",
                },
            },
        },
        {
            "tag": "MaterializeBaseNameAuthor",
            "arguments": {
                "person": {"model": "Person", "id": 51902, "label": "Goin"},
                "order": 1,
            },
        },
        {
            "tag": "MaterializeBaseNameAuthor",
            "arguments": {
                "person": {
                    "model": "Person",
                    "ref": "person:woodley",
                    "label": "Woodley",
                },
                "order": 2,
            },
        },
    ]


def test_builder_lowers_cross_source_materialize_parent() -> None:
    bundle = ClassificationBundleBuilder()
    evidence = [DraftEvidence("source_page", "Exact taxonomic account.")]
    with bundle.article("revision", evidence=evidence) as revision:
        genus = revision.add_entry(
            "Atlantihyla",
            Rank.genus,
            page="24",
            materialize=True,
            materialize_parent_taxon_id=54078,
        )
    with bundle.article("later-species", evidence=evidence) as later:
        later.add_entry(
            "Atlantihyla melissa",
            Rank.species,
            page="739",
            materialize=True,
            materialize_parent=genus,
        )

    genus_row, species_row = bundle.to_rows()
    assert genus_row["values"]["parent"] is None
    assert species_row["values"]["parent"] is None
    assert species_row["values"]["tags"][-1] == {
        "tag": "MaterializeParent",
        "arguments": {
            "ce": {
                "model": "ClassificationEntry",
                "ref": genus.ref,
                "label": "Atlantihyla",
            }
        },
    }


def test_builder_rejects_materialize_parent_with_source_parent() -> None:
    bundle = ClassificationBundleBuilder()
    evidence = [DraftEvidence("source_page", "Exact taxonomic account.")]
    with bundle.article("revision", evidence=evidence) as revision:
        genus = revision.add_entry(
            "Atlantihyla", Rank.genus, page="1", materialize=True
        )
        with pytest.raises(
            ClassificationDraftError,
            match="cannot be combined with a source-local parent",
        ):
            revision.add_entry(
                "Atlantihyla melissa",
                Rank.species,
                page="2",
                parent=genus,
                materialize=True,
                materialize_parent=genus,
            )


def test_builder_rejects_materialize_parent_from_another_bundle() -> None:
    first = ClassificationBundleBuilder()
    evidence = [DraftEvidence("source_page", "Exact taxonomic account.")]
    with first.article("revision", evidence=evidence) as revision:
        genus = revision.add_entry(
            "Atlantihyla", Rank.genus, page="1", materialize=True
        )
    second = ClassificationBundleBuilder()
    with (
        second.article("description", evidence=evidence) as description,
        pytest.raises(
            ClassificationDraftError, match="must reference a CE in the same bundle"
        ),
    ):
        description.add_entry(
            "Atlantihyla melissa",
            Rank.species,
            page="2",
            materialize=True,
            materialize_parent=genus,
        )
