import pytest

from taxonomy.applicator import taxon as recommendations
from taxonomy.db.constants import AgeClass, Rank
from taxonomy.db.models import Name, Taxon


def _row(ref: str, name: str, rank: str, parent_ref: str) -> dict[str, object]:
    return {
        "schema_version": 2,
        "action": "create_taxon",
        "taxon": {
            "ref": f"taxon:{ref}",
            "valid_name": name,
            "rank": rank,
            "age": "extant",
            "parent": {"model": "Taxon", "ref": parent_ref},
        },
        "base_name": {"ref": f"name:{ref}", "values": {}},
        "confidence": "high",
        "reason": "The reviewed classification supports the accepted taxon.",
        "evidence": [{"kind": "source", "text": "Classification page."}],
    }


def test_create_taxon_owns_cycle_and_orders_forward_parent() -> None:
    ancestor = Taxon.virtual(
        valid_name="Punctoidea", rank=Rank.superfamily, age=AgeClass.extant
    )
    child = recommendations.parse_recommendation(
        _row("endodonta", "Endodonta", "genus", "taxon:endodontidae"), 1
    )
    parent = recommendations.parse_recommendation(
        _row("endodontidae", "Endodontidae", "family", "taxon:punctoidea"), 2
    )
    plan = recommendations.build_plan(
        [child, parent],
        initial_references={"taxon:punctoidea": ancestor},
        find_taxa=lambda _name: [],
    )
    created_taxa: list[Taxon] = []
    created_names: list[Name] = []

    def create_taxon(**values: object) -> Taxon:
        obj = Taxon.virtual(**values)
        created_taxa.append(obj)
        return obj

    def create_name(**values: object) -> Name:
        obj = Name.virtual(**values)
        created_names.append(obj)
        return obj

    recommendations.execute_plan(
        plan, apply=True, create_taxon=create_taxon, create_name=create_name
    )

    assert [taxon.valid_name for taxon in created_taxa] == ["Endodontidae", "Endodonta"]
    assert created_taxa[1].parent is created_taxa[0]
    assert created_taxa[0].base_name is created_names[0]
    assert created_names[0].taxon is created_taxa[0]


def test_review_expands_taxon_and_base_name_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _row("endodonta", "Endodonta", "genus", "taxon:endodontidae")
    base_name = data["base_name"]
    assert isinstance(base_name, dict)
    base_name["values"] = {"year": "1896"}
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "    - taxon field: valid_name='Endodonta'" in output
    assert "    - taxon field: rank=genus" in output
    assert "    - taxon field: age=extant" in output
    assert "    - taxon field: parent=Taxon(ref='taxon:endodontidae')" in output
    assert "    - base name field: corrected_original_name='Endodonta'" in output
    assert "    - base name field: root_name='Endodonta'" in output
    assert "    - base name field: status='valid'" in output
    assert "    - base name field: nomenclature_status='available'" in output
    assert "    - base name field: year='1896'" in output
