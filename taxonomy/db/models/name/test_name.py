from pathlib import Path

import taxonomy
from taxonomy.db.constants import NomenclatureStatus, Status
from taxonomy.db.models.name import NameTag, TypeTag
from taxonomy.db.models.name.name import Name


def _get_expected(value: str) -> str:
    return f"- _{value.replace('_', ' ')}_:"


EXCLUDED = {
    # NameTag
    "MappedClassificationEntry",
    "HMW",
    # TypeTag
    "ImpreciseLocality",
    "TypeLocality",
    "TypeSpecimenLink",
    "_RawCollector",
    "StratigraphyDetail",
    "NoOriginalParent",
    "Habitat",
    "IgnoreLintName",
    "NoAge",
    "DifferentAuthority",
    "NoLocation",
    "NoOrgan",
    "RejectedLSIDName",
    "NoCollector",
    "NoGender",
    "IncorrectGrammar",
    "NoEtymology",
    "NoSpecimen",
    "NoDate",
    # Fields
    "type_tags",
    "tags",
    "target",
    "_definition",
}


def test_docs() -> None:
    docs_root = Path(taxonomy.__file__).parent.parent / "docs"
    name_docs = (docs_root / "name.md").read_text()

    missing_statuses = {
        status for status in Status if _get_expected(status.name) not in name_docs
    }
    assert (
        not missing_statuses
    ), f"Missing documentation for Status values: {missing_statuses}"

    missing_nomenclature_statuses = {
        status
        for status in NomenclatureStatus
        if _get_expected(status.name) not in name_docs
    }
    assert (
        not missing_nomenclature_statuses
    ), f"Missing documentation for NomenclatureStatus values: {missing_nomenclature_statuses}"

    for tag_cls in (NameTag, TypeTag):
        missing_tags = {
            tag
            for tag in tag_cls._members
            if tag not in EXCLUDED and _get_expected(tag) not in name_docs
        }
        assert (
            not missing_tags
        ), f"Missing documentation for {tag_cls.__name__} values: {missing_tags}"

    missing_fields = {
        field for field in Name.clirm_fields if _get_expected(field) not in name_docs
    }
    assert (
        not missing_fields
    ), f"Missing documentation for Name fields: {missing_fields}"
