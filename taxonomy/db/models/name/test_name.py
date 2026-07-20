from collections.abc import Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call

import pytest

import taxonomy
from taxonomy.apis import zoobank
from taxonomy.db.constants import NomenclatureStatus, Status
from taxonomy.db.models.name import NameTag, TypeTag
from taxonomy.db.models.name.name import Name


def _get_tags(tags: tuple[object, ...], tag_cls: type[object]) -> Iterable[object]:
    return (tag for tag in tags if isinstance(tag, tag_cls))


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
    "IgnorePotentialCitationFrom",
    "InterpretedTypeLocality",
    "InterpretedTypeSpecimen",
    "InterpretedTypeTaxon",
    "NomenclatureComments",
    # Fields
    "type_tags",
    "tags",
    "target",
}


def test_docs() -> None:
    assert taxonomy.__file__ is not None
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
        field
        for field in Name.clirm_fields
        if _get_expected(field) not in name_docs and field not in EXCLUDED
    }
    assert (
        not missing_fields
    ), f"Missing documentation for Name fields: {missing_fields}"


def test_clear_zoobank_caches_is_adt_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Name, "original_citation", None)
    name = object.__new__(Name)

    callbacks = name.get_adt_callbacks()

    assert callbacks["clear_zoobank_caches"] == name.clear_zoobank_caches


def test_clear_zoobank_caches_clears_name_and_act_lsid_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_cache = Mock()
    monkeypatch.setattr(zoobank, "clear_zoobank_act_cache", clear_cache)
    name = cast(
        Name,
        SimpleNamespace(
            corrected_original_name="Pseudovespertiliavus parva",
            type_tags=(
                TypeTag.LSIDName(
                    "urn:lsid:zoobank.org:act:fc07acbe-03f7-414a-bb64-1bb0711766bf"
                ),
            ),
            get_tags=_get_tags,
        ),
    )

    Name.clear_zoobank_caches(name)

    assert clear_cache.call_count == 4
    clear_cache.assert_has_calls(
        [
            call("Pseudovespertiliavus_parva"),
            call("urn:lsid:zoobank.org:act:fc07acbe-03f7-414a-bb64-1bb0711766bf"),
            call("FC07ACBE-03F7-414A-BB64-1BB0711766BF"),
            call("fc07acbe-03f7-414a-bb64-1bb0711766bf"),
        ],
        any_order=True,
    )
