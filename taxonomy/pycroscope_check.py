"""Test that certain types are resolved correctly by pycroscope."""

from typing import assert_type

from taxonomy.db.constants import Group
from taxonomy.db.models.article import Article
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    DetailTag,
    DetailTagCons,
)
from taxonomy.db.models.name import Name, TypeTag
from taxonomy.db.models.name.name import SourceAbsenceTagCons, SourceTag, SourceTagCons


def test_fields(name: Name) -> None:
    assert_type(name.root_name, str)
    assert_type(name.group, Group)


def test_adts(ld: TypeTag.LocationDetail) -> None:
    assert_type(ld, TypeTag.LocationDetail)
    assert_type(ld.text, str)
    assert_type(ld.comment, str | None)


def test_detail_tag_types(
    source_tag: SourceTag,
    ce_tag: DetailTag,
    source_constructor: SourceTagCons,
    ce_constructor: DetailTagCons,
    absence_constructor: SourceAbsenceTagCons,
    source: Article,
) -> None:
    assert_type(source_tag.text, str)
    assert_type(source_tag.page, str | None)
    assert_type(source_tag.source, Article)
    assert_type(source_tag.classification_entry, ClassificationEntry | None)
    assert_type(ce_tag.text, str)
    assert_type(ce_tag.page_link, str | None)
    assert_type(source_constructor("quotation", source), SourceTag)
    assert_type(ce_constructor("quotation"), DetailTag)
    assert_type(absence_constructor(source=source).source, Article)
