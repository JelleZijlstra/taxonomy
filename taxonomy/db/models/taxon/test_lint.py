from taxonomy.db.constants import Group, Rank, Status
from taxonomy.db.models.name import Name

from .lint import _switch_basename_issue
from .taxon import Taxon


def test_switch_basename_issue_updates_name_cycle_and_valid_name() -> None:
    taxon = Taxon.virtual(rank=Rank.genus, valid_name="Old")
    old_base = Name.virtual(
        root_name="Old", group=Group.genus, status=Status.valid, taxon=taxon
    )
    new_base = Name.virtual(
        root_name="New", group=Group.genus, status=Status.synonym, taxon=taxon
    )
    taxon.base_name = old_base

    issue = _switch_basename_issue("switch base name", taxon, new_base)

    assert issue.fix is not None
    assert issue.fix.is_virtual_safe is True
    assert issue.fix.apply() is True
    assert taxon.base_name is new_base
    assert taxon.valid_name == "New"
    assert old_base.status is Status.synonym
    assert new_base.status is Status.valid
    assert issue.fix.apply() is False


def test_switch_shared_basename_changes_only_taxon_reference() -> None:
    owner = Taxon.virtual(rank=Rank.genus, valid_name="Shared")
    taxon = Taxon.virtual(rank=Rank.genus, valid_name="Shared")
    old_base = Name.virtual(
        root_name="Shared", group=Group.genus, status=Status.valid, taxon=owner
    )
    new_base = Name.virtual(
        root_name="Replacement", group=Group.genus, status=Status.synonym, taxon=owner
    )
    taxon.base_name = old_base

    issue = _switch_basename_issue("switch shared base name", taxon, new_base)

    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert taxon.base_name is new_base
    assert taxon.valid_name == "Shared"
    assert old_base.status is Status.valid
    assert new_base.status is Status.synonym
