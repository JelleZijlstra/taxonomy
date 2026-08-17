from taxonomy.db.constants import Group, Rank, Status
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.name import Name

from .lint import _known_acyclic_taxa, _switch_basename_issue, check_parent_cycle
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


def test_parent_cycle_reuses_known_acyclic_path() -> None:
    _known_acyclic_taxa.clear()
    root = Taxon.virtual(rank=Rank.order, valid_name="Root")
    parent = Taxon.virtual(rank=Rank.family, valid_name="Parent", parent=root)
    child = Taxon.virtual(rank=Rank.genus, valid_name="Child", parent=parent)

    assert list(check_parent_cycle(parent, LintConfig())) == []
    assert list(check_parent_cycle(child, LintConfig())) == []
    assert {root, parent, child} <= _known_acyclic_taxa


def test_parent_cycle_still_reports_cycle() -> None:
    _known_acyclic_taxa.clear()
    first = Taxon.virtual(rank=Rank.family, valid_name="First")
    second = Taxon.virtual(rank=Rank.genus, valid_name="Second", parent=first)
    first.parent = second

    messages = list(check_parent_cycle(first, LintConfig()))

    assert len(messages) == 1
    assert "parent cycle detected" in str(messages[0])
    assert not _known_acyclic_taxa
