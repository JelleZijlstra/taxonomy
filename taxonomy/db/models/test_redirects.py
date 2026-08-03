from types import SimpleNamespace
from typing import cast

from taxonomy.db.models.period import Period
from taxonomy.db.models.stratigraphic_unit import StratigraphicUnit


def test_period_redirect_uses_verified_merge_parent() -> None:
    target = cast(Period, SimpleNamespace(id=2))
    source = cast(
        Period,
        SimpleNamespace(
            deleted=True, parent=target, comment="Merged into target (P#2)"
        ),
    )

    assert Period.get_redirect_target(source) is target


def test_period_redirect_rejects_unverified_parent() -> None:
    target = cast(Period, SimpleNamespace(id=2))
    source = cast(
        Period,
        SimpleNamespace(
            deleted=True, parent=target, comment="Merged into another target (P#3)"
        ),
    )

    assert Period.get_redirect_target(source) is None


def test_period_merge_creates_redirect() -> None:
    target = cast(Period, SimpleNamespace(id=2, is_invalid=lambda: False))
    source = cast(
        Period,
        SimpleNamespace(
            id=1,
            locations_min=(),
            locations_max=(),
            comment="",
            parent=None,
            deleted=False,
        ),
    )

    Period.merge(source, target)

    assert source.deleted
    assert source.parent is target
    assert Period.get_redirect_target(source) is target


def test_stratigraphic_unit_redirect_uses_verified_merge_parent() -> None:
    target = cast(StratigraphicUnit, SimpleNamespace(id=2))
    source = cast(
        StratigraphicUnit,
        SimpleNamespace(
            deleted=True, parent=target, comment="Merged into target (P#2)"
        ),
    )

    assert StratigraphicUnit.get_redirect_target(source) is target


def test_stratigraphic_unit_redirect_rejects_unverified_parent() -> None:
    target = cast(StratigraphicUnit, SimpleNamespace(id=2))
    source = cast(
        StratigraphicUnit,
        SimpleNamespace(
            deleted=True, parent=target, comment="Merged into another target (P#3)"
        ),
    )

    assert StratigraphicUnit.get_redirect_target(source) is None


def test_stratigraphic_unit_merge_creates_redirect() -> None:
    target = cast(StratigraphicUnit, SimpleNamespace(id=2, is_invalid=lambda: False))
    source = cast(
        StratigraphicUnit,
        SimpleNamespace(id=1, locations=(), comment="", parent=None, deleted=False),
    )

    StratigraphicUnit.merge(source, target)

    assert source.deleted
    assert source.parent is target
    assert StratigraphicUnit.get_redirect_target(source) is target
