from taxonomy.db.models.period import Period
from taxonomy.db.models.stratigraphic_unit import StratigraphicUnit


def test_period_redirect_uses_verified_merge_parent() -> None:
    target = Period.virtual(name="target")
    source = Period.virtual(
        deleted=True, parent=target, comment=f"Merged into target (P#{target.id})"
    )

    assert Period.get_redirect_target(source) is target


def test_period_redirect_rejects_unverified_parent() -> None:
    target = Period.virtual(name="target")
    source = Period.virtual(
        deleted=True, parent=target, comment="Merged into another target (P#3)"
    )

    assert Period.get_redirect_target(source) is None


def test_period_merge_creates_redirect() -> None:
    target = Period.virtual(name="target", deleted=False)
    source = Period.virtual(name="source", comment="", parent=None, deleted=False)

    Period.merge(source, target)

    assert source.deleted
    assert source.parent is target
    assert Period.get_redirect_target(source) is target


def test_stratigraphic_unit_redirect_uses_verified_merge_parent() -> None:
    target = StratigraphicUnit.virtual(name="target")
    source = StratigraphicUnit.virtual(
        deleted=True, parent=target, comment=f"Merged into target (P#{target.id})"
    )

    assert StratigraphicUnit.get_redirect_target(source) is target


def test_stratigraphic_unit_redirect_rejects_unverified_parent() -> None:
    target = StratigraphicUnit.virtual(name="target")
    source = StratigraphicUnit.virtual(
        deleted=True, parent=target, comment="Merged into another target (P#3)"
    )

    assert StratigraphicUnit.get_redirect_target(source) is None


def test_stratigraphic_unit_merge_creates_redirect() -> None:
    target = StratigraphicUnit.virtual(name="target", deleted=False)
    source = StratigraphicUnit.virtual(
        name="source", comment="", parent=None, deleted=False
    )

    StratigraphicUnit.merge(source, target)

    assert source.deleted
    assert source.parent is target
    assert StratigraphicUnit.get_redirect_target(source) is target
