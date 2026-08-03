from dataclasses import dataclass, field
from typing import Any, ClassVar, cast

import pytest

from taxonomy.db.models.lint import Lint


@dataclass(frozen=True)
class FakeIgnore:
    label: str
    comment: str


@dataclass
class FakeObject:
    id: int
    needs_ignore: bool
    tags: list[FakeIgnore] = field(default_factory=list)
    raises: bool = False

    def __repr__(self) -> str:
        return f"FakeObject({self.id})"


class FakeModel:
    objects: ClassVar[list[FakeObject]] = []

    @classmethod
    def select_valid(cls) -> list[FakeObject]:
        return cls.objects


def make_lint(objects: list[FakeObject]) -> Lint[Any]:
    FakeModel.objects = objects

    def get_ignores(obj: FakeObject) -> list[FakeIgnore]:
        return obj.tags

    def remove_unused_ignores(obj: FakeObject, unused: set[str]) -> None:
        obj.tags = [tag for tag in obj.tags if tag.label not in unused]

    def add_ignore(obj: FakeObject, label: str, comment: str) -> None:
        obj.tags.append(FakeIgnore(label, comment))

    lint: Lint[Any] = Lint(
        cast(Any, FakeModel),
        cast(Any, get_ignores),
        cast(Any, remove_unused_ignores),
        cast(Any, add_ignore),
    )

    @lint.add("problem")
    def check_problem(obj: FakeObject, _cfg: Any) -> list[str]:
        if obj.raises:
            raise ValueError("broken linter")
        if obj.needs_ignore:
            return ["has problem"]
        return []

    return lint


def test_bulk_ignore_dry_run_does_not_mutate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    needs_tag = FakeObject(1, needs_ignore=True)
    clean = FakeObject(2, needs_ignore=False)
    already_ignored = FakeObject(
        3, needs_ignore=True, tags=[FakeIgnore("problem", "earlier review")]
    )
    lint = make_lint([needs_tag, clean, already_ignored])

    plan = lint.add_ignore_lint_to_all("problem", "review later")

    assert [target.obj for target in plan.targets] == [needs_tag]
    assert [target.obj for target in plan.already_ignored] == [already_ignored]
    assert needs_tag.tags == []
    output = capsys.readouterr().out
    assert "WOULD_ADD_IGNORE_LINT object=FakeObject(1)" in output
    assert "Dry run: 1 IgnoreLint tag(s) to add, 1 already present." in output
    assert "No database changes made" in output


def test_bulk_ignore_apply_is_idempotent() -> None:
    obj = FakeObject(1, needs_ignore=True)
    lint = make_lint([obj])

    lint.add_ignore_lint_to_all("problem", "review later", dry_run=False)

    assert obj.tags == [FakeIgnore("problem", "review later")]
    second_plan = lint.add_ignore_lint_to_all("problem", "review later", dry_run=False)
    assert second_plan.targets == ()
    assert len(second_plan.already_ignored) == 1
    assert obj.tags == [FakeIgnore("problem", "review later")]


def test_bulk_ignore_validates_code_and_comment() -> None:
    lint = make_lint([])

    with pytest.raises(ValueError, match="unknown FakeModel lint 'other'"):
        lint.add_ignore_lint_to_all("other", "review later")
    with pytest.raises(ValueError, match="comment is required"):
        lint.add_ignore_lint_to_all("problem", "  ")


def test_bulk_ignore_finishes_scan_before_writing() -> None:
    first = FakeObject(1, needs_ignore=True)
    second = FakeObject(2, needs_ignore=True, raises=True)
    lint = make_lint([first, second])

    with pytest.raises(RuntimeError, match="broken linter"):
        lint.add_ignore_lint_to_all("problem", "review later", dry_run=False)

    assert first.tags == []
