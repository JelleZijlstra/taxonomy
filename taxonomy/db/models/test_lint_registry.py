from dataclasses import dataclass, field
from typing import Any, ClassVar, cast

import pytest

from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import Lint, count_lint_codes, field_issue
from taxonomy.db.models.lint_types import LintIssue


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
    is_virtual: bool = False

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


def test_linter_error_preserves_ignore() -> None:
    obj = FakeObject(
        1,
        needs_ignore=True,
        tags=[FakeIgnore("problem", "earlier review")],
        raises=True,
    )
    lint = make_lint([obj])

    messages = list(lint.run(obj, LintConfig(autofix=True, interactive=False)))

    assert [str(message) for message in messages] == [
        "FakeObject(1): error running problem linter: broken linter"
    ]
    assert obj.tags == [FakeIgnore("problem", "earlier review")]


def test_ignored_linter_does_not_receive_autofix() -> None:
    obj = FakeObject(
        1, needs_ignore=True, tags=[FakeIgnore("problem", "earlier review")]
    )
    lint = make_lint([obj])
    received_configs: list[LintConfig] = []
    lint.linters[0].linter = lambda _obj, cfg: received_configs.append(cfg) or [  # type: ignore[func-returns-value]
        "has problem"
    ]

    assert list(lint.run(obj, LintConfig(autofix=True, interactive=True))) == []

    assert received_configs == [LintConfig(autofix=False, interactive=False)]
    assert obj.tags == [FakeIgnore("problem", "earlier review")]


def test_linter_can_skip_virtual_objects_and_preserve_ignore() -> None:
    obj = FakeObject(
        1,
        needs_ignore=True,
        tags=[FakeIgnore("persisted_only", "requires database state")],
        is_virtual=True,
    )
    lint = make_lint([obj])
    calls: list[FakeObject] = []

    @lint.add("persisted_only", skip_virtual=True)
    def check_persisted_only(item: FakeObject, _cfg: Any) -> list[str]:
        calls.append(item)
        return ["has persisted-only problem"]

    messages = list(lint.run(obj, LintConfig(autofix=True, interactive=False)))

    assert [str(message) for message in messages] == [
        "FakeObject(1): has problem [problem]"
    ]
    assert calls == []
    assert obj.tags == [FakeIgnore("persisted_only", "requires database state")]


def test_decorator_adds_code_to_plain_lint_issue() -> None:
    obj = FakeObject(1, needs_ignore=True)
    lint = make_lint([obj])

    (message,) = lint.run(obj, LintConfig(autofix=False, interactive=False))

    assert isinstance(message, LintIssue)
    assert not isinstance(message, str)
    assert message.message == "FakeObject(1): has problem [problem]"
    assert message.code == "problem"
    assert str(message) == "FakeObject(1): has problem [problem]"
    assert count_lint_codes([message, "legacy issue"]) == {"problem": 1, "uncoded": 1}


def test_structured_autofix_only_changes_virtual_objects() -> None:
    virtual = FakeObject(1, needs_ignore=True, is_virtual=True)
    persisted = FakeObject(2, needs_ignore=True)
    lint = make_lint([virtual, persisted])
    lint.linters[0].linter = lambda obj, _cfg: [
        field_issue("clear problem", cast(Any, obj), "needs_ignore", new=False)
    ]
    applied: list[Any] = []
    cfg = LintConfig(
        autofix=False,
        structured_autofix=True,
        fix_callback=applied.append,
        interactive=False,
    )

    assert list(lint.run(virtual, cfg)) == []
    assert virtual.needs_ignore is False
    assert len(applied) == 1
    assert applied[0].code == "problem"

    (unresolved,) = lint.run(persisted, cfg)
    assert persisted.needs_ignore is True
    assert isinstance(unresolved, LintIssue)
    assert unresolved.code == "problem"
