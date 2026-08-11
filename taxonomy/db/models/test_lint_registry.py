import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import (
    Lint,
    LintFix,
    LintFixError,
    add_tag_issue,
    count_lint_codes,
    field_issue,
    remove_tag_issue,
    replace_tag_issue,
)
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
    reload_count: int = 0

    def __repr__(self) -> str:
        return f"FakeObject({self.id})"

    def reload(self) -> FakeObject:
        self.reload_count += 1
        return self


class FakeModel:
    objects: ClassVar[list[FakeObject]] = []

    @classmethod
    def select_valid(cls) -> list[FakeObject]:
        return cls.objects


@dataclass(frozen=True)
class FailingOperation:
    target: Any

    def validate(self) -> bool:
        return True

    def apply(self) -> None:
        raise ValueError("rejected change")


def make_lint(objects: list[FakeObject]) -> Lint[Any]:
    FakeModel.objects = objects

    def get_ignores(obj: FakeObject) -> list[FakeIgnore]:
        return obj.tags

    def add_ignore(obj: FakeObject, label: str, comment: str) -> None:
        obj.tags.append(FakeIgnore(label, comment))

    lint: Lint[Any] = Lint(
        cast(Any, FakeModel), cast(Any, get_ignores), cast(Any, add_ignore)
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


def test_autofix_error_becomes_non_autofixable_issue() -> None:
    obj = FakeObject(1, needs_ignore=True)
    lint = make_lint([obj])
    lint.linters[0].linter = lambda item, _cfg: [
        LintIssue(
            "change rejected field", fix=LintFix((FailingOperation(cast(Any, item)),))
        )
    ]

    (issue,) = lint.run(obj, LintConfig(autofix=True, interactive=False))

    assert isinstance(issue, LintIssue)
    assert issue.fix is None
    assert issue.code == "problem"
    assert "autofix failed with ValueError: rejected change" in issue.message
    assert obj.reload_count == 1


def test_unused_ignore_is_removed_with_structured_tag_fix() -> None:
    unused = FakeIgnore("unused", "old suppression")
    obj = FakeObject(1, needs_ignore=False, tags=[unused])
    lint = make_lint([obj])
    applied: list[LintIssue] = []

    assert (
        list(
            lint.run(
                obj,
                LintConfig(
                    autofix=True, interactive=False, fix_callback=applied.append
                ),
            )
        )
        == []
    )

    assert tuple(obj.tags) == ()
    assert len(applied) == 1
    assert applied[0].code == "unused_ignore"


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


def _apply_issue(issue: LintIssue) -> bool:
    assert issue.fix is not None
    return issue.fix.apply()


def test_tag_operations_preserve_intervening_unrelated_edits() -> None:
    first = FakeIgnore("first", "")
    second = FakeIgnore("second", "")
    added = FakeIgnore("added", "")
    external = FakeIgnore("external", "")
    obj = FakeObject(1, needs_ignore=False, tags=[first, second])
    add_issue = add_tag_issue("add tag", cast(Any, obj), added)
    remove_issue = remove_tag_issue("remove tag", cast(Any, obj), first)

    obj.tags.append(external)

    assert _apply_issue(add_issue) is True
    assert _apply_issue(remove_issue) is True
    assert tuple(obj.tags) == (second, external, added)


def test_tag_operations_are_idempotent() -> None:
    tag = FakeIgnore("tag", "")
    obj = FakeObject(1, needs_ignore=False)
    add_issue = add_tag_issue("add tag", cast(Any, obj), tag)
    remove_issue = remove_tag_issue("remove tag", cast(Any, obj), tag)

    assert _apply_issue(add_issue) is True
    assert _apply_issue(add_issue) is False
    assert _apply_issue(remove_issue) is True
    assert _apply_issue(remove_issue) is False
    assert tuple(obj.tags) == ()


def test_replace_tag_preserves_other_tags_and_removes_duplicates() -> None:
    old = FakeIgnore("old", "")
    new = FakeIgnore("new", "")
    other = FakeIgnore("other", "")
    obj = FakeObject(1, needs_ignore=False, tags=[old, other, old])
    issue = replace_tag_issue("replace tag", cast(Any, obj), old, new)

    assert _apply_issue(issue) is True
    assert tuple(obj.tags) == (new, other, new)
    assert _apply_issue(issue) is False


def test_replace_tag_fails_when_neither_precondition_nor_postcondition_holds() -> None:
    old = FakeIgnore("old", "")
    new = FakeIgnore("new", "")
    obj = FakeObject(1, needs_ignore=False, tags=[old])
    issue = replace_tag_issue("replace tag", cast(Any, obj), old, new)
    obj.tags = [FakeIgnore("external", "")]

    with pytest.raises(LintFixError, match="is no longer present"):
        _apply_issue(issue)


def test_already_satisfied_tag_fix_does_not_call_fix_callback() -> None:
    obj = FakeObject(1, needs_ignore=False, is_virtual=True)
    lint = make_lint([obj])
    tag = FakeIgnore("extra", "")
    lint.linters[0].linter = lambda item, _cfg: [
        add_tag_issue("add tag", cast(Any, item), tag),
        add_tag_issue("add tag again", cast(Any, item), tag),
    ]
    applied: list[LintIssue] = []
    cfg = LintConfig(
        autofix=False,
        structured_autofix=True,
        fix_callback=applied.append,
        interactive=False,
    )

    assert list(lint.linters[0](obj, cfg)) == []
    assert tuple(obj.tags) == (tag,)
    assert len(applied) == 1


_LEGACY_AUTOFIX_BRANCHES = {
    ("article/lint.py", "specify_authors"): "creates or resolves Person records",
    ("name/lint.py", "remove_duplicates"): "interactively merges records",
    ("name/lint.py", "_maybe_add_name_variant"): "creates and edits a Name",
    ("name/lint.py", "infer_name_variants"): "creates a Name through a CE method",
    ("taxon/lint.py", "check_base_name"): "switches the Taxon-Name base-name cycle",
    (
        "taxon/lint.py",
        "check_conservative_expected_base_name",
    ): "switches the Taxon-Name base-name cycle",
}


def test_legacy_autofix_branches_are_explicitly_allowlisted() -> None:
    """Keep model lints on structured fixes unless an operation is unsupported."""
    models_dir = Path(__file__).parent
    paths = [
        models_dir / "base.py",
        models_dir / "lint.py",
        *models_dir.glob("*/lint.py"),
        models_dir / "name/page.py",
    ]
    actual: set[tuple[str, str]] = set()
    for path in paths:
        tree = ast.parse(path.read_text())
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Attribute)
                and node.attr == "autofix"
                and isinstance(node.value, ast.Name)
                and node.value.id == "cfg"
            ):
                continue
            parent = parents.get(node)
            while parent is not None and not isinstance(
                parent, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                parent = parents.get(parent)
            assert parent is not None
            branch = (str(path.relative_to(models_dir)), parent.name)
            if branch in {
                ("base.py", "_process_lint_results"),
                ("lint.py", "apply_lint_fix"),
                ("lint.py", "__call__"),
                ("lint.py", "run"),
            }:
                # This is the central structured-fix consumer, not a lint rule.
                continue
            actual.add(branch)

    assert actual == set(_LEGACY_AUTOFIX_BRANCHES), {
        branch: _LEGACY_AUTOFIX_BRANCHES.get(
            branch, "unsupported reason not documented"
        )
        for branch in actual | set(_LEGACY_AUTOFIX_BRANCHES)
        if branch not in actual or branch not in _LEGACY_AUTOFIX_BRANCHES
    }
