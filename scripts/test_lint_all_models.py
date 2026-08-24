import io
import json
from collections.abc import Iterator
from types import SimpleNamespace
from typing import ClassVar, cast

import pytest

from scripts import lint_all_models
from taxonomy.db.models.base import BaseModel, LintConfig
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintIssue


class _FakeQuery:
    def __init__(self, objects: list[_FakeObject]) -> None:
        self.objects = objects

    def count(self) -> int:
        return len(self.objects)

    def __iter__(self) -> Iterator[_FakeObject]:
        return iter(self.objects)


class _FakeObject:
    label = "before"
    id = 1

    def general_lint(self, cfg: LintConfig) -> Iterator[LintIssue | str]:
        assert cfg.autofix
        assert not cfg.interactive
        assert cfg.fix_callback is not None
        issue = field_issue("autofix label", cast(BaseModel, self), "label", "after")
        assert isinstance(issue, LintIssue)
        assert issue.fix is not None
        yield from BaseModel._process_lint_results([issue], cfg)
        yield "needs review"


class _FakeModel:
    label_field = "label"
    objects: ClassVar[list[_FakeObject]] = [_FakeObject()]
    caches_cleared = 0

    @classmethod
    def select(cls) -> _FakeQuery:
        return _FakeQuery(cls.objects)

    @classmethod
    def clear_lint_caches(cls) -> None:
        cls.caches_cleared += 1


def test_lint_models_applies_fixes_and_writes_remaining_issues(
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = io.StringIO()
    model = cast(type[BaseModel], _FakeModel)

    summary = lint_all_models.lint_models([model], output, progress_every=1)

    assert _FakeModel.objects[0].label == "after"
    assert summary.objects_checked == 1
    assert summary.objects_with_issues == 1
    assert summary.issues == 1
    assert summary.structured_autofixes == 1
    assert summary.issues_by_model == {"_FakeModel": 1}
    assert summary.issues_by_code == {"uncoded": 1}
    assert json.loads(output.getvalue()) == {
        "schema_version": 1,
        "object": {"model": "_FakeModel", "id": 1, "label": "after"},
        "issues": [{"code": None, "message": "needs review"}],
    }
    printed = capsys.readouterr().out
    assert "PROGRESS model=_FakeModel objects=1/1" in printed
    assert "ISSUE model=_FakeModel id=1 code=uncoded: needs review" in printed
    assert "DONE model=_FakeModel objects=1 objects_with_issues=1 issues=1" in printed
    assert _FakeModel.caches_cleared == 2


def test_lint_models_records_errors_and_continues(
    capsys: pytest.CaptureFixture[str],
) -> None:
    broken = SimpleNamespace(
        id=2,
        label="broken",
        general_lint=lambda _cfg: (_ for _ in ()).throw(ValueError("bad lint")),
    )
    clean = SimpleNamespace(id=3, label="clean", general_lint=lambda _cfg: ())

    class ErrorModel:
        label_field = "label"

        @classmethod
        def select(cls) -> _FakeQuery:
            return _FakeQuery(cast(list[_FakeObject], [broken, clean]))

        @classmethod
        def clear_lint_caches(cls) -> None:
            pass

    output = io.StringIO()
    summary = lint_all_models.lint_models(
        [cast(type[BaseModel], ErrorModel)], output, progress_every=10
    )

    assert summary.objects_checked == 2
    assert summary.issues == 1
    row = json.loads(output.getvalue())
    assert row["object"] == {"model": "ErrorModel", "id": 2, "label": "broken"}
    assert row["issues"][0]["code"] == "lint_runner_error"
    assert "ValueError: bad lint" in row["issues"][0]["message"]
    assert (
        "ISSUE model=ErrorModel id=2 code=lint_runner_error" in capsys.readouterr().out
    )


def test_select_model_classes_rejects_unknown_model() -> None:
    with pytest.raises(ValueError, match=r"unknown model\(s\): NotAModel") as exc_info:
        lint_all_models.select_model_classes(["NotAModel"])
    assert "Person" in str(exc_info.value)
