"""

Abstraction for linting models.

"""

from __future__ import annotations

import traceback
from collections.abc import Callable, Collection, Generator, Iterable
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar

from .base import BaseModel, LintConfig

ModelT = TypeVar("ModelT", bound=BaseModel)

Linter = Callable[[ModelT, LintConfig], Iterable[str]]


class IgnoreLint(Protocol):
    label: str


@dataclass
class LintWrapper(Generic[ModelT]):
    linter: Linter[ModelT]
    disabled: bool
    label: str
    lint: Lint[ModelT]

    def __call__(self, obj: ModelT, cfg: LintConfig) -> Generator[str, None, set[str]]:
        try:
            issues = list(self.linter(obj, cfg))
        except Exception as e:
            traceback.print_exc()
            yield f"{obj}: error running {self.label} linter: {e}"
            return set()
        if not issues:
            return set()
        ignored_lints = self.lint.get_ignored_lints(obj)
        if self.label in ignored_lints:
            return {self.label}
        for issue in issues:
            yield f"{obj}: {issue} [{self.label}]"
        return set()


@dataclass
class Lint(Generic[ModelT]):
    get_ignores: Callable[[ModelT], Iterable[IgnoreLint]]
    remove_unused_ignores: Callable[[ModelT, Collection[str]], None]

    linters: list[LintWrapper[ModelT]] = field(default_factory=list)
    disabled_linters: list[LintWrapper[ModelT]] = field(default_factory=list)

    def add(
        self, label: str, *, disabled: bool = False
    ) -> Callable[[Linter[ModelT]], LintWrapper[ModelT]]:
        def decorator(linter: Linter[ModelT]) -> LintWrapper[ModelT]:
            lint_wrapper = LintWrapper(linter, disabled, label, self)
            if disabled:
                self.disabled_linters.append(lint_wrapper)
            else:
                self.linters.append(lint_wrapper)
            return lint_wrapper

        return decorator

    def run(
        self, obj: ModelT, cfg: LintConfig, *, include_disabled: bool = False
    ) -> Iterable[str]:
        if include_disabled:
            linters = [*self.linters, *self.disabled_linters]
        else:
            linters = self.linters

        used_ignores: set[str] = set()
        for linter in linters:
            used_ignores |= yield from linter(obj, cfg)
        actual_ignores = self.get_ignored_lints(obj)
        unused = actual_ignores - used_ignores
        if unused:
            # Don't remove IgnoreLints for disabled linters
            unused -= {linter.label for linter in self.disabled_linters}
        if unused:
            if cfg.autofix:
                self.remove_unused_ignores(obj, unused)
            else:
                yield f"{obj}: has unused IgnoreLint tags {', '.join(unused)}"

    def get_ignored_lints(self, obj: ModelT) -> set[str]:
        tags = self.get_ignores(obj)
        return {tag.label for tag in tags}
