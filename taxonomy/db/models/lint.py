"""Abstraction for linting models."""

from __future__ import annotations

import traceback
from collections.abc import Callable, Collection, Generator, Hashable, Iterable
from dataclasses import dataclass, field
from functools import cache
from typing import Generic, Protocol, TypeVar

from .base import BaseModel, LintConfig

ModelT = TypeVar("ModelT", bound=BaseModel)

Linter = Callable[[ModelT, LintConfig], Iterable[str]]
DuplicateKey = Callable[[ModelT], Hashable | None]
DuplicateFixer = Callable[[Hashable, list[ModelT], LintConfig], None]


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
    model_cls: type[ModelT]
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

    def add_duplicate_finder(
        self,
        label: str,
        *,
        disabled: bool = False,
        query: Iterable[ModelT] | None = None,
        fixer: DuplicateFixer[ModelT] | None = None,
    ) -> Callable[[DuplicateKey[ModelT]], LintWrapper[ModelT]]:
        def decorator(dupe_key: DuplicateKey[ModelT]) -> LintWrapper[ModelT]:
            @cache
            def get_object_to_issues() -> dict[int, list[tuple[str, list[ModelT]]]]:
                key_to_objs: dict[Hashable, list[ModelT]] = {}
                for obj in query or self.model_cls.select_valid():
                    key = dupe_key(obj)
                    if key is not None:
                        key_to_objs.setdefault(key, []).append(obj)
                output: dict[int, list[tuple[str, list[ModelT]]]] = {}
                for key, objs in key_to_objs.items():
                    if len(objs) > 1:
                        objs = sorted(objs, key=lambda o: o.id)
                        # Skip the first object, as it's likely the one we'd want to keep
                        for obj in objs[1:]:
                            others = [o for o in objs if o != obj]
                            message = f"Duplicate of {others} (key {key!r})"
                            output.setdefault(obj.id, []).append((message, others))
                return output

            def linter(obj: ModelT, cfg: LintConfig) -> Iterable[str]:
                if obj.is_invalid():
                    return
                mapping = get_object_to_issues()
                if obj.id in mapping:
                    my_key = dupe_key(obj)
                    if my_key is None:
                        return
                    for message, others in mapping[obj.id]:
                        # Recheck in case information has changed
                        matching_others = [
                            o
                            for o in others
                            if dupe_key(o) == my_key and not o.is_invalid()
                        ]
                        if matching_others:
                            yield message
                            if fixer is not None:
                                fixer(my_key, [obj, *matching_others], cfg)

            return self.add(label, disabled=disabled)(linter)

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

    def is_ignoring_lint(self, obj: ModelT, label: str) -> bool:
        ignored_lints = self.get_ignored_lints(obj)
        return label in ignored_lints

    def get_ignored_lints(self, obj: ModelT) -> set[str]:
        tags = self.get_ignores(obj)
        return {tag.label for tag in tags}
