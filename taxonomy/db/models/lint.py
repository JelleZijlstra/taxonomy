"""

Abstraction for linting models.

"""

import functools
import traceback
from collections.abc import Callable, Collection, Generator, Iterable
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar

from .base import BaseModel, LintConfig

ModelT = TypeVar("ModelT", bound=BaseModel)

Linter = Callable[[ModelT, LintConfig], Iterable[str]]
IgnorableLinter = Callable[[ModelT, LintConfig], Generator[str, None, set[str]]]


class IgnoreLint(Protocol):
    label: str


@dataclass
class Lint(Generic[ModelT]):
    get_ignores: Callable[[ModelT], Iterable[IgnoreLint]]
    remove_unused_ignores: Callable[[ModelT, Collection[str]], None]

    linters: list[IgnorableLinter[ModelT]] = field(default_factory=list)
    disabled_linters: list[IgnorableLinter[ModelT]] = field(default_factory=list)

    def add(
        self, label: str, *, disabled: bool = False
    ) -> Callable[[Linter[ModelT]], IgnorableLinter[ModelT]]:
        def decorator(linter: Linter[ModelT]) -> IgnorableLinter[ModelT]:
            @functools.wraps(linter)
            def wrapper(obj: ModelT, cfg: LintConfig) -> Generator[str, None, set[str]]:
                try:
                    issues = list(linter(obj, cfg))
                except Exception as e:
                    traceback.print_exc()
                    yield f"{obj}: error running {label} linter: {e}"
                    return set()
                if not issues:
                    return set()
                ignored_lints = self.get_ignored_lints(obj)
                if label in ignored_lints:
                    return {label}
                for issue in issues:
                    yield f"{obj}: {issue} [{label}]"
                return set()

            if disabled:
                self.disabled_linters.append(wrapper)
            else:
                self.linters.append(wrapper)
            return wrapper

        return decorator

    def run(
        self, obj: ModelT, cfg: LintConfig, *, include_disabled: bool = False
    ) -> Iterable[str]:
        if include_disabled:
            linters = [*self.linters, *self.disabled_linters]
        else:
            linters = [*self.linters]

        used_ignores: set[str] = set()
        for linter in linters:
            used_ignores |= yield from linter(obj, cfg)
        actual_ignores = self.get_ignored_lints(obj)
        unused = actual_ignores - used_ignores
        if unused:
            if cfg.autofix:
                self.remove_unused_ignores(obj, unused)
            else:
                yield f"{obj}: has unused IgnoreLint tags {', '.join(unused)}"

    def get_ignored_lints(self, obj: ModelT) -> set[str]:
        tags = self.get_ignores(obj)
        return {tag.label for tag in tags}
