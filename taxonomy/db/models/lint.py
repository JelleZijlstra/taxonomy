"""Abstraction for linting models."""

import traceback
from collections.abc import Callable, Collection, Generator, Hashable, Iterable
from dataclasses import dataclass, field, replace
from functools import cache
from typing import Any, ClassVar, Generic, Protocol, TypeVar, cast

from clirm import UnsetVirtualFieldError

from taxonomy import getinput
from taxonomy.config import is_network_available

from .base import BaseModel, LintConfig

ModelT = TypeVar("ModelT", bound=BaseModel)

Linter = Callable[[ModelT, LintConfig], Iterable[str]]
DuplicateKey = Callable[[ModelT], Hashable | None]
DuplicateFixer = Callable[[Hashable, list[ModelT], LintConfig], None]


class IgnoreLint(Protocol):
    label: str


@dataclass(frozen=True)
class IgnoreLintTarget(Generic[ModelT]):
    obj: ModelT
    issues: tuple[str, ...]


@dataclass(frozen=True)
class IgnoreLintPlan(Generic[ModelT]):
    label: str
    comment: str
    targets: tuple[IgnoreLintTarget[ModelT], ...]
    already_ignored: tuple[IgnoreLintTarget[ModelT], ...]


@dataclass
class LintWrapper(Generic[ModelT]):
    linter: Linter[ModelT]
    disabled: bool
    label: str
    lint: Lint[ModelT]
    requires_network: bool = False

    @staticmethod
    def _format_object(obj: ModelT) -> str:
        try:
            return str(obj)
        except UnsetVirtualFieldError:
            if not obj.is_virtual:
                raise
            return f"<virtual {type(obj).__name__} {obj.id!r}>"

    def __call__(self, obj: ModelT, cfg: LintConfig) -> Generator[str, None, set[str]]:
        if self.requires_network and not is_network_available():
            return {self.label}
        try:
            issues = list(self.linter(obj, cfg))
        except Exception as e:
            traceback.print_exc()
            yield f"{self._format_object(obj)}: error running {self.label} linter: {e}"
            # The linter could not establish whether an IgnoreLint is still needed.
            # Preserve it rather than allowing Lint.run() to remove it as unused.
            return {self.label}
        if not issues:
            return set()
        ignored_lints = self.lint.get_ignored_lints(obj)
        if self.label in ignored_lints:
            return {self.label}
        for issue in issues:
            yield f"{self._format_object(obj)}: {issue} [{self.label}]"
        return set()


@dataclass
class Lint(Generic[ModelT]):
    model_cls: type[ModelT]
    get_ignores: Callable[[ModelT], Iterable[IgnoreLint]]
    remove_unused_ignores: Callable[[ModelT, Collection[str]], None]
    add_ignore: Callable[[ModelT, str, str], None] | None = None

    linters: list[LintWrapper[ModelT]] = field(default_factory=list)
    disabled_linters: list[LintWrapper[ModelT]] = field(default_factory=list)
    cache_clearers: list[Callable[[], None]] = field(default_factory=list)

    _by_model: ClassVar[dict[type[BaseModel], Lint[Any]]] = {}

    def __post_init__(self) -> None:
        self._by_model[self.model_cls] = self

    @classmethod
    def for_model(cls, model_cls: type[ModelT]) -> Lint[ModelT]:
        try:
            return cast(Lint[ModelT], cls._by_model[model_cls])
        except KeyError as exc:
            raise ValueError(
                f"{model_cls.__name__} does not use the lint registry"
            ) from exc

    def clear_caches(self) -> None:
        for clear_cache in self.cache_clearers:
            clear_cache()

    def add_ignore_lint_to_all(
        self,
        label: str,
        comment: str,
        *,
        dry_run: bool = True,
        query: Iterable[ModelT] | None = None,
    ) -> IgnoreLintPlan[ModelT]:
        if self.add_ignore is None:
            raise ValueError(
                f"{self.model_cls.__name__} does not support IgnoreLint tags"
            )
        if not comment.strip():
            raise ValueError("an IgnoreLint comment is required")
        matching_linters = [
            linter
            for linter in (*self.linters, *self.disabled_linters)
            if linter.label == label
        ]
        if not matching_linters:
            available = ", ".join(
                sorted(
                    linter.label for linter in (*self.linters, *self.disabled_linters)
                )
            )
            raise ValueError(
                f"unknown {self.model_cls.__name__} lint {label!r}; "
                f"available lints: {available}"
            )
        if len(matching_linters) > 1:
            raise ValueError(
                f"{self.model_cls.__name__} has multiple lints labeled {label!r}"
            )
        linter = matching_linters[0]
        if linter.requires_network and not is_network_available():
            raise RuntimeError(
                f"cannot evaluate network-required lint {label!r} while network "
                "linting is unavailable"
            )

        if query is None:
            query = self.model_cls.select_valid()
        cfg = LintConfig(autofix=False, interactive=False, enable_all=linter.disabled)
        targets: list[IgnoreLintTarget[ModelT]] = []
        already_ignored: list[IgnoreLintTarget[ModelT]] = []
        self.clear_caches()
        try:
            for obj in getinput.print_every_n(
                query, label=f"{self.model_cls.__name__}s"
            ):
                try:
                    issues = tuple(linter.linter(obj, cfg))
                except Exception as exc:
                    raise RuntimeError(
                        f"error running {label!r} for {obj}: {exc}"
                    ) from exc
                if not issues:
                    continue
                target = IgnoreLintTarget(obj, issues)
                if self.is_ignoring_lint(obj, label):
                    already_ignored.append(target)
                else:
                    targets.append(target)
        finally:
            self.clear_caches()

        plan = IgnoreLintPlan(label, comment, tuple(targets), tuple(already_ignored))
        if not dry_run:
            for target in plan.targets:
                self.add_ignore(target.obj, label, comment)
        for target in plan.targets:
            prefix = "WOULD_ADD_IGNORE_LINT" if dry_run else "ADD_IGNORE_LINT"
            print(
                f"{prefix} object={target.obj!r} lint={label!r} "
                f"comment={comment!r} issues={' | '.join(target.issues)!r}"
            )
        mode = "Dry run" if dry_run else "Applied"
        print(
            f"{mode}: {len(plan.targets)} IgnoreLint tag(s) to add, "
            f"{len(plan.already_ignored)} already present."
        )
        if dry_run:
            print("No database changes made. Pass dry_run=False to add these tags.")
        return plan

    def add(
        self,
        label: str,
        *,
        disabled: bool = False,
        requires_network: bool = False,
        clear_caches: Callable[[], None] | None = None,
    ) -> Callable[[Linter[ModelT]], LintWrapper[ModelT]]:

        def decorator(linter: Linter[ModelT]) -> LintWrapper[ModelT]:
            lint_wrapper = LintWrapper(linter, disabled, label, self, requires_network)
            if disabled:
                self.disabled_linters.append(lint_wrapper)
            else:
                self.linters.append(lint_wrapper)
            if clear_caches is not None:
                self.cache_clearers.append(clear_caches)
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
                            if fixer is not None and not self.is_ignoring_lint(
                                obj, label
                            ):
                                fixer(my_key, [obj, *matching_others], cfg)

            return self.add(
                label, disabled=disabled, clear_caches=get_object_to_issues.cache_clear
            )(linter)

        return decorator

    def run(self, obj: ModelT, cfg: LintConfig) -> Iterable[str]:
        if cfg.enable_all:
            linters = [*self.linters, *self.disabled_linters]
        else:
            linters = self.linters

        used_ignores: set[str] = set()
        actual_ignores = self.get_ignored_lints(obj)
        for linter in linters:
            if linter.label in actual_ignores:
                # IgnoreLint must also prevent automated mutations. Individual
                # linters should not need to remember to check their own label.
                lint_cfg = replace(cfg, autofix=False, interactive=False)
            else:
                lint_cfg = cfg
            used_ignores |= yield from linter(obj, lint_cfg)
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
