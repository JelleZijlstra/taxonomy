"""Abstraction for linting models."""

from __future__ import annotations

import traceback
from collections import Counter
from collections.abc import Callable, Generator, Hashable, Iterable
from dataclasses import dataclass, field, replace
from functools import cache
from typing import Any, ClassVar, Generic, Protocol, TypeVar, cast

from clirm import UnsetVirtualFieldError

from taxonomy import getinput
from taxonomy.config import is_network_available

from .base import BaseModel, LintConfig
from .lint_types import LintIssue, LintResult

ModelT = TypeVar("ModelT", bound=BaseModel)


class LintFixError(RuntimeError):
    """Raised when a structured lint fix is no longer safe to apply."""


class FixOperation(Protocol):
    """One independently guarded operation within a structured lint fix."""

    @property
    def target(self) -> BaseModel:
        """Return the model mutated by this operation."""
        ...

    def validate(self) -> bool:
        """Validate the operation and return whether it still needs applying."""
        ...

    def apply(self) -> None:
        """Apply the previously validated operation."""
        ...


@dataclass(frozen=True)
class FieldChange:
    """One guarded field assignment in a structured lint fix."""

    target: BaseModel
    field: str
    expected: Any
    new: Any

    def validate(self) -> bool:
        current = getattr(self.target, self.field)
        if current == self.new:
            return False
        if current != self.expected:
            raise LintFixError(
                f"{self.target}: field {self.field} changed from "
                f"{self.expected!r} to {current!r} before autofix"
            )
        return True

    def apply(self) -> None:
        setattr(self.target, self.field, self.new)


def _get_tags(target: BaseModel, field: str) -> tuple[Any, ...]:
    current = getattr(target, field)
    return () if current is None else tuple(current)


@dataclass(frozen=True)
class AddTag:
    """Add one tag without guarding or replacing the complete tags field."""

    target: BaseModel
    tag: Any
    field: str = "tags"

    def validate(self) -> bool:
        return self.tag not in _get_tags(self.target, self.field)

    def apply(self) -> None:
        tags = _get_tags(self.target, self.field)
        if self.tag not in tags:
            setattr(self.target, self.field, (*tags, self.tag))


@dataclass(frozen=True)
class RemoveTag:
    """Remove every occurrence of one exact tag, preserving unrelated tags."""

    target: BaseModel
    tag: Any
    field: str = "tags"

    def validate(self) -> bool:
        return self.tag in _get_tags(self.target, self.field)

    def apply(self) -> None:
        tags = _get_tags(self.target, self.field)
        setattr(self.target, self.field, tuple(tag for tag in tags if tag != self.tag))


@dataclass(frozen=True)
class ReplaceTag:
    """Replace every occurrence of one exact tag, preserving unrelated tags."""

    target: BaseModel
    old: Any
    new: Any
    field: str = "tags"

    def validate(self) -> bool:
        if self.old == self.new:
            return False
        tags = _get_tags(self.target, self.field)
        if self.old in tags:
            return True
        if self.new in tags:
            return False
        raise LintFixError(
            f"{self.target}: tag {self.old!r} is no longer present and replacement "
            f"{self.new!r} is not present"
        )

    def apply(self) -> None:
        tags = _get_tags(self.target, self.field)
        setattr(
            self.target,
            self.field,
            tuple(self.new if tag == self.old else tag for tag in tags),
        )


@dataclass(frozen=True)
class LintFix:
    """A deterministic, guarded collection of model operations."""

    operations: tuple[FixOperation, ...]

    @property
    def is_virtual_safe(self) -> bool:
        return all(operation.target.is_virtual for operation in self.operations)

    def apply(self) -> bool:
        """Apply the fix and return whether it materially changed any target."""
        # Validate the complete plan before the first mutation so multi-object fixes
        # fail closed instead of leaving a partial state.
        pending = tuple(
            operation for operation in self.operations if operation.validate()
        )
        for operation in pending:
            operation.apply()
        return bool(pending)


def field_fix(target: BaseModel, field: str, new: Any) -> LintFix:
    """Build a guarded assignment, snapshotting the current value."""
    return LintFix((FieldChange(target, field, getattr(target, field), new),))


def combine_fixes(*fixes: LintFix) -> LintFix:
    """Combine guarded fixes into one all-or-nothing plan."""
    return LintFix(tuple(operation for fix in fixes for operation in fix.operations))


def fixes_issue(message: str, *fixes: LintFix, code: str | None = None) -> LintIssue:
    """Create one issue from independently guarded primitive fixes."""
    return LintIssue(message, code=code, fix=combine_fixes(*fixes))


def field_issue(
    message: str, target: BaseModel, field: str, new: Any, *, code: str | None = None
) -> LintIssue:
    """Create the common one-field autofixable lint issue."""
    return fixes_issue(message, field_fix(target, field, new), code=code)


def fields_issue(
    message: str, *changes: tuple[BaseModel, str, Any], code: str | None = None
) -> LintIssue:
    """Create one guarded issue that assigns several model fields."""
    return fixes_issue(
        message,
        *(field_fix(target, field, new) for target, field, new in changes),
        code=code,
    )


def add_tag_fix(target: BaseModel, tag: Any, *, field: str = "tags") -> LintFix:
    """Build an idempotent operation that adds one exact tag."""
    return LintFix((AddTag(target, tag, field),))


def remove_tag_fix(target: BaseModel, tag: Any, *, field: str = "tags") -> LintFix:
    """Build an idempotent operation that removes one exact tag."""
    return LintFix((RemoveTag(target, tag, field),))


def replace_tag_fix(
    target: BaseModel, old: Any, new: Any, *, field: str = "tags"
) -> LintFix:
    """Build a guarded operation that replaces one exact tag."""
    return LintFix((ReplaceTag(target, old, new, field),))


def add_tag_issue(
    message: str,
    target: BaseModel,
    tag: Any,
    *,
    field: str = "tags",
    code: str | None = None,
) -> LintIssue:
    """Create an issue that adds one tag without snapshotting all tags."""
    return fixes_issue(message, add_tag_fix(target, tag, field=field), code=code)


def replace_tag_issue(
    message: str,
    target: BaseModel,
    old: Any,
    new: Any,
    *,
    field: str = "tags",
    code: str | None = None,
) -> LintIssue:
    """Create an issue that replaces one tag without snapshotting all tags."""
    return fixes_issue(
        message, replace_tag_fix(target, old, new, field=field), code=code
    )


def remove_tag_issue(
    message: str,
    target: BaseModel,
    tag: Any,
    *,
    field: str = "tags",
    code: str | None = None,
) -> LintIssue:
    """Create an issue that removes one tag without snapshotting all tags."""
    return fixes_issue(message, remove_tag_fix(target, tag, field=field), code=code)


def count_lint_codes(issues: Iterable[LintResult]) -> Counter[str]:
    """Count structured lint codes while retaining support for plain strings."""
    return Counter(
        issue.code if isinstance(issue, LintIssue) and issue.code else "uncoded"
        for issue in issues
    )


def apply_lint_fix(issue: LintIssue, cfg: LintConfig) -> tuple[bool, LintIssue | None]:
    """Apply a structured fix, returning any issue that remains unresolved."""
    if issue.fix is None or not (cfg.autofix or cfg.structured_autofix):
        return False, issue
    if cfg.structured_autofix and not cfg.autofix and not issue.fix.is_virtual_safe:
        return False, issue
    try:
        changed = issue.fix.apply()
    except Exception as exc:
        reloaded: set[int] = set()
        for operation in issue.fix.operations:
            target = operation.target
            if target.is_virtual or id(target) in reloaded:
                continue
            reload_method = getattr(target, "reload", None)
            if reload_method is not None:
                try:
                    reload_method()
                except Exception:
                    pass
            reloaded.add(id(target))
        return False, LintIssue(
            f"{issue.message}; autofix failed with {type(exc).__name__}: {exc}",
            code=issue.code,
        )
    return changed, None


Linter = Callable[[ModelT, LintConfig], Iterable[LintResult]]
DuplicateKey = Callable[[ModelT], Hashable | None]
DuplicateFixer = Callable[[Hashable, list[ModelT], LintConfig], None]


class IgnoreLint(Protocol):
    label: str


@dataclass(frozen=True)
class IgnoreLintTarget(Generic[ModelT]):
    obj: ModelT
    issues: tuple[LintResult, ...]


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
    skip_virtual: bool = False

    @staticmethod
    def _format_object(obj: ModelT) -> str:
        try:
            return str(obj)
        except UnsetVirtualFieldError:
            if not obj.is_virtual:
                raise
            return f"<virtual {type(obj).__name__} {obj.id!r}>"

    def __call__(
        self, obj: ModelT, cfg: LintConfig
    ) -> Generator[LintIssue, None, set[str]]:
        if self.requires_network and not is_network_available():
            return {self.label}
        if self.skip_virtual and obj.is_virtual:
            return {self.label}
        try:
            issues = list(self.linter(obj, cfg))
        except Exception as e:
            traceback.print_exc()
            yield LintIssue(
                f"{self._format_object(obj)}: error running {self.label} linter: {e}",
                code=self.label,
            )
            # The linter could not establish whether an IgnoreLint is still needed.
            # Preserve it rather than allowing Lint.run() to remove it as unused.
            return {self.label}
        if not issues:
            return set()
        ignored_lints = self.lint.get_ignored_lints(obj)
        if self.label in ignored_lints:
            return {self.label}
        for raw_issue in issues:
            issue = (
                raw_issue.with_code(self.label)
                if isinstance(raw_issue, LintIssue)
                else LintIssue(str(raw_issue), code=self.label)
            )
            code = issue.code or self.label
            formatted = issue.with_message(
                f"{self._format_object(obj)}: {issue} [{code}]"
            )
            changed, remaining = apply_lint_fix(formatted, cfg)
            if remaining is not None:
                yield remaining
                continue
            if changed:
                if cfg.fix_callback is not None:
                    cfg.fix_callback(formatted)
                if cfg.autofix:
                    print(formatted)
        return set()


@dataclass
class Lint(Generic[ModelT]):
    model_cls: type[ModelT]
    get_ignores: Callable[[ModelT], Iterable[IgnoreLint]]
    add_ignore: Callable[[ModelT, str, str], None] | None = None
    ignore_field: str = "tags"

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
                f"comment={comment!r} "
                f"issues={' | '.join(map(str, target.issues))!r}"
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
        skip_virtual: bool = False,
        clear_caches: Callable[[], None] | None = None,
    ) -> Callable[[Linter[ModelT]], LintWrapper[ModelT]]:

        def decorator(linter: Linter[ModelT]) -> LintWrapper[ModelT]:
            lint_wrapper = LintWrapper(
                linter=linter,
                disabled=disabled,
                label=label,
                lint=self,
                requires_network=requires_network,
                skip_virtual=skip_virtual,
            )
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

    def run(self, obj: ModelT, cfg: LintConfig) -> Iterable[LintResult]:
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
                lint_cfg = replace(
                    cfg, autofix=False, structured_autofix=False, interactive=False
                )
            else:
                lint_cfg = cfg
            used_ignores |= yield from linter(obj, lint_cfg)
        actual_ignores = self.get_ignored_lints(obj)
        unused = actual_ignores - used_ignores
        if unused:
            # Don't remove IgnoreLints for disabled linters
            unused -= {linter.label for linter in self.disabled_linters}
        if unused:
            for tag in tuple(self.get_ignores(obj)):
                if tag.label not in unused:
                    continue
                issue = remove_tag_issue(
                    f"{obj}: remove unused IgnoreLint tag {tag}",
                    obj,
                    tag,
                    field=self.ignore_field,
                    code="unused_ignore",
                )
                changed, remaining = apply_lint_fix(issue, cfg)
                if remaining is not None:
                    yield remaining
                elif changed:
                    if cfg.fix_callback is not None:
                        cfg.fix_callback(issue)
                    if cfg.autofix:
                        print(issue)

    def is_ignoring_lint(self, obj: ModelT, label: str) -> bool:
        ignored_lints = self.get_ignored_lints(obj)
        return label in ignored_lints

    def get_ignored_lints(self, obj: ModelT) -> set[str]:
        tags = self.get_ignores(obj)
        return {tag.label for tag in tags}
