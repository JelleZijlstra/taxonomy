from __future__ import annotations

import builtins
import enum
import functools
import importlib
import inspect
import json
import pickle
import re
import sqlite3
import traceback
import typing
import urllib.parse
from collections import defaultdict
from collections.abc import Callable, Collection, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from types import NoneType
from typing import Any, ClassVar, Generic, Self, TypeVar

import typing_inspect
from clirm import Clirm, Field, Model, Query

from taxonomy import adt, config, events, getinput
from taxonomy.apis.cloud_search import SearchField
from taxonomy.db import cached_data, derived_data, helpers, models
from taxonomy.db.constants import StringKind

settings = config.get_options()


class LazyClirm(Clirm):
    _conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = self.make_connection()
        return self._conn

    @conn.setter
    def conn(self, value: sqlite3.Connection) -> None:
        self._conn = value

    def reconnect(self) -> None:
        if self._conn is not None:
            self._conn.close()
        self._conn = self.make_connection()

    def make_connection(self) -> sqlite3.Connection:
        # static analysis: ignore[internal_error]
        return sqlite3.connect(str(settings.db_filename))

    def __init__(self) -> None:
        super().__init__(None)  # static analysis: ignore[incompatible_argument]


_getters: dict[tuple[type[Model], str | None], _NameGetter[Any]] = {}


class _FieldEditor:
    """For easily editing fields. This is exposed as object.e."""

    def __init__(self, instance: Any = None) -> None:
        self.instance = instance

    def __get__(self, instance: Any, instance_type: Any) -> _FieldEditor:
        return self.__class__(instance)

    def __getattr__(self, field: str) -> None:
        if field == "all":
            self.instance.fill_required_fields()
        else:
            self.instance.fill_field(field)

    def __dir__(self) -> list[str]:
        return ["all", *sorted(self.instance.clirm_fields.keys())]


@dataclass(frozen=True)
class LintConfig:
    autofix: bool = True
    interactive: bool = True
    verbose: bool = False
    manual_mode: bool = False
    enable_all: bool = False
    # Enables lints that I am aiming to enable but that are not clean yet.
    experimental: bool = False


ADTT = TypeVar("ADTT", bound=adt.ADT)
ModelT = TypeVar("ModelT", bound="BaseModel")
Linter = Callable[[ModelT, LintConfig], Iterable[str]]


class BaseModel(Model):
    id: Any
    label_field: str
    label_field_has_underscores = False
    # If given, lists are separated into groups based on this field.
    grouping_field: str | None = None
    call_sign: str
    creation_event: events.Event[Any]
    save_event: events.Event[Any]
    field_defaults: ClassVar[dict[str, Any]] = {}
    excluded_fields: ClassVar[set[str]] = set()
    derived_fields: ClassVar[list[derived_data.DerivedField[Any]]] = []
    _name_to_derived_field: ClassVar[dict[str, derived_data.DerivedField[Any]]] = {}
    call_sign_to_model: ClassVar[dict[str, type[BaseModel]]] = {}
    fields_may_be_invalid: ClassVar[set[str]] = set()
    markdown_fields: ClassVar[set[str]] = set()
    search_fields: ClassVar[Sequence[SearchField]] = ()
    fields_without_completers: ClassVar[Collection[str]] = set()

    clirm = LazyClirm()

    e = _FieldEditor()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if hasattr(cls, "call_sign"):
            BaseModel.call_sign_to_model[cls.call_sign] = cls
        cls._name_to_derived_field = {field.name: field for field in cls.derived_fields}
        for field in cls.derived_fields:
            if field.typ is derived_data.SetLater:
                field.typ = cls

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        result = super().create(**kwargs)
        if hasattr(cls, "creation_event"):
            cls.creation_event.trigger(result)
        return result

    @classmethod
    def clear_lint_caches(cls) -> None:
        pass

    @classmethod
    def lint_all(
        cls,
        linter: Linter[Self] | None = None,
        *,
        autofix: bool = True,
        interactive: bool = False,
        verbose: bool = False,
        manual_mode: bool = False,
        enable_all: bool = False,
        experimental: bool = False,
        query: Iterable[Self] | None = None,
    ) -> list[tuple[Self, list[str]]]:
        cls.clear_lint_caches()
        cfg = LintConfig(
            autofix=autofix,
            interactive=interactive,
            verbose=verbose,
            manual_mode=manual_mode,
            enable_all=enable_all,
            experimental=experimental,
        )
        if query is None:
            if linter is None:
                query = cls.select()
            else:
                # For specific linters, only worry about valid names
                query = cls.select_valid()
        if linter is None:
            linter = cls.general_lint
        bad = []
        for obj in getinput.print_every_n(query, label=f"{cls.__name__}s"):
            messages = list(linter(obj, cfg))
            if messages:
                for message in messages:
                    print(message)
                bad.append((obj, messages))
        cls.clear_lint_caches()
        return bad

    def format(self, *, quiet: bool = False, cfg: LintConfig = LintConfig()) -> bool:
        # First autofix
        for _ in self.general_lint(replace(cfg, interactive=False)):
            pass
        # Then allow interactive fixing
        messages = list(self.general_lint(cfg))
        if not messages:
            if not quiet:
                print("Everything clean")
            return True
        for message in messages:
            print(message)
        return False

    def is_lint_clean(
        self,
        extra_linter: Linter[Self] | None = None,
        cfg: LintConfig = LintConfig(interactive=False, autofix=False),
    ) -> bool:
        messages = list(self.general_lint(cfg))
        if extra_linter is not None and not self.is_invalid():
            messages += extra_linter(self, cfg)
        if not messages:
            return True
        for message in messages:
            print(message)
        return False

    def general_lint(self, cfg: LintConfig = LintConfig()) -> Iterable[str]:
        unrenderable = list(self.check_renderable())
        if unrenderable:
            yield from unrenderable
            return
        yield from self.check_all_fields(cfg)
        if self.is_invalid():
            yield from self.lint_invalid(cfg)
        else:
            yield from self.lint(cfg)

    def check_renderable(self) -> Iterable[str]:
        try:
            repr(self)
        except Exception as e:
            yield f"{self.id} ({type(self).__name__}): cannot repr() due to {e!r}"
        for field in self.fields():
            try:
                getattr(self, field)
            except Exception as e:
                yield (
                    f"{self.id} ({type(self).__name__}): cannot get field {field} due"
                    f" to {e!r}"
                )

    def _edit_by_word(self, text: str) -> str:
        callbacks: dict[str, Callable[[], object]] = {
            **self.get_adt_callbacks(),
            "reload_helpers": lambda: importlib.reload(helpers),
        }
        return getinput.edit_by_word(text, callbacks=callbacks)

    def check_all_fields(self, cfg: LintConfig) -> Iterable[str]:
        is_invalid = self.is_invalid()
        message: str | None
        for field in self.fields():
            if field in self.fields_may_be_invalid:
                continue
            value = getattr(self, field)
            if value is None:
                continue
            field_obj = getattr(type(self), field)
            if issubclass(field_obj.type_object, Model):
                try:
                    target = value.get_redirect_target()
                except field_obj.type_object.DoesNotExist:
                    yield f"{self}: references non-existent object {value} in field {field}"
                    continue
                if target is not None:
                    message = (
                        f"{self}: references redirected object {value} -> {target} in"
                        f" field {field}"
                    )
                    if cfg.autofix:
                        print(message)
                        setattr(self, field, target)
                    else:
                        yield message
                # We don't care if invalid objects reference other invalid objects
                elif not is_invalid and value.is_invalid():
                    yield f"{self}: references invalid object {value} in field {field}"
            elif isinstance(field_obj, ADTField):
                assert isinstance(value, tuple)
                raw_value = field_obj.get_raw(self)
                serialized = field_obj.serialize(value)
                if serialized is None:
                    if value:
                        yield f"{self}: field {field}: serialized data is None"
                elif raw_value != serialized:
                    getinput.diff_strings(raw_value, serialized)
                    if len(value) == len(json.loads(serialized)):
                        print(
                            f"{self}: field {field}: raw data and serialized data differ"
                        )
                        # Force updating the value
                        setattr(self, field, value)
                    else:
                        yield (
                            f"{self}: field {field}: raw data does not match serialized"
                            f" data"
                        )
                if cfg.autofix:
                    new_tags = []
                    made_change = False
                    for tag in value:
                        tag_type = type(tag)
                        overrides = {}
                        for attr_name in tag_type._attributes:
                            attr_value = getattr(tag, attr_name)
                            if (
                                attr_value is None
                                and attr_name in tag_type.__required_attrs__
                            ):
                                yield (
                                    f"{self}: missing required attribute {attr_name} on"
                                    f" {field} tag {tag}"
                                )
                            if isinstance(attr_value, BaseModel):
                                target = attr_value.get_redirect_target()
                                if target is not None:
                                    print(
                                        f"{self}: references redirected object"
                                        f" {attr_value} -> {target}"
                                    )
                                    overrides[attr_name] = target
                                elif not is_invalid and attr_value.is_invalid():
                                    yield (
                                        f"{self}: references invalid object"
                                        f" {attr_value} in {field} tag {tag}"
                                    )
                            elif isinstance(attr_value, str):
                                cleaned = helpers.interactive_clean_string(
                                    attr_value,
                                    clean_whitespace=True,
                                    interactive=cfg.interactive,
                                )
                                if cleaned != attr_value:
                                    print(
                                        f"{self}: in tags: clean {attr_value!r} ->"
                                        f" {cleaned!r}"
                                    )
                                    overrides[attr_name] = cleaned
                                if (
                                    attr_value == ""
                                    and attr_name in tag_type.__optional_attrs__
                                ):
                                    print(
                                        f"{self}: in tags: empty attribute {attr_name}"
                                        f" on {field} tag {tag}"
                                    )
                                    overrides[attr_name] = None
                                if not is_invalid:
                                    if message := helpers.is_string_clean(cleaned):
                                        yield f"{self}: in tags: {message} in {cleaned!r}"
                                    if not cleaned.isprintable():
                                        message = (
                                            f"{self}: contains unprintable characters:"
                                            f" {cleaned!r}"
                                        )
                                        if cfg.interactive:
                                            self.display()
                                            print(message)
                                            overrides[attr_name] = self._edit_by_word(
                                                cleaned
                                            )
                                        yield message

                                    match helpers.get_string_kind(
                                        tag_type.__annotations__[attr_name]
                                    ):
                                        case StringKind.markdown:
                                            cleaned_value = yield from models.article.lint.lint_referenced_text(
                                                attr_value, prefix=f"{self}: "
                                            )
                                            if cleaned_value != attr_value:
                                                print(
                                                    f"{self} (#{self.id}): field {field}: clean"
                                                    f" {attr_value!r} -> {cleaned_value!r}"
                                                )
                                                overrides[attr_name] = cleaned_value
                                        case StringKind.url:
                                            parsed = urllib.parse.urlparse(attr_value)
                                            if (
                                                parsed.scheme not in ("http", "https")
                                                or not parsed.netloc
                                            ):
                                                yield (
                                                    f"{self}: field {field}: invalid URL: {attr_value!r}"
                                                )
                                        case StringKind.regex:
                                            try:
                                                re.compile(attr_value)
                                            except re.error:
                                                yield (
                                                    f"{self}: field {field}: invalid regex: {attr_value!r}"
                                                )
                                        case StringKind.managed:
                                            pass
                        if overrides:
                            made_change = True
                            new_tags.append(adt.replace(tag, **overrides))
                        else:
                            new_tags.append(tag)
                    if not field_obj.is_ordered:
                        new_tags = sorted(set(new_tags))
                        if tuple(new_tags) != value:
                            made_change = True
                    if made_change:
                        setattr(self, field, tuple(new_tags))
                else:
                    for tag in value:
                        tag_type = type(tag)
                        for attr_name in tag_type._attributes:
                            attr_value = getattr(tag, attr_name)
                            if isinstance(value, BaseModel):
                                if not is_invalid and value.is_invalid():
                                    yield (
                                        f"{self}: references invalid object"
                                        f" {attr_value} in {field} tag {tag}"
                                    )
                            elif isinstance(attr_value, str):
                                cleaned = helpers.interactive_clean_string(
                                    attr_value,
                                    clean_whitespace=True,
                                    interactive=cfg.interactive,
                                )
                                if cleaned != attr_value:
                                    yield (
                                        f"{self}: in tags: clean {attr_value!r} ->"
                                        f" {cleaned!r}"
                                    )
                                if (
                                    attr_value == ""
                                    and attr_name in tag_type.__optional_attrs__
                                ):
                                    yield (
                                        f"{self}: in tags: empty attribute {attr_name}"
                                        f" on {field} tag {tag}"
                                    )
                                if not is_invalid:
                                    if message := helpers.is_string_clean(cleaned):
                                        yield f"{self}: in tags: {message} in {cleaned!r}"
                                    if not cleaned.isprintable():
                                        yield (
                                            f"{self}: contains unprintable characters:"
                                            f" {cleaned!r}"
                                        )
                                    match helpers.get_string_kind(
                                        tag_type.__annotations__[attr_name]
                                    ):
                                        case StringKind.markdown:
                                            cleaned_value = yield from models.article.lint.lint_referenced_text(
                                                attr_value, prefix=f"{self}: "
                                            )
                                            if cleaned_value != attr_value:
                                                yield (
                                                    f"{self} (#{self.id}): field {field}: clean"
                                                    f" {attr_value!r} -> {cleaned_value!r}"
                                                )
                                        case StringKind.url:
                                            parsed = urllib.parse.urlparse(attr_value)
                                            if (
                                                parsed.scheme not in ("http", "https")
                                                or not parsed.netloc
                                            ):
                                                yield (
                                                    f"{self}: field {field}: invalid URL: {attr_value!r}"
                                                )
                                        case StringKind.regex:
                                            try:
                                                re.compile(attr_value)
                                            except re.error:
                                                yield (
                                                    f"{self}: field {field}: invalid regex: {attr_value!r}"
                                                )
                                        case StringKind.managed:
                                            pass
                    if not field_obj.is_ordered:
                        if list(value) != sorted(set(value)):
                            yield (
                                f"{self}: contains duplicate or unsorted tags in"
                                f" {field}"
                            )
            elif field_obj.type_object is str:
                if self.should_exempt_from_string_cleaning(field):
                    continue
                allow_newlines = isinstance(field_obj, (TextField, TextOrNullField))
                cleaned = helpers.interactive_clean_string(
                    value,
                    clean_whitespace=not allow_newlines,
                    verbose=True,
                    interactive=cfg.interactive,
                )
                if cleaned != value:
                    message = (
                        f"{self} (#{self.id}): field {field}: clean {value!r} ->"
                        f" {cleaned!r}"
                    )
                    if cfg.autofix:
                        print(message)
                        try:
                            setattr(self, field, cleaned)
                        except sqlite3.IntegrityError:
                            if (
                                self.get_redirect_target() is not None
                                or field == "pattern"
                            ):
                                print(f"{self}: adding '(merged)'")
                                setattr(self, field, f"{cleaned} (merged)")
                            else:
                                raise
                    else:
                        yield message
                if not is_invalid:
                    if message := helpers.is_string_clean(cleaned):
                        yield f"{self}: field {field}: {message} in {cleaned!r}"
                    if not cleaned.isprintable():
                        if allow_newlines and cleaned.replace("\n", "").isprintable():
                            continue
                        message = (
                            f"{self}: field {field}: contains unprintable characters:"
                            f" {cleaned!r}"
                        )
                        if cfg.interactive:
                            self.display()
                            print(message)
                            if allow_newlines:
                                self.fill_field(field)
                            else:
                                new_value = self._edit_by_word(cleaned)
                                setattr(self, field, new_value)
                        yield message
                    unredirected = yield from models.article.lint.lint_referenced_text(
                        value, prefix=f"{self}: "
                    )
                    if unredirected != value:
                        message = (
                            f"{self} (#{self.id}): field {field}: clean {value!r} ->"
                            f" {unredirected!r}"
                        )
                        if cfg.autofix:
                            print(message)
                            setattr(self, field, unredirected)
                        else:
                            yield message
        if is_invalid:
            target = self.get_redirect_target()
            if target is not None:
                try:
                    secondary_target = target.get_redirect_target()
                except target.DoesNotExist:
                    yield f"{self}: redirect target {target} is invalid"
                else:
                    if secondary_target is not None:
                        yield f"{self}: double redirect to {target} -> {secondary_target}"

    def should_exempt_from_string_cleaning(self, field: str) -> bool:
        """If this returns True, we won't call clean_string() on the field in lint."""
        return False

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        """Yield messages if something is wrong with this object."""
        return []

    def lint_invalid(self, cfg: LintConfig) -> Iterable[str]:
        """Like lint() but only called if is_invalid() returned True."""
        return []

    def save(self) -> None:
        super().save()
        if hasattr(self, "save_event"):
            self.save_event.trigger(self)

    def dump_data(self) -> str:
        return f"{self.__class__.__name__}({self.__dict__!r})"

    def full_data(self) -> None:
        print(f"id: {self.id}")
        for field in sorted(self.fields()):
            try:
                value = getattr(self, field)
                if isinstance(value, enum.Enum):
                    value = value.name
                if value is not None:
                    print(f"{field}: {value}")
            except Exception:
                traceback.print_exc()
                print(f"{field}: could not get value")

    def get_search_dicts(self) -> list[dict[str, Any]]:
        return []

    def display(self, *, full: bool = False) -> None:
        """Print data about this object.

        Subclasses may use the full parameter to decide how much data to show.

        """
        self.full_data()

    def display_concise(self) -> None:
        """Print data about this object, optionally in a more concise manner."""
        self.display()

    def get_derived_field(self, name: str, *, force_recompute: bool = False) -> Any:
        return self._name_to_derived_field[name].get_value(
            self, force_recompute=force_recompute
        )

    def get_raw_derived_field(self, name: str, *, force_recompute: bool = False) -> Any:
        return self._name_to_derived_field[name].get_raw_value(
            self, force_recompute=force_recompute
        )

    def set_derived_field(self, name: str, value: Any) -> None:
        self._name_to_derived_field[name].set_value(self, value)

    def get_raw_tags_field(self, name: str) -> Any:
        data = getattr(type(self), name).get_raw(self)
        if not data:
            return []
        return json.loads(data)

    def map_tags_field(
        self,
        field: ADTField[Any],
        fn: Callable[[adt.ADT], adt.ADT | None],
        *,
        dry_run: bool = False,
    ) -> None:
        existing_tags = getattr(self, field.name)
        if existing_tags is None:
            return
        new_tags = []
        for tag in existing_tags:
            new_tag = fn(tag)
            if new_tag is not None:
                new_tags.append(new_tag)
        if existing_tags != tuple(new_tags):
            if dry_run:
                print(f"Changing tags on {self}")
                getinput.print_diff(existing_tags, tuple(new_tags))
            else:
                setattr(self, field.name, tuple(new_tags))

    def map_tags_by_type(
        self,
        field: ADTField[Any],
        typ: builtins.type[Any],
        fn: Callable[[Any], Any],
        *,
        dry_run: bool = False,
    ) -> None:
        def map_fn(tag: adt.ADT) -> adt.ADT:
            new_args = {}
            tag_type = type(tag)
            if not tag_type._attributes:
                return tag
            for arg_name, arg_type in tag_type._attributes.items():
                val = getattr(tag, arg_name)
                if arg_type is typ:
                    new_args[arg_name] = fn(val)
                else:
                    new_args[arg_name] = val
            return tag_type(**new_args)

        self.map_tags_field(field, map_fn, dry_run=dry_run)

    @classmethod
    def compute_all_derived_fields(cls) -> None:
        if not cls.derived_fields:
            return
        single_compute_fields = [
            field for field in cls.derived_fields if field.compute is not None
        ]
        if single_compute_fields:
            print(
                f"Computing {', '.join(field.name for field in single_compute_fields)}",
                flush=True,
            )
            for i, obj in enumerate(cls.select_valid()):
                if i > 0 and i % 1000 == 0:
                    print(f"{i} done", flush=True)
                for field in cls.derived_fields:
                    if field.compute is not None:
                        value = field.compute(obj)
                        field.set_value(obj, value)

        for field in cls.derived_fields:
            if field.compute_all is not None:
                print(f"Computing {field.name}", flush=True)
                field.compute_and_store_all(cls)

    def sort_key(self) -> Any:
        if hasattr(self, "label_field"):
            return getattr(self, self.label_field)
        else:
            return self.id

    def get_url(self) -> str:
        return f"/{self.call_sign.lower()}/{self.id}"

    def get_absolute_url(self) -> str:
        return f"https://hesperomys.com{self.get_url()}"

    @classmethod
    def select_for_field(cls, field: str | None) -> Any:
        if field is not None:
            field_obj = getattr(cls, field)
            return cls.select_valid().filter(field_obj != None)
        else:
            return cls.select_valid()

    def get_value_to_show_for_field(self, field: str | None) -> str:
        if field is None:
            return f"{self} ({self.get_url()})"
        return getattr(self, field)

    def s(self, **kwargs: Any) -> None:
        """Set attributes on the object.

        Use this in the shell instead of directly assigning properties because that does
        not automatically save the object. This is especially problematic if one does
        something like `Oryzomys.base_name.authority = 'Smith'`, because `Oryzomys.base_name`
        creates a temporary object that is immediately thrown away.

        """
        for name, value in kwargs.items():
            assert hasattr(self, name), f"Invalid attribute {name}"
            setattr(self, name, value)

    def __hash__(self) -> int:
        return self.id

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        assert isinstance(other, BaseModel)
        return self.id < other.id

    @classmethod
    def fields(cls) -> Iterable[str]:
        yield from cls.clirm_fields.keys()

    def short_description(self) -> str:
        """Used as the prompt when editing this object."""
        return str(self)

    def get_page_title(self) -> str:
        """Page title used in the web interface."""
        return str(self)

    def __str__(self) -> str:
        if hasattr(self, "label_field"):
            label = getattr(self, self.label_field)
            if isinstance(label, str):
                return label
        return BaseModel.__repr__(self)

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                f"{field}={getattr(self, field)}"
                for field in self.fields()
                if getattr(self, field) is not None
            ),
        )

    def _merge_fields(self, into: Self, exclude: Container[str] = set()) -> None:
        for field in self.fields():
            if field in exclude:
                continue
            my_data = getattr(self, field)
            into_data = getattr(into, field)
            if my_data is None or my_data == ():
                pass
            elif into_data is None or into_data == ():
                print(f"setting {field}: {my_data}")
                setattr(into, field, my_data)
            elif my_data != into_data:
                print(f"warning: dropping {field}: {my_data}")

    @classmethod
    def bfind(
        cls,
        *args: Any,
        quiet: bool = False,
        sort_key: Callable[[Self], Any] | None = None,
        sort: bool = True,
        **kwargs: Any,
    ) -> list[Self]:
        filters = [*args]
        fields = cls.clirm_fields
        for key, value in kwargs.items():
            if key not in fields:
                raise ValueError(f"{key} is not a valid field")
            field = fields[key]
            if isinstance(value, str):
                filters.append(field.contains(value))
            else:
                filters.append(field == value)
        objs = list(cls.select_valid().filter(*filters))
        if sort:
            if sort_key is None and hasattr(cls, "label_field"):
                sort_key = lambda obj: getattr(obj, cls.label_field) or ""
            if sort_key is not None:
                objs = sorted(objs, key=sort_key)
        if not quiet:
            if hasattr(cls, "label_field"):
                for obj in objs:
                    print(getattr(obj, cls.label_field))
            else:
                for obj in objs:
                    print(obj)
            print(f"{len(objs)} found")
        return objs

    @classmethod
    def select_one(cls, *args: Any, **kwargs: Any) -> Self | None:
        rows = cls.bfind(
            *args,
            *[getattr(cls, key) == value for key, value in kwargs.items()],
            quiet=True,
        )
        if len(rows) > 1:
            raise RuntimeError(f"Found multiple rows from {args}, {kwargs}")
        elif not rows:
            return None
        return rows[0]

    def reload(self) -> Self:
        return type(self).get(id=self.id)

    def serialize(self) -> int:
        return self.id

    @classmethod
    def unserialize(cls, data: int) -> Self:
        return cls(data)

    @classmethod
    def select_valid(cls) -> Query[Self]:
        """Subclasses may override this to filter out removed instances."""
        return cls.add_validity_check(cls.select())

    @classmethod
    def add_validity_check(cls, query: Query[Self]) -> Query[Self]:
        """Add a filter to the query that removes invalid objects."""
        return query

    def get_redirect_target(self) -> Self | None:
        """Return the object this object redirects to, if any."""
        return None

    def resolve_redirect(self) -> Self:
        if target := self.get_redirect_target():
            return target
        return self

    def is_invalid(self) -> bool:
        """If True, no valid object should have a reference to this object."""
        return False

    def should_skip(self) -> bool:
        return False

    @classmethod
    def getter(cls, attr: str | None) -> _NameGetter[Self]:
        key = (cls, attr)
        if key in _getters:
            return _getters[key]
        else:
            getter = _NameGetter(cls, attr)
            _getters[key] = getter
            return getter

    @classmethod
    def get_one_by(
        cls, field: str | None, *, prompt: str = "> ", allow_empty: bool = True
    ) -> Self | None:
        return cls.getter(field).get_one(prompt, allow_empty=allow_empty)

    def get_value_for_field(self, field: str, default: Any | None = None) -> Any:
        field_obj = getattr(type(self), field)
        prompt = f"{field}> "
        current_value = getattr(self, field)
        callbacks = self.get_adt_callbacks()
        if issubclass(field_obj.type_object, Model):
            return self.get_value_for_foreign_key_field(field, callbacks=callbacks)
        elif isinstance(field_obj, ADTField):

            def get_existing() -> list[getinput.ADTOrInstance]:
                return getattr(self, field) or []

            def set_existing(adts: Sequence[getinput.ADTOrInstance]) -> None:
                setattr(self, field, adts)

            return getinput.get_adt_list(
                field_obj.adt_type,
                completers=self.get_completers_for_adt_field(field),
                callbacks=callbacks,
                prompt=self.short_description(),
                get_existing=get_existing,
                set_existing=set_existing,
                member_callbacks=self.get_member_callbacks_for_adt_field(field),
            )
        elif field_obj.type_object is str:
            if default is None:
                default = "" if current_value is None else current_value
            if isinstance(field_obj, (TextField, TextOrNullField)):
                line = getinput.get_line(
                    prompt, default=default, mouse_support=True, callbacks=callbacks
                )
            else:
                line = self.getter(field).get_one_key(
                    prompt, default=default, callbacks=callbacks
                )
            return line or None
        elif issubclass(field_obj.type_object, enum.Enum):
            default = current_value
            if default is None and field in self.field_defaults:
                default = self.field_defaults[field]
            return getinput.get_enum_member(
                field_obj.type_object,
                prompt=prompt,
                default=default,
                callbacks=callbacks,
            )
        elif field_obj.type_object is int:
            default = "" if current_value is None else str(current_value)
            result = getinput.get_line(
                prompt, default=default, mouse_support=True, callbacks=callbacks
            )
            if result == "" or result is None:
                return None
            else:
                return int(result)
        elif field_obj.type_object is bool:
            return getinput.yes_no(prompt, default=current_value, callbacks=callbacks)
        else:
            raise ValueError(f"don't know how to fill {field}")

    @classmethod
    def get_value_for_field_on_class(
        cls,
        field: str,
        default: Any = "",
        *,
        callbacks: getinput.CallbackMap = {},
        prompt: str | None = None,
    ) -> Any:
        field_obj = getattr(cls, field)
        prompt = f"{field}> " if prompt is None else prompt
        if issubclass(field_obj.type_object, Model):
            return cls.get_value_for_foreign_key_field_on_class(
                field, callbacks=callbacks
            )
        elif isinstance(field_obj, ADTField):
            return getinput.get_adt_list(
                field_obj.adt_type,
                prompt=prompt,
                completers=cls.get_completers_for_adt_field(field),
                callbacks=callbacks,
                member_callbacks=cls.get_member_callbacks_for_adt_field(field),
            )
        elif isinstance(field_obj, (TextField, TextOrNullField)):
            return (
                getinput.get_line(
                    prompt, default=default, mouse_support=True, callbacks=callbacks
                )
                or None
            )
        elif field_obj.type_object is str:
            return (
                cls.getter(field).get_one_key(
                    prompt, default=default, callbacks=callbacks
                )
                or None
            )
        elif issubclass(field_obj.type_object, enum.Enum):
            if default == "":
                default = None
            if default is None and field in cls.field_defaults:
                default = cls.field_defaults[field]
            return getinput.get_enum_member(
                field_obj.type_object,
                prompt=prompt,
                default=default,
                callbacks=callbacks,
            )
        elif field_obj.type_object is int:
            result = getinput.get_line(
                prompt, default=default, mouse_support=True, callbacks=callbacks
            )
            if result == "" or result is None:
                return None
            else:
                return int(result)
        elif field_obj.type_object is bool:
            return getinput.yes_no(prompt, default=default, callbacks=callbacks)
        else:
            raise ValueError(f"don't know how to fill {field}")

    @classmethod
    def get_interactive_creators(cls) -> dict[str, Callable[[], Any]]:
        return {"n": cls.create_interactively}

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        def callback(field: str) -> Callable[[], None]:
            return lambda: self.fill_field(field)

        field_editors = {field: callback(field) for field in self.get_field_names()}

        return {
            **get_static_callbacks(),
            **field_editors,
            **self.get_tag_callbacks(),
            **self.get_shareable_adt_callbacks(),
            "d": self.display,
            "f": lambda: self.display(full=True),
            "foreign": self.edit_foreign,
            "sibling": self.edit_sibling,
            "sibling_by_field": self.edit_sibling_by_field,
            "empty": self.empty,
            "full_data": self.full_data,
            "call": self.call,
            "lint": self.format,
            "manual_lint": lambda: self.format(cfg=LintConfig(manual_mode=True)),
            "verbose_lint": lambda: self.format(cfg=LintConfig(verbose=True)),
            "print_character_names": self.print_character_names_for_field,
            "edit_reverse_rel": self.edit_reverse_rel,
            "lint_reverse_rel": self.lint_reverse_rel,
            "lint_and_fix": self.lint_and_fix,
            "lint_all_associated": self.lint_all_associated,
            "lint_all_associated_experimental": lambda: self.lint_all_associated(
                cfg=LintConfig(experimental=True)
            ),
            "edit_derived_field": self.edit_derived_field,
        }

    def get_shareable_adt_callbacks(self) -> getinput.CallbackMap:
        return {}

    def get_tag_callbacks(self) -> getinput.CallbackMap:
        def make_tag_callback(field: ADTField[Any], member: str) -> Callable[[], None]:
            def callback() -> None:
                constructor = getattr(field.adt_type, member)
                tag = getinput.get_adt_member(
                    constructor,
                    completers=self.get_completers_for_adt_field(field.name),
                )
                existing_value = getattr(self, field.name)
                setattr(self, field.name, (*existing_value, tag))

            return callback

        callbacks = {}
        for field in self.clirm_fields.values():
            if not isinstance(field, ADTField):
                continue
            for member in field.adt_type._members:
                callbacks[f"+{member.lower()}"] = make_tag_callback(field, member)
        return callbacks

    def call(self) -> None:
        """Call an arbitrary method interactively."""
        raw_options = {name: self._get_possible_callable(name) for name in dir(self)}
        options = {name: data for name, data in raw_options.items() if data is not None}
        name = getinput.get_with_completion(
            options, message="method to call> ", disallow_other=True
        )
        if not name:
            return
        obj, sig = options[name]
        _call_obj(obj, sig)

    def _get_possible_callable(
        self, name: str
    ) -> tuple[Callable[..., Any], inspect.Signature] | None:
        if name.startswith("_"):
            return None
        try:
            obj = getattr(self, name)
        except AttributeError:
            return None
        if not callable(obj):
            return None
        try:
            sig = inspect.signature(obj)
        except Exception:
            return None
        return obj, sig

    def edit_reverse_rel(self) -> None:
        options = [field.related_name for field in self.clirm_backrefs]
        chosen = getinput.choose_one_by_name(options)
        if chosen is None:
            return
        for obj in getattr(self, chosen):
            obj.display()
            try:
                obj.edit()
            except getinput.StopException:
                return

    def lint_reverse_rel(self) -> None:
        options = [field.related_name for field in self.clirm_backrefs]
        chosen = getinput.choose_one_by_name(options)
        if chosen is None:
            return
        for obj in getattr(self, chosen):
            obj.format(quiet=True)

    def lint_and_fix(self) -> None:
        options = [field.related_name for field in self.clirm_backrefs]
        chosen = getinput.choose_one_by_name(options)
        if chosen is None:
            return
        self.lint_object_list(getattr(self, chosen))

    def lint_all_associated(self, *, cfg: LintConfig = LintConfig()) -> None:
        for field in self.clirm_backrefs:
            if field.related_name is None:
                continue
            objs = list(getattr(self, field.related_name))
            self.lint_object_list(objs, cfg=cfg)

    def lint_object_list(
        self, objs: Iterable[BaseModel], cfg: LintConfig = LintConfig()
    ) -> None:
        for obj in objs:
            obj.format(quiet=True)
            if obj.is_lint_clean():
                continue
            obj.display()
            try:
                obj.edit_until_clean(cfg=cfg)
            except getinput.StopException:
                return

    def edit_derived_field(self) -> None:
        options = [field.name for field in self.derived_fields]
        chosen = getinput.choose_one_by_name(options)
        if chosen is None:
            return
        value = self.get_derived_field(chosen)
        if not isinstance(value, list):
            print(f"{value} is not a list")
            return
        for obj in value:
            obj.display()
            try:
                obj.edit()
            except getinput.StopException:
                return

    def edit_sibling(self) -> None:
        sibling = self.get_value_for_foreign_class(self.label_field, type(self))
        if sibling is not None:
            sibling.display()
            sibling.edit()

    def edit_sibling_by_field(self) -> None:
        field = self.prompt_for_field_name()
        if not field:
            return
        sibling = self.getter(field).get_one()
        if sibling is not None:
            sibling.display()
            sibling.edit()

    @classmethod
    def prompt_for_field_name(cls, prompt: str = "field> ") -> str | None:
        return getinput.get_with_completion(
            cls.get_field_names(),
            message=prompt,
            history_key=(cls, "edit_sibling_by_field"),
            disallow_other=True,
        )

    def print_character_names_for_field(self, field: str | None = None) -> None:
        if field is None:
            field = self.prompt_for_field_name()
        if not field:
            return
        value = getattr(self, field)
        if isinstance(value, str):
            helpers.print_character_names(value)

    def edit_foreign(self) -> None:
        options = {
            name: field
            for name, field in self.clirm_fields.items()
            if issubclass(field.type_object, Model)
        }
        chosen = getinput.get_with_completion(
            options,
            "field> ",
            history_key=(type(self), "edit_foreign"),
            disallow_other=True,
        )
        if not chosen:
            return
        value = getattr(self, chosen)
        if value is None:
            print(f"{self} has no {chosen}")
            return
        value.display()
        value.edit()

    def empty(self) -> None:
        chosen = self.prompt_for_field_name("field to empty> ")
        if not chosen:
            return
        value = getattr(self, chosen)
        if value is None:
            print(f"{self}: {chosen} is already None")
            return
        print(f"Current value: {value}")
        setattr(self, chosen, None)

    def edit(self) -> None:
        getinput.get_with_completion(
            options=[],
            message=f"{self}> ",
            disallow_other=True,
            callbacks=self.get_adt_callbacks(),
        )

    def edit_until_clean(
        self, *, initial_edit: bool = False, cfg: LintConfig = LintConfig()
    ) -> None:
        try:
            if initial_edit:
                self.edit()
            while not self.is_lint_clean(cfg=cfg):
                self.display()
                self.edit()
                self.reload()
                self.format(cfg=cfg)
        except getinput.StopException:
            pass

    @classmethod
    def get_completers_for_adt_field(cls, field: str) -> getinput.CompleterMap:
        field_obj = getattr(cls, field)
        assert isinstance(field_obj, ADTField)
        tag_cls = field_obj.adt_type
        completers: dict[tuple[type[adt.ADT], str], getinput.Completer[Any]] = {}
        for tag in tag_cls._tag_to_member.values():
            for attribute, typ in tag._attributes.items():
                if isinstance(typ, type) and issubclass(typ, BaseModel):
                    completer = get_completer(typ, None)
                    completers[(tag, attribute)] = completer
        return completers

    @classmethod
    def get_member_callbacks_for_adt_field(
        cls, field: str
    ) -> getinput.PerMemberCallbackMap:
        member_callbacks: dict[
            type[adt.ADT], dict[str, Callable[[Mapping[str, Any]], object]]
        ] = {}
        field_obj = getattr(cls, field)
        assert isinstance(field_obj, ADTField)
        tag_cls = field_obj.adt_type
        for tag in tag_cls._tag_to_member.values():
            if not tag._has_args:
                continue
            member_callbacks[tag] = {}
            if any(typ is models.Article for typ in tag._attributes.values()):

                def opener(args: Mapping[str, Any]) -> None:
                    for obj in args.values():
                        if isinstance(obj, models.Article):
                            obj.openf()

                member_callbacks[tag]["o"] = opener
            if any(
                isinstance(typ, type) and issubclass(typ, BaseModel)
                for typ in tag._attributes.values()
            ):
                options = [
                    attr
                    for attr, typ in tag._attributes.items()
                    if isinstance(typ, type) and issubclass(typ, BaseModel)
                ]

                def foreign(
                    args: Mapping[str, Any], *, options: Sequence[str] = options
                ) -> None:
                    field: str | None
                    if len(options) == 1:
                        print(f"Editing {options[0]}")
                        field = options[0]
                    else:
                        field = getinput.get_with_completion(
                            options,
                            message="field> ",
                            history_key=(tag_cls, "edit_foreign"),
                            disallow_other=True,
                        )
                    if not field:
                        return
                    if field not in args or args[field] is None:
                        print(f"{field} does not exist")
                        return
                    args[field].edit()

                member_callbacks[tag]["foreign"] = foreign
        return member_callbacks

    def get_value_for_foreign_key_field(
        self,
        field: str,
        *,
        default_obj: Any | None = None,
        callbacks: getinput.CallbackMap = {},
    ) -> Any:
        if default_obj is None:
            default_obj = getattr(self, field)
        return self.get_value_for_foreign_key_field_on_class(
            field, default_obj=default_obj, callbacks=callbacks
        )

    @classmethod
    def get_value_for_foreign_key_field_on_class(
        cls,
        field: str,
        *,
        default_obj: Any | None = None,
        callbacks: getinput.CallbackMap = {},
        allow_none: bool = True,
    ) -> Any:
        field_obj = getattr(cls, field)
        return cls.get_value_for_foreign_class(
            field,
            field_obj.type_object,
            default_obj=default_obj,
            callbacks=callbacks,
            allow_none=allow_none,
        )

    @staticmethod
    def get_value_for_foreign_class(
        label: str,
        foreign_cls: type[BaseModel],
        *,
        default_obj: Any | None = None,
        callbacks: getinput.CallbackMap = {},
        allow_none: bool = True,
    ) -> Any:
        if default_obj is None:
            default = ""
        else:
            default = getattr(default_obj, foreign_cls.label_field)
            if default is None:
                default = ""
        getter = foreign_cls.getter(None)
        creators = foreign_cls.get_interactive_creators()
        while True:
            value = getter.get_one_key(
                f"{label}> ",
                default=default,
                callbacks=callbacks,
                allow_empty=allow_none,
            )
            if value is None:
                return None
            elif value in creators:
                result = creators[value]()
                if result is not None:
                    print(f"created {foreign_cls} {result}")
                    return result
            else:
                try:
                    return getter(value)
                except foreign_cls.DoesNotExist:
                    if getinput.yes_no(
                        f"create new {foreign_cls.__name__} named {value}? "
                    ):
                        result = foreign_cls.create_interactively(
                            **{foreign_cls.label_field: value}
                        )
                        print(f"created new {foreign_cls} {result}")
                        return result
                    elif allow_none:
                        continue
                    else:
                        return None
        assert False, "should never get here"

    def fill_field(self, field: str) -> None:
        setattr(self, field, self.get_value_for_field(field))

    @classmethod
    def get_field_names(cls) -> list[str]:
        return [field for field in cls.clirm_fields if field != "id"]

    def get_required_fields(self) -> Iterable[str]:
        yield from self.get_field_names()

    def get_empty_required_fields(self) -> Iterable[str]:
        deprecated_fields = set(self.get_deprecated_fields())
        for field in self.get_required_fields():
            value = getattr(self, field)
            if field in deprecated_fields:
                if value is not None and value != ():
                    yield field
            elif value is None or value == ():
                yield field

    def get_deprecated_fields(self) -> Iterable[str]:
        return ()

    def get_nonempty_deprecated_fields(self) -> Iterable[str]:
        return (
            field
            for field in self.get_deprecated_fields()
            if getattr(self, field) is not None
        )

    def fill_required_fields(self, skip_fields: Container[str] = frozenset()) -> bool:
        """Edit all required fields that are empty. Returns whether any field was edited."""
        edited_any = False
        for field in self.get_empty_required_fields():
            if field not in skip_fields:
                self.fill_field(field)
                edited_any = True
        return edited_any

    def get_tags(
        self, tags: Sequence[adt.ADT] | None, tag_cls: type[ADTT] | ADTT
    ) -> Iterable[ADTT]:
        if tags is None:
            return
        for tag in tags:
            if isinstance(tag_cls, type):
                if isinstance(tag, tag_cls):
                    yield tag
            elif tag is tag_cls:
                yield tag

    def add_to_history(self, field: str | None = None) -> None:
        """Add this object to the history for its label field."""
        getters = [self.getter(field)]
        if field is None and hasattr(self, "label_field"):
            getters.append(self.getter(self.label_field))
        for getter in getters:
            getter.add_name(self)
            getinput.append_history(getter, self.get_value_to_show_for_field(field))

    def concise_markdown_link(self) -> str:
        return self.markdown_link()

    def markdown_link(self) -> str:
        return f"[{self!s}]({self.get_url()})"

    @classmethod
    def get_from_key(cls, key: str) -> Self | None:
        getter = cls.getter(None)
        try:
            return getter(key)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_interactively(cls, **kwargs: Any) -> Self | None:
        data = {**kwargs}
        for field in cls.fields():
            if field not in data and field != "id":
                try:
                    data[field] = cls.get_value_for_field_on_class(field)
                except getinput.StopException:
                    return None
        return cls.create(**data)

    @classmethod
    def create_many(cls) -> None:
        while True:
            obj = cls.create_interactively()
            if obj is None:
                break
            print(f"Created {cls.__name__}:")
            obj.full_data()
            print("==================================")
            obj.edit()


class ADTField(Field[Sequence[ADTT]]):
    _adt_type: type[adt.ADT]
    is_ordered: bool

    def __init__(self, name: str | None = None, *, is_ordered: bool = True) -> None:
        super().__init__(name)
        self.is_ordered = is_ordered

    @property
    def adt_type(self) -> type[adt.ADT]:
        self.resolve_type()
        return self._adt_type

    def deserialize(self, raw_value: Any) -> Sequence[ADTT]:
        if isinstance(raw_value, str) and raw_value:
            tags_list = []
            for val in json.loads(raw_value):
                try:
                    tags_list.append(self.adt_type.unserialize(val))
                except Exception:
                    traceback.print_exc()
                    print("Drop value", val)
            tags = tuple(tags_list)
            return tags
        else:
            return ()

    def serialize(self, value: Sequence[ADTT]) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            if value:
                return json.dumps([val.serialize() for val in value])
            else:
                return None
        elif value is None:
            return None
        raise TypeError(f"Unsupported type {value}")

    def get_resolved_type(self) -> tuple[Any, type[object], bool]:
        orig_class = self.__orig_class__
        (arg,) = typing.get_args(orig_class)
        if isinstance(arg, typing.ForwardRef):
            arg = self.resolve_forward_ref(arg)
        if not issubclass(arg, adt.ADT):
            raise TypeError(f"ADTField must be instantiated with an ADT, not {arg}")
        self._adt_type = arg
        return (Sequence[arg], Sequence, True)  # type: ignore[valid-type]


class TextField(Field[str]):
    """Indicates that long text is allowed."""


class TextOrNullField(Field[str | None]):
    """Indicates that long text is allowed but may be NULL."""


class _NameGetter(Generic[ModelT]):
    def __init__(self, cls: type[ModelT], field: str | None = None) -> None:
        self.cls = cls
        self.field = field
        self.field_obj = getattr(cls, field if field is not None else cls.label_field)
        self._data: set[str] | None = None
        self._encoded_data: set[str] | None = None
        if hasattr(cls, "creation_event"):
            cls.creation_event.on(self.add_name)
        if hasattr(cls, "save_event"):
            cls.save_event.on(self.add_name)

    def __repr__(self) -> str:
        return f"_NameGetter({self.cls}, {self.field})"

    def __dir__(self) -> set[str]:
        result = set(super().__dir__())
        self._warm_cache()
        assert self._encoded_data is not None
        return result | self._encoded_data

    def __getattr__(self, name: str) -> ModelT | None:
        return self._get_from_key(getinput.decode_name(name))

    def __call__(self, name: str | None = None) -> ModelT | None:
        if name is not None:
            return self._get_from_key(name)
        else:
            return self.cls.get_one_by(self.field)

    def __contains__(self, name: str) -> bool:
        self._warm_cache()
        assert self._data is not None
        return name in self._data

    def get_or_choose(self, name: str) -> ModelT:
        nams = list(self.cls.select_valid().filter(self.field_obj == name))
        count = len(nams)
        if count == 0:
            raise self.cls.DoesNotExist(name)
        elif count == 1:
            return nams[0]
        else:
            choice = getinput.choose_one(
                sorted(nams, key=lambda nam: nam.sort_key()),
                display_fn=lambda nam: f"{nam!r} (#{nam.id})",
                history_key=(self, name),
            )
            if choice is None:
                raise self.cls.DoesNotExist(name)
            return choice

    def clear_cache(self) -> None:
        self._data = None
        self._encoded_data = None
        key = self._cache_key()
        cached_data.clear(key)

    def rewarm_cache(self) -> None:
        self.clear_cache()
        self._warm_cache()

    def save_cache(self) -> None:
        if self._data is None:
            return
        key = self._cache_key()
        cached_data.set(key, pickle.dumps((self._data, self._encoded_data)))

    def add_name(self, nam: ModelT) -> None:
        if self._data is not None:
            self._add_obj(nam)

    def _cache_key(self) -> str:
        return f"{self.cls.call_sign}:{self.field}"

    def _add_obj(self, obj: ModelT) -> None:
        assert self._data is not None
        assert self._encoded_data is not None
        val = obj.get_value_to_show_for_field(self.field)
        if val is None:
            return
        val = str(val)
        if val == "":
            return
        self._data.add(val)
        self._encoded_data.add(getinput.encode_name(val))

    def get_one_key(
        self,
        prompt: str = "> ",
        *,
        default: str = "",
        callbacks: getinput.CallbackMap = {},
        allow_empty: bool = True,
        history_key: object = None,
    ) -> str | None:
        self._warm_cache()
        callbacks = {**callbacks, "clear_cache": self.rewarm_cache}
        key = getinput.get_with_lazy_completion(
            prompt,
            options_provider=self._get_data,
            is_valid=self.__contains__,
            default=default,
            history_key=self if history_key is None else history_key,
            callbacks=callbacks,
            allow_empty=allow_empty,
        )
        if key == "":
            return None
        return key

    def get_one(
        self,
        prompt: str = "> ",
        *,
        default: str = "",
        callbacks: getinput.CallbackMap = {},
        allow_empty: bool = True,
    ) -> ModelT | None:
        self._warm_cache()
        creators = self.cls.get_interactive_creators()
        callbacks = {**callbacks, "clear_cache": self.rewarm_cache}
        while True:
            key = getinput.get_with_lazy_completion(
                prompt,
                options_provider=self._get_data,
                is_valid=self.__contains__,
                default=default,
                history_key=self,
                callbacks=callbacks,
                allow_empty=allow_empty,
            )
            if not key:
                return None
            elif key in creators:
                result = creators[key]()
                if result is not None:
                    print(f"created {self.cls} {result}")
                    return result
            elif key == "e":
                try:
                    obj = self._get_from_key(default)
                except self.cls.DoesNotExist:
                    continue
                if obj is not None:
                    obj.edit()
                continue
            try:
                return self._get_from_key(key)
            except self.cls.DoesNotExist:
                print(f"{key!r} does not exist")
                continue
        assert False, "should never get here"

    def _get_data(self) -> set[str]:
        self._warm_cache()
        assert self._data is not None
        return self._data

    def _get_from_key(self, key: str) -> ModelT | None:
        if key.isnumeric():
            return self.cls.get(id=int(key))
        else:
            call_sign = self.cls.call_sign
            match = re.search(rf"/({call_sign.lower()}|{call_sign.upper()})/(\d+)", key)
            if match:
                oid = int(match.group(2))
                return self.cls.get(id=int(oid))
            return self.get_or_choose(key)

    def get_and_edit(self, prompt: str = "> ") -> None:
        while True:
            obj = self.get_one(prompt)
            if obj is None:
                return
            obj.display_concise()
            obj.edit()

    def get_all(self) -> list[str]:
        self._warm_cache()
        assert self._data is not None
        return sorted(self._data)

    def _warm_cache(self) -> None:
        if self._data is not None:
            return
        key = self._cache_key()
        data = cached_data.get(key)
        if data is not None:
            self._data, self._encoded_data = pickle.loads(data)
        else:
            self._data = set()
            self._encoded_data = set()
            for i, obj in enumerate(self.cls.select_for_field(self.field)):
                if i % 1000 == 0:
                    print(f"{self}: {i} done")
                self._add_obj(obj)
            self.save_cache()


def get_completer(
    cls: type[ModelT], field: str | None
) -> Callable[[str, str | None], ModelT | None]:
    def completer(prompt: str, default: Any) -> Any:
        if isinstance(default, BaseModel):
            default = str(default.id)
        elif default is None:
            default = ""
        elif not isinstance(default, str):
            raise TypeError(f"default must be str or Model, not {default!r}")
        return cls.getter(field).get_one(prompt, default=default)

    return completer


def get_str_completer(
    cls: type[Model], field: str | None
) -> Callable[[str, str | None], str | None]:
    def completer(prompt: str, default: str | None) -> Any:
        return cls.getter(field).get_one_key(prompt, default=default or "")

    return completer


def get_tag_based_derived_field(
    name: str,
    lazy_model_cls: Callable[[], type[BaseModel]],
    tag_field: str,
    lazy_tag_cls: Callable[[], type[adt.ADT]],
    field_index: int,
    *,
    skip_filter: bool = False,
) -> derived_data.DerivedField[list[Any]]:
    def compute_all() -> dict[int, list[BaseModel]]:
        model_cls = lazy_model_cls()
        out: dict[int, list[BaseModel]] = defaultdict(list)
        tag_id = lazy_tag_cls()._tag
        field_obj = getattr(model_cls, tag_field)
        if skip_filter:
            query = field_obj != None
        else:
            query = field_obj.contains(f"[{tag_id},")
        for obj in model_cls.select_valid().filter(query):
            for tag in obj.get_raw_tags_field(tag_field):
                if tag[0] == tag_id:
                    out[tag[field_index]].append(obj)
        return out

    return derived_data.DerivedField(
        name,
        derived_data.LazyType(lambda: list[lazy_model_cls()]),  # type: ignore[arg-type,misc]
        compute_all=compute_all,
        pull_on_miss=False,
    )


@functools.cache
def get_static_callbacks() -> getinput.CallbackMap:
    import taxonomy.lib
    import taxonomy.shell

    sibling_editors = {
        # At first I had this as "edit_{name}", but that was annoying
        # because it's a lot of typing before you get to the model.
        # Then I had "{name}_edit", but that created too many conflicts
        # where I ended up picking a location instead of adding a LocationDetail
        # tag. Keeping the name uppercase requires few keystrokes but is unique.
        cls.__name__: _make_editor(cls)
        for cls in BaseModel.__subclasses__()
        if hasattr(cls, "label_field")
    }
    commands = {
        f":{cmd.__name__}": partial(_call_obj, cmd, use_default=True)
        for cs in taxonomy.shell.COMMAND_SETS
        for cmd in cs.commands
    }
    flexible_commands = {
        f"::{cmd.__name__}": partial(_call_obj, cmd, use_default=False)
        for cs in taxonomy.shell.COMMAND_SETS
        for cmd in cs.commands
    }
    return {
        **sibling_editors,
        **commands,
        **flexible_commands,
        ":h": lambda: _call_obj(taxonomy.lib.h, use_default=True),
        ":hp": lambda: _call_obj(taxonomy.lib.hp, use_default=True),
        "RootName": lambda: models.Name.getter("root_name").get_and_edit(),
    }


def _make_editor(cls: type[BaseModel]) -> Callable[[], None]:
    return lambda: cls.getter(None).get_and_edit(f"{cls.__name__}> ")


def _call_obj(
    obj: Callable[..., Any],
    sig: inspect.Signature | None = None,
    *,
    use_default: bool = False,
) -> None:
    if sig is None:
        try:
            sig = inspect.signature(obj)
        except Exception:
            pass
    if hasattr(obj, "__doc__") and obj.__doc__ is not None:
        print(obj.__doc__)
    args = {}
    if sig is not None:
        for name, param in sig.parameters.items():
            try:
                args[name] = _fill_param(name, param, use_default=use_default)
            except getinput.StopException:
                return
    result = obj(**args)
    print("Result:", result)


def _fill_param(
    name: str, param: inspect.Parameter, *, use_default: bool = False
) -> object:
    if param.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    ):
        print(f"Cannot fill parameter {name} of kind {param.kind!r}")
        raise getinput.StopException
    typ = param.annotation
    is_optional = param.default is not inspect.Parameter.empty
    default_value = param.default
    if use_default and default_value is not inspect.Parameter.empty:
        return default_value
    if typing_inspect.is_optional_type(typ):
        args = typing_inspect.get_args(typ)
        if len(args) == 2 and isinstance(args[1], NoneType):
            typ = args[0]
            is_optional = True
            default_value = None

    value: object = None
    if typ is bool:
        value = getinput.yes_no(name)
    elif typ is str:
        value = getinput.get_line(name + "> ", allow_none=is_optional) or None
    elif typ is int:
        line = getinput.get_line(
            name + "> ",
            allow_none=is_optional,
            validate=lambda value: value == "" or value.isnumeric(),
        )
        if not line:
            return default_value
        return int(line)
    elif isinstance(typ, type):
        if issubclass(typ, enum.Enum):
            value = getinput.get_enum_member(typ, f"{name}> ", allow_empty=is_optional)
        elif issubclass(typ, BaseModel) and typ is not BaseModel:
            value = typ.getter(None).get_one(f"{name}> ", allow_empty=is_optional)
    if value is not None:
        return value
    if value is None and default_value is not inspect.Parameter.empty:
        return default_value
    print(f"Cannot fill parameter {param}")
    raise getinput.StopException
