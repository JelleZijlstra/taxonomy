from collections import Counter, defaultdict
import enum
import functools
from functools import partial
import inspect
import json
import re
import time
import traceback
from typing import (
    Any,
    Callable,
    ClassVar,
    Container,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
import typing_inspect

import peewee
from peewee import (
    BooleanField,
    CharField,
    ForeignKeyField,
    IntegerField,
    Model,
    MySQLDatabase,
    SqliteDatabase,
    TextField,
    FieldAccessor,
    ForeignKeyAccessor,
)

from ... import adt, config, events, getinput
from .. import derived_data, helpers

settings = config.get_options()

_log_path = settings.new_path / "log" / "queries.txt"
if _log_path.exists():
    _log_f = open(_log_path, "a", encoding="utf-8")
else:
    _log_f = None

if settings.use_sqlite:

    class LoggingDatabase(SqliteDatabase):
        def execute_sql(self, sql: str, *args: Any, **kwargs: Any) -> Any:
            if _log_f is None:
                return super().execute_sql(sql, *args, **kwargs)
            if sql.startswith("UPDATE"):
                _log_f.write(f"--- Write query: {sql}\n")
                traceback.print_stack(file=_log_f)
                _log_f.flush()
            start = time.time()
            try:
                return super().execute_sql(sql, *args, **kwargs)
            finally:
                end = time.time()
                if end - start > 0.1:
                    _log_f.write(f"{end - start:.03f}: {sql}\n")
                    _log_f.flush()

    database = LoggingDatabase(str(settings.db_filename))
else:
    database = MySQLDatabase(
        settings.db_name,
        user=settings.db_username,
        passwd=settings.db_password,
        charset="utf8",
    )
    database.get_conn().ping(True)


ModelT = TypeVar("ModelT", bound="BaseModel")
Linter = Callable[[ModelT, bool], Iterable[str]]
_getters: Dict[Tuple[Type[Model], Optional[str]], "_NameGetter[Any]"] = {}


class _FieldEditor(object):
    """For easily editing fields. This is exposed as object.e."""

    def __init__(self, instance: Any = None) -> None:
        self.instance = instance

    def __get__(self, instance: Any, instance_type: Any) -> "_FieldEditor":
        return self.__class__(instance)

    def __getattr__(self, field: str) -> None:
        if field == "all":
            self.instance.fill_required_fields()
        else:
            self.instance.fill_field(field)
        return None

    def __dir__(self) -> List[str]:
        return ["all"] + sorted(self.instance._meta.fields.keys())


def _descriptor_set(self: FieldAccessor, instance: Model, value: Any) -> None:
    """Monkeypatch the __set__ method on peewee descriptors to always save immediately.

    This is useful for us because in interactive use, it is easy to forget to call .save(), and
    we are not concerned about the performance implications of saving often.

    """
    has_old_value = self.name in instance.__data__
    old_value = instance.__data__.get(self.name)
    instance.__data__[self.name] = value
    # Otherwise this gets called in the constructor.
    if (not has_old_value or old_value != value) and getattr(
        instance, "_is_prepared", False
    ):
        instance._dirty.add(self.name)
        instance.save()


FieldAccessor.__set__ = _descriptor_set

_real_foreign_key_set = ForeignKeyAccessor.__set__


def _foreign_key_set(self: ForeignKeyAccessor, instance: Model, value: Any) -> None:
    """Same for ForeignKeyAccessor, which has its own __set__."""
    is_dirty = self.name in instance._dirty
    has_old_value = self.name in instance.__data__
    old_value = instance.__data__.get(self.name)
    _real_foreign_key_set(self, instance, value)
    if (not has_old_value or old_value != value) and getattr(
        instance, "_is_prepared", False
    ):
        instance._dirty.add(self.name)
        instance.save()
    elif not is_dirty:
        instance._dirty.discard(self.name)


ForeignKeyAccessor.__set__ = _foreign_key_set


class BaseModel(Model):
    label_field: str
    label_field_has_underscores = False
    # If given, lists are separated into groups based on this field.
    grouping_field: Optional[str] = None
    call_sign: str
    creation_event: events.Event[Any]
    save_event: events.Event[Any]
    field_defaults: Dict[str, Any] = {}
    excluded_fields: Set[str] = set()
    derived_fields: List["derived_data.DerivedField[Any]"] = []
    _name_to_derived_field: Dict[str, "derived_data.DerivedField[Any]"] = {}
    call_sign_to_model: ClassVar[Dict[str, Type["BaseModel"]]] = {}
    fields_may_be_invalid: ClassVar[set[str]] = set()
    markdown_fields: ClassVar[set[str]] = set()

    class Meta(object):
        database = database

    e = _FieldEditor()

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if hasattr(cls, "call_sign"):
            BaseModel.call_sign_to_model[cls.call_sign] = cls
        cls._name_to_derived_field = {field.name: field for field in cls.derived_fields}
        for field in cls.derived_fields:
            if field.typ is derived_data.SetLater:
                field.typ = cls

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._is_prepared = True

    @classmethod
    def create(cls: Type[ModelT], **kwargs: Any) -> ModelT:
        kwargs = {
            key: helpers.clean_string(value) if isinstance(value, str) else value
            for key, value in kwargs.items()
        }
        result = super().create(**kwargs)
        if hasattr(cls, "creation_event"):
            cls.creation_event.trigger(result)
        return result

    @classmethod
    def lint_all(
        cls: Type[ModelT],
        linter: Linter[ModelT] | None = None,
        *,
        autofix: bool = True,
        query: Iterable[ModelT] | None = None,
    ) -> List[Tuple[ModelT, List[str]]]:
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
            messages = list(linter(obj, autofix))
            if messages:
                for message in messages:
                    print(message)
                bad.append((obj, messages))
        return bad

    def lint_wrapper(self) -> bool:
        messages = list(self.general_lint())
        if not messages:
            print("Everything clean")
            return True
        for message in messages:
            print(message)
        return False

    def is_lint_clean(self) -> bool:
        messages = list(self.general_lint())
        if not messages:
            return True
        for message in messages:
            print(message)
        return False

    def general_lint(self, autofix: bool = True) -> Iterable[str]:
        unrenderable = list(self.check_renderable())
        if unrenderable:
            yield from unrenderable
            return
        yield from self.check_outbound_references(autofix)
        if self.is_invalid():
            yield from self.lint_invalid(autofix)
        else:
            yield from self.lint(autofix)

    def check_renderable(self) -> Iterable[str]:
        try:
            repr(self)
        except Exception as e:
            yield f"{self.id} ({type(self).__name__}): cannot repr() due to {e!r}"
        for field in self.fields():
            try:
                repr(getattr(self, field))
            except Exception as e:
                yield f"{self.id} ({type(self).__name__}): cannot get field {field} due to {e!r}"

    def check_outbound_references(self, autofix: bool = False) -> Iterable[str]:
        is_invalid = self.is_invalid()
        for field in self.fields():
            if field in self.fields_may_be_invalid:
                continue
            value = getattr(self, field)
            if value is None:
                continue
            field_obj = getattr(type(self), field)
            if isinstance(field_obj, ForeignKeyField):
                target = value.get_redirect_target()
                if target is not None:
                    message = f"{self}: references redirected object {value} -> {target} in field {field}"
                    if autofix:
                        print(message)
                        setattr(self, field, target)
                    else:
                        yield message
                # We don't care if invalid objects reference other invalid objects
                elif not is_invalid and value.is_invalid():
                    yield f"{self}: references invalid object {value} in field {field}"
            elif isinstance(field_obj, ADTField):
                if autofix:
                    new_tags = []
                    made_change = False
                    for tag in value:
                        tag_type = type(tag)
                        overrides = {}
                        for attr_name in tag_type._attributes:
                            value = getattr(tag, attr_name)
                            if not isinstance(value, BaseModel):
                                continue
                            target = value.get_redirect_target()
                            if target is not None:
                                print(
                                    f"{self}: references redirected object {value} -> {target}"
                                )
                                overrides[attr_name] = target
                            elif not is_invalid and value.is_invalid():
                                yield f"{self}: references invalid object {value} in {field} tag {tag}"
                        if overrides:
                            made_change = True
                            new_tags.append(adt.replace(tag, **overrides))
                        else:
                            new_tags.append(tag)
                    if made_change:
                        setattr(self, field, tuple(new_tags))
                else:
                    for tag in value:
                        tag_type = type(tag)
                        for attr_name in tag_type._attributes:
                            value = getattr(tag, attr_name)
                            if not isinstance(value, BaseModel):
                                continue
                            if not is_invalid and value.is_invalid():
                                yield f"{self}: references invalid object {value} in {field} tag {tag}"
        if is_invalid:
            target = self.get_redirect_target()
            if target is not None:
                secondary_target = target.get_redirect_target()
                if secondary_target is not None:
                    yield f"{self}: double redirect to {target} -> {secondary_target}"

    def lint(self, autofix: bool = True) -> Iterable[str]:
        """Yield messages if something is wrong with this object."""
        return []

    def lint_invalid(self, autofix: bool = True) -> Iterable[str]:
        """Like lint() but only called if is_invalid() returned True."""
        return []

    def prepared(self) -> None:
        super().prepared()
        self._is_prepared = True

    def save(self, *args: Any, **kwargs: Any) -> None:
        result = super().save(*args, **kwargs)
        if hasattr(self, "save_event"):
            self.save_event.trigger(self)
        return result

    def dump_data(self) -> str:
        return f"{self.__class__.__name__}({self.__dict__!r})"

    def full_data(self) -> None:
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

    def debug_data(self) -> None:
        self.full_data()
        print("Data:", self.__data__)
        print("Dict:", self.__dict__)

    def display(self, *, full: bool = False) -> None:
        """Print data about this object.

        Subclasses may use the full parameter to decide how much data to show.

        """
        self.full_data()

    def get_derived_field(self, name: str, force_recompute: bool = False) -> Any:
        return self._name_to_derived_field[name].get_value(
            self, force_recompute=force_recompute
        )

    def get_raw_derived_field(self, name: str, force_recompute: bool = False) -> Any:
        return self._name_to_derived_field[name].get_raw_value(
            self, force_recompute=force_recompute
        )

    def set_derived_field(self, name: str, value: Any) -> None:
        self._name_to_derived_field[name].set_value(self, value)

    def get_raw_tags_field(self, name: str) -> Any:
        data = self.__data__.get(name)
        if not data:
            return []
        return json.loads(data)

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
                if i > 0 and i % 100 == 0:
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

    @classmethod
    def select_for_field(cls, field: Optional[str]) -> Any:
        if field is not None:
            field_obj = getattr(cls, field)
            return cls.select_valid(field_obj).filter(field_obj != None)
        else:
            return cls.select_valid()

    def get_value_to_show_for_field(self, field: Optional[str]) -> str:
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
            assert hasattr(self, name), "Invalid attribute %s" % name
            setattr(self, name, value)

    def __hash__(self) -> int:
        return self.id

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id < other.id

    def __del__(self) -> None:
        if self.is_dirty():
            try:
                self.save()
            except peewee.IntegrityError:
                pass

    @classmethod
    def fields(cls) -> Iterable[str]:
        yield from cls._meta.fields.keys()

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
                "{}={}".format(field, getattr(self, field))
                for field in self.fields()
                if getattr(self, field) is not None
            ),
        )

    def _merge_fields(
        self: ModelT, into: ModelT, exclude: Container[peewee.Field] = set()
    ) -> None:
        for field in self.fields():
            if field in exclude:
                continue
            my_data = getattr(self, field)
            into_data = getattr(into, field)
            if my_data is None:
                pass
            elif into_data is None:
                print(f"setting {field}: {my_data}")
                setattr(into, field, my_data)
            elif my_data != into_data:
                print(f"warning: dropping {field}: {my_data}")

    @classmethod
    def mlist(cls, attribute: str) -> Dict[Any, int]:
        sql = f"""
            SELECT {attribute}, COUNT(*)
            FROM {cls._meta.db_table}
            GROUP BY {attribute}
        """
        return Counter(dict(database.execute_sql(sql)))

    @classmethod
    def bfind(
        cls: Type[ModelT],
        *args: Any,
        quiet: bool = False,
        sort_key: Optional[Callable[[ModelT], Any]] = None,
        sort: bool = True,
        **kwargs: Any,
    ) -> List[ModelT]:
        filters = [*args]
        fields = cls._meta.fields
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
    def select_one(cls: Type[ModelT], *args: Any, **kwargs: Any) -> Optional[ModelT]:
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

    def reload(self: ModelT) -> ModelT:
        return type(self).get(id=self.id)

    def serialize(self) -> int:
        return self.id

    @classmethod
    def unserialize(cls: Type[ModelT], data: int) -> ModelT:
        return cls.get(id=data)

    @classmethod
    def get_quick(cls: type[ModelT], data: int) -> ModelT:
        # TODO: This is actually slower, why? It seems to trigger more queries, maybe
        # peewee caches the instance?
        query, fields = get_query_and_fields(cls)
        cursor = database.execute_sql(query, (data,))
        kwargs = {
            field: value for field, value in zip(fields, cursor.fetchone(), strict=True)
        }
        return cls(**kwargs)

    @classmethod
    def select_valid(cls, *args: Any) -> Any:
        """Subclasses may override this to filter out removed instances."""
        return cls.add_validity_check(cls.select(*args))

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        """Add a filter to the query that removes invalid objects."""
        return query

    def get_redirect_target(self: ModelT) -> Optional[ModelT]:
        """Return the object this object redirects to, if any."""
        return None

    def is_invalid(self) -> bool:
        """If True, no valid object should have a reference to this object."""
        return False

    def should_skip(self) -> bool:
        return False

    @classmethod
    def getter(cls: Type[ModelT], attr: Optional[str]) -> "_NameGetter[ModelT]":
        key = (cls, attr)
        if key in _getters:
            return _getters[key]
        else:
            getter = _NameGetter(cls, attr)
            _getters[key] = getter
            return getter

    @classmethod
    def get_one_by(
        cls: Type[ModelT],
        field: Optional[str],
        *,
        prompt: str = "> ",
        allow_empty: bool = True,
    ) -> Optional[ModelT]:
        return cls.getter(field).get_one(prompt, allow_empty=allow_empty)

    def get_value_for_field(self, field: str, default: Optional[Any] = None) -> Any:
        field_obj = getattr(type(self), field)
        prompt = f"{field}> "
        current_value = getattr(self, field)
        callbacks = self.get_adt_callbacks()
        if isinstance(field_obj, ForeignKeyField):
            return self.get_value_for_foreign_key_field(field, callbacks=callbacks)
        elif isinstance(field_obj, ADTField):

            def get_existing() -> List[getinput.ADTOrInstance]:
                return getattr(self, field) or []

            def set_existing(adts: Sequence[getinput.ADTOrInstance]) -> None:
                setattr(self, field, adts)

            return getinput.get_adt_list(
                field_obj.get_adt(),
                completers=self.get_completers_for_adt_field(field),
                callbacks=callbacks,
                prompt=self.short_description(),
                get_existing=get_existing,
                set_existing=set_existing,
            )
        elif isinstance(field_obj, CharField):
            if default is None:
                default = "" if current_value is None else current_value
            return (
                self.getter(field).get_one_key(
                    prompt, default=default, callbacks=callbacks
                )
                or None
            )
        elif isinstance(field_obj, TextField):
            if default is None:
                default = "" if current_value is None else current_value
            return (
                getinput.get_line(
                    prompt, default=default, mouse_support=True, callbacks=callbacks
                )
                or None
            )
        elif isinstance(field_obj, EnumField):
            default = current_value
            if default is None and field in self.field_defaults:
                default = self.field_defaults[field]
            return getinput.get_enum_member(
                field_obj.enum_cls, prompt=prompt, default=default, callbacks=callbacks
            )
        elif isinstance(field_obj, IntegerField):
            default = "" if current_value is None else str(current_value)
            result = getinput.get_line(
                prompt, default=default, mouse_support=True, callbacks=callbacks
            )
            if result == "" or result is None:
                return None
            else:
                return int(result)
        elif isinstance(field_obj, BooleanField):
            return getinput.yes_no(prompt, default=current_value, callbacks=callbacks)
        else:
            raise ValueError(f"don't know how to fill {field}")

    @classmethod
    def get_interactive_creators(cls) -> Dict[str, Callable[[], Any]]:
        return {"n": cls.create_interactively}

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        def callback(field: str) -> Callable[[], None]:
            return lambda: self.fill_field(field)

        field_editors = {field: callback(field) for field in self.get_field_names()}
        return {
            **field_editors,
            "d": self.display,
            "f": lambda: self.display(full=True),
            "edit_foreign": self.edit_foreign,
            "edit_sibling": self.edit_sibling,
            "edit_sibling_by_field": self.edit_sibling_by_field,
            "empty": self.empty,
            "full_data": self.full_data,
            "debug": self.debug_data,
            "call": self.call,
            "lint": self.lint_wrapper,
        }

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
        if hasattr(obj, "__doc__"):
            print(obj.__doc__)
        args = {}
        for name, param in sig.parameters.items():
            try:
                args[name] = self._fill_param(name, param)
            except getinput.StopException:
                return None
        result = obj(**args)
        print("Result:", result)

    def _fill_param(self, name: str, param: inspect.Parameter) -> object:
        if param.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            print(f"Cannot fill parameter {name} of kind {param.kind!r}")
            raise getinput.StopException
        typ = param.annotation
        is_optional = False
        if typing_inspect.is_optional_type(typ):
            args = typing_inspect.get_args(typ)
            if len(args) == 2 and args[1] is type(None):
                typ = args[0]
                is_optional = True

        if typ is bool:
            return getinput.yes_no(name)
        elif typ is str:
            return getinput.get_line(name, allow_none=is_optional)
        elif typ is int:
            line = getinput.get_line(
                name,
                allow_none=is_optional,
                validate=lambda value: value.isnumeric(),
            )
            if line is None:
                return None
            return int(line)
        elif isinstance(typ, type):
            if issubclass(typ, enum.Enum):
                return getinput.get_enum_member(
                    typ, f"{name}> ", allow_empty=is_optional
                )
            elif issubclass(typ, BaseModel) and typ is not BaseModel:
                return typ.getter(None).get_one(f"{name}> ", allow_empty=is_optional)
        if param.default is not inspect.Parameter.empty:
            return param.default
        print(f"Cannot fill parameter {param}")
        raise getinput.StopException

    def _get_possible_callable(
        self, name: str
    ) -> Optional[Tuple[Callable[..., Any], inspect.Signature]]:
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

    def edit_sibling(self) -> None:
        sibling = self.get_value_for_foreign_class(self.label_field, type(self))
        if sibling is not None:
            sibling.display()
            sibling.edit()

    def edit_sibling_by_field(self) -> None:
        field = getinput.get_with_completion(
            self.get_field_names(),
            message="field> ",
            history_key=(type(self), "edit_sibling_by_field"),
            disallow_other=True,
        )
        if field is None:
            return
        sibling = self.getter(field).get_one()
        if sibling is not None:
            sibling.display()
            sibling.edit()

    def edit_foreign(self) -> None:
        options = {
            name: field
            for name, field in self._meta.fields.items()
            if isinstance(field, peewee.ForeignKeyField)
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
        chosen = getinput.get_with_completion(
            self._meta.fields,
            "field to empty> ",
            history_key=(type(self), "empty"),
            disallow_other=True,
        )
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

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        return {}

    def get_value_for_foreign_key_field(
        self,
        field: str,
        *,
        default_obj: Optional[Any] = None,
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
        default_obj: Optional[Any] = None,
        callbacks: getinput.CallbackMap = {},
        allow_none: bool = True,
    ) -> Any:
        field_obj = getattr(cls, field)
        return cls.get_value_for_foreign_class(
            field,
            field_obj.rel_model,
            default_obj=default_obj,
            callbacks=callbacks,
            allow_none=allow_none,
        )

    @staticmethod
    def get_value_for_foreign_class(
        label: str,
        foreign_cls: Type["BaseModel"],
        *,
        default_obj: Optional[Any] = None,
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

    def fill_field(self, field: str) -> None:
        setattr(self, field, self.get_value_for_field(field))

    @classmethod
    def get_field_names(cls) -> List[str]:
        return [field for field in cls._meta.fields.keys() if field != "id"]

    def get_required_fields(self) -> Iterable[str]:
        yield from self.get_field_names()

    def get_empty_required_fields(self) -> Iterable[str]:
        deprecated_fields = set(self.get_deprecated_fields())
        for field in self.get_required_fields():
            if field in deprecated_fields:
                if getattr(self, field) is not None:
                    yield field
            else:
                if getattr(self, field) is None:
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
        self, tags: Optional[Sequence[adt.ADT]], tag_cls: Type[adt.ADT]
    ) -> Iterable[adt.ADT]:
        if tags is None:
            return
        for tag in tags:
            if isinstance(tag, tag_cls):
                yield tag

    def add_to_history(self, field: Optional[str] = None) -> None:
        """Add this object to the history for its label field."""
        getter = self.getter(field)
        getinput.append_history(getter, self.get_value_to_show_for_field(field))

    @classmethod
    def create_interactively(cls: Type[ModelT], **kwargs: Any) -> Optional[ModelT]:
        obj = cls.create(**kwargs)
        obj.fill_required_fields()
        return obj


EnumT = TypeVar("EnumT", bound=enum.Enum)


class _EnumFieldDescriptor(FieldAccessor, Generic[EnumT]):
    def __init__(
        self,
        model: Type[BaseModel],
        field: peewee.Field,
        name: str,
        enum_cls: Type[EnumT],
    ) -> None:
        super().__init__(model, field, name)
        self.enum_cls = enum_cls

    def __get__(self, instance: Any, instance_type: Any = None) -> EnumT:
        value = super().__get__(instance, instance_type=instance_type)
        if isinstance(value, int):
            value = self.enum_cls(value)
        return value

    def __set__(self, instance: Any, value: Union[int, EnumT]) -> None:
        if isinstance(value, self.enum_cls):
            value = value.value
        super().__set__(instance, value)


class EnumField(IntegerField):
    def __init__(self, enum_cls: Type[enum.Enum], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.enum_cls = enum_cls
        self.accessor_class = partial(_EnumFieldDescriptor, enum_cls=enum_cls)


class _ADTDescriptor(FieldAccessor):
    def __init__(
        self, model: Type[BaseModel], field: peewee.Field, name: str, adt_cls: Any
    ) -> None:
        super().__init__(model, field, name)
        self.adt_cls = adt_cls
        self.field_name = name

    def __get__(self, instance: Any, instance_type: Any = None) -> Any:
        value = super().__get__(instance, instance_type=instance_type)
        if isinstance(value, str) and value:
            if not hasattr(instance, "_adt_cache"):
                instance._adt_cache = {}
            key = (self.field_name, value)
            if key in instance._adt_cache:
                return instance._adt_cache[key]
            if not isinstance(self.adt_cls, type):
                self.adt_cls = self.adt_cls()
            tags = tuple(self.adt_cls.unserialize(val) for val in json.loads(value))
            instance._adt_cache[key] = tags
            return tags
        else:
            return value

    def __set__(self, instance: Any, value: Any) -> None:
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            if value:
                value = json.dumps([val.serialize() for val in value])
            else:
                value = None
        super().__set__(instance, value)


class ADTField(TextField):
    def __init__(self, adt_cls: Callable[[], Type[Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.adt_cls = adt_cls
        self.accessor_class = partial(_ADTDescriptor, adt_cls=adt_cls)

    def add_to_class(self, model_class: Type[BaseModel], name: str) -> None:
        super().add_to_class(model_class, name)
        setattr(
            model_class, name, _ADTDescriptor(model_class, self, name, self.adt_cls)
        )
        setattr(model_class, f"_raw_{name}", FieldAccessor(model_class, self, name))

    def get_adt(self) -> Type[Any]:
        return self.adt_cls()


class _NameGetter(Generic[ModelT]):
    def __init__(self, cls: Type[ModelT], field: Optional[str] = None) -> None:
        self.cls = cls
        self.field = field
        self.field_obj = getattr(cls, field if field is not None else cls.label_field)
        self._data: Optional[Set[str]] = None
        self._encoded_data: Optional[Set[str]] = None
        if hasattr(cls, "creation_event"):
            cls.creation_event.on(self.add_name)
        if hasattr(cls, "save_event"):
            cls.save_event.on(self.add_name)

    def __repr__(self) -> str:
        return f"_NameGetter({self.cls}, {self.field})"

    def __dir__(self) -> Set[str]:
        result = set(super().__dir__())
        self._warm_cache()
        assert self._encoded_data is not None
        return result | self._encoded_data

    def __getattr__(self, name: str) -> Optional[ModelT]:
        return self._get_from_key(getinput.decode_name(name))

    def __call__(self, name: Optional[str] = None) -> Optional[ModelT]:
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
        cache = derived_data.load_cached_data()
        key = self._cache_key()
        cache.pop(key, None)

    def rewarm_cache(self) -> None:
        self.clear_cache()
        self._warm_cache()

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
    ) -> Optional[str]:
        self._warm_cache()
        callbacks = {**callbacks, "clear_cache": self.rewarm_cache}
        key = getinput.get_with_lazy_completion(
            prompt,
            options_provider=self._get_data,
            is_valid=self.__contains__,
            default=default,
            history_key=self,
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
    ) -> Optional[ModelT]:
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

    def _get_data(self) -> Set[str]:
        self._warm_cache()
        assert self._data is not None
        return self._data

    def _get_from_key(self, key: str) -> Optional[ModelT]:
        if key.isnumeric():
            return self.cls.get(id=int(key))
        else:
            call_sign = self.cls.call_sign
            match = re.search(rf"/({call_sign.lower()}|{call_sign.upper()})/(\d+)", key)
            if match:
                oid = int(match.group(2))
                return self.cls.get(id=int(oid))
            return self.get_or_choose(key)

    def get_and_edit(self) -> None:
        while True:
            obj = self.get_one()
            if obj is None:
                return
            obj.display()
            obj.edit()

    def get_all(self) -> List[str]:
        self._warm_cache()
        assert self._data is not None
        return sorted(self._data)

    def _warm_cache(self) -> None:
        if self._data is None:
            cache = derived_data.load_cached_data()
            key = self._cache_key()
            if key in cache:
                self._data, self._encoded_data = cache[key]
            else:
                self._data = set()
                self._encoded_data = set()
                for i, obj in enumerate(self.cls.select_for_field(self.field)):
                    if i % 1000 == 0:
                        print(f"{self}: {i} done")
                    self._add_obj(obj)
                cache[key] = (self._data, self._encoded_data)


def get_completer(
    cls: Type[ModelT], field: Optional[str]
) -> Callable[[str, Optional[str]], Optional[ModelT]]:
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
    cls: Type[Model], field: Optional[str]
) -> Callable[[str, Optional[str]], Optional[str]]:
    def completer(prompt: str, default: Optional[str]) -> Any:
        return cls.getter(field).get_one_key(prompt, default=default or "")

    return completer


def get_tag_based_derived_field(
    name: str,
    lazy_model_cls: Callable[[], Type[BaseModel]],
    tag_field: str,
    lazy_tag_cls: Callable[[], Type[adt.ADT]],
    field_index: int,
    skip_filter: bool = False,
) -> derived_data.DerivedField[List[Any]]:
    def compute_all() -> Dict[int, List[BaseModel]]:
        model_cls = lazy_model_cls()
        out: Dict[int, List[BaseModel]] = defaultdict(list)
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
        derived_data.LazyType(lambda: List[lazy_model_cls()]),  # type: ignore
        compute_all=compute_all,
        pull_on_miss=False,
    )


@functools.cache
def get_query_and_fields(cls: type[Model]) -> tuple[str, list[str]]:
    fields = [field.column_name for field in cls._meta.fields.values()]
    columns = ", ".join(f'"{field}"' for field in fields)
    query = f'SELECT {columns} FROM "{cls._meta.table_name}" WHERE ("{cls._meta.table_name}"."id" = ?)'
    return query, fields
