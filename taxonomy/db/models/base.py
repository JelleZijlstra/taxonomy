from collections import Counter
import enum
import json
import traceback
from typing import (
    Any,
    Callable,
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

import peewee
from peewee import (
    CharField,
    ForeignKeyField,
    IntegerField,
    Model,
    MySQLDatabase,
    SqliteDatabase,
    TextField,
)

from ... import adt, config, events, getinput

settings = config.get_options()

if settings.use_sqlite:
    database = SqliteDatabase(str(settings.db_filename))
else:
    database = MySQLDatabase(
        settings.db_name,
        user=settings.db_user,
        passwd=settings.db_password,
        charset="utf8",
    )
    database.get_conn().ping(True)


ModelT = TypeVar("ModelT", bound="BaseModel")
_getters: Dict[Tuple[Type[Model], str], "_NameGetter[Any]"] = {}


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


def _descriptor_set(self: peewee.FieldDescriptor, instance: Model, value: Any) -> None:
    """Monkeypatch the __set__ method on peewee descriptors to always save immediately.

    This is useful for us because in interactive use, it is easy to forget to call .save(), and
    we are not concerned about the performance implications of saving often.

    """
    instance._data[self.att_name] = value
    instance._dirty.add(self.att_name)
    # Otherwise this gets called in the constructor.
    if getattr(instance, "_is_prepared", False):
        instance.save()


peewee.FieldDescriptor.__set__ = _descriptor_set


class BaseModel(Model):
    label_field: str
    call_sign: str
    creation_event: events.Event[Any]
    save_event: events.Event[Any]
    field_defaults: Dict[str, Any] = {}

    class Meta(object):
        database = database

    e = _FieldEditor()

    @classmethod
    def create(cls: Type[ModelT], *args: Any, **kwargs: Any) -> ModelT:
        result = super().create(*args, **kwargs)
        if hasattr(cls, "creation_event"):
            cls.creation_event.trigger(result)
        return result

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
        self.save()

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
    def fields(cls) -> Iterable[peewee.Field]:
        for field in dir(cls):
            if isinstance(getattr(cls, field), peewee.Field):
                yield field

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
        into.save()

    @classmethod
    def mlist(cls, attribute: str) -> Dict[Any, int]:
        sql = f"""
            SELECT {attribute}, COUNT(*)
            FROM {cls._meta.db_table}
            GROUP BY {attribute}
        """
        return Counter(database.execute_sql(sql))

    @classmethod
    def bfind(
        cls: Type[ModelT], *args: Any, quiet: bool = False, **kwargs: Any
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
        objs = list(cls.filter(*filters))
        if not quiet:
            if hasattr(cls, "label_field"):
                objs = sorted(objs, key=lambda obj: getattr(obj, cls.label_field) or "")
                for obj in objs:
                    print(getattr(obj, cls.label_field))
            else:
                for obj in objs:
                    print(obj)
        return objs

    def reload(self: ModelT) -> ModelT:
        return type(self).get(id=self.id)

    def serialize(self) -> int:
        return self.id

    @classmethod
    def unserialize(cls: Type[ModelT], data: int) -> ModelT:
        return cls.get(id=data)

    @classmethod
    def select_valid(cls, *args: Any) -> Any:
        """Subclasses may override this to filter out removed instances."""
        return cls.select(*args)

    @classmethod
    def getter(cls: Type[ModelT], attr: str) -> "_NameGetter[ModelT]":
        key = (cls, attr)
        if key in _getters:
            return _getters[key]
        else:
            getter = _NameGetter(cls, attr)
            _getters[key] = getter
            return getter

    @classmethod
    def get_one_by(
        cls: Type[ModelT], attr: str, prompt: str = "> "
    ) -> Optional[ModelT]:
        return cls.getter(attr).get_one(prompt)

    def get_value_for_field(self, field: str) -> Any:
        field_obj = getattr(type(self), field)
        prompt = f"{field}> "
        current_value = getattr(self, field)
        callbacks = self.get_adt_callbacks()
        if isinstance(field_obj, ForeignKeyField):
            return self.get_value_for_foreign_key_field(field, callbacks=callbacks)
        elif isinstance(field_obj, ADTField):
            return getinput.get_adt_list(
                field_obj.get_adt(),
                existing=current_value,
                completers=self.get_completers_for_adt_field(field),
                callbacks=callbacks,
            )
        elif isinstance(field_obj, CharField):
            default = "" if current_value is None else current_value
            return (
                self.getter(field).get_one_key(
                    prompt, default=default, callbacks=callbacks
                )
                or None
            )
        elif isinstance(field_obj, TextField):
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
            default = "" if current_value is None else current_value
            result = getinput.get_line(
                prompt, default=default, mouse_support=True, callbacks=callbacks
            )
            if result == "" or result is None:
                return None
            else:
                return int(result)
        else:
            raise ValueError(f"don't know how to fill {field}")

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        def callback(field: str) -> Callable[[], None]:
            return lambda: self.fill_field(field)

        return {field: callback(field) for field in self.get_field_names()}

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        return {}

    def get_value_for_foreign_key_field(
        self,
        field: str,
        default: Optional[Any] = None,
        callbacks: getinput.CallbackMap = {},
    ) -> Any:
        if default is None:
            default = getattr(self, field)
        return self.get_value_for_foreign_key_field_on_class(
            field, default, callbacks=callbacks
        )

    @classmethod
    def get_value_for_foreign_key_field_on_class(
        cls,
        field: str,
        current_val: Optional[Any] = None,
        callbacks: getinput.CallbackMap = {},
    ) -> Any:
        field_obj = getattr(cls, field)
        return cls.get_value_for_foreign_class(
            field, field_obj.rel_model, current_val, callbacks=callbacks
        )

    @staticmethod
    def get_value_for_foreign_class(
        label: str,
        foreign_cls: Type["BaseModel"],
        default_obj: Optional[Any] = None,
        callbacks: getinput.CallbackMap = {},
    ) -> Any:
        if default_obj is None:
            default = ""
        else:
            default = getattr(default_obj, foreign_cls.label_field)
        getter = foreign_cls.getter(foreign_cls.label_field)
        value = getter.get_one_key(f"{label}> ", default=default, callbacks=callbacks)
        if value == "n":
            result = foreign_cls.create_interactively()
            print(f"created new {foreign_cls} {result}")
            return result
        elif value is None:
            return None
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
                else:
                    return None

    def fill_field(self, field: str) -> None:
        setattr(self, field, self.get_value_for_field(field))
        self.save()

    def get_field_names(self) -> List[str]:
        return [field for field in self._meta.fields.keys() if field != "id"]

    def get_required_fields(self) -> Iterable[str]:
        yield from self.get_field_names()

    def get_empty_required_fields(self) -> Iterable[str]:
        return (
            field
            for field in self.get_required_fields()
            if getattr(self, field) is None
        )

    def get_deprecated_fields(self) -> Iterable[str]:
        return ()

    def get_nonempty_deprecated_fields(self) -> Iterable[str]:
        return (
            field
            for field in self.get_deprecated_fields()
            if getattr(self, field) is not None
        )

    def fill_required_fields(self, skip_fields: Container[str] = frozenset()) -> None:
        for field in self.get_empty_required_fields():
            if field not in skip_fields:
                self.fill_field(field)

    def get_tag(
        self, tags: Optional[Sequence[adt.ADT]], tag_cls: Type[adt.ADT]
    ) -> Iterable[adt.ADT]:
        if tags is None:
            return
        for tag in tags:
            if isinstance(tag, tag_cls):
                yield tag

    @classmethod
    def create_interactively(cls: Type[ModelT], **kwargs: Any) -> ModelT:
        obj = cls.create(**kwargs)
        obj.fill_required_fields()
        return obj


EnumT = TypeVar("EnumT", bound=enum.Enum)


class _EnumFieldDescriptor(peewee.FieldDescriptor, Generic[EnumT]):
    def __init__(self, field: peewee.Field, enum_cls: Type[EnumT]) -> None:
        super().__init__(field)
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

    def add_to_class(self, model_class: Type[BaseModel], name: str) -> None:
        super().add_to_class(model_class, name)
        setattr(model_class, name, _EnumFieldDescriptor(self, self.enum_cls))


class _ADTDescriptor(peewee.FieldDescriptor):
    def __init__(self, field: peewee.Field, adt_cls: Any) -> None:
        super().__init__(field)
        self.adt_cls = adt_cls

    def __get__(self, instance: Any, instance_type: Any = None) -> Any:
        value = super().__get__(instance, instance_type=instance_type)
        if isinstance(value, str) and value:
            if not isinstance(self.adt_cls, type):
                self.adt_cls = self.adt_cls()
            return tuple(self.adt_cls.unserialize(val) for val in json.loads(value))
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

    def add_to_class(self, model_class: Type[BaseModel], name: str) -> None:
        super().add_to_class(model_class, name)
        setattr(model_class, name, _ADTDescriptor(self, self.adt_cls))
        setattr(model_class, f"_raw_{name}", peewee.FieldDescriptor(self))

    def get_adt(self) -> Type[Any]:
        return self.adt_cls()


class _NameGetter(Generic[ModelT]):
    def __init__(self, cls: Type[ModelT], field: str) -> None:
        self.cls = cls
        self.field = field
        self.field_obj = getattr(cls, field)
        self._data: Optional[Set[str]] = None
        self._encoded_data: Optional[Set[str]] = None
        if hasattr(cls, "creation_event"):
            cls.creation_event.on(self.add_name)
        if hasattr(cls, "save_event"):
            cls.save_event.on(self.add_name)

    def __dir__(self) -> Set[str]:
        result = set(super().__dir__())
        self._warm_cache()
        assert self._encoded_data is not None
        return result | self._encoded_data

    def __getattr__(self, name: str) -> ModelT:
        return self.get_or_choose(getinput.decode_name(name))

    def __call__(self, name: Optional[str] = None) -> ModelT:
        if name is not None:
            return self.get_or_choose(name)
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
            for i, nam in enumerate(nams):
                print(f"{i}: {nam} (#{nam.id})")
            choices = [str(i) for i in range(len(nams))]
            choice = getinput.get_with_completion(
                options=choices,
                message="Choose one: ",
                disallow_other=True,
                history_key=(self, name),
            )
            if choice == "":
                raise self.cls.DoesNotExist(name)
            idx = int(choice)
            return nams[idx]

    def clear_cache(self) -> None:
        self._data = None
        self._encoded_data = None

    def add_name(self, nam: ModelT) -> None:
        if self._data is not None:
            self._add_obj(nam)

    def _add_obj(self, obj: ModelT) -> None:
        assert self._data is not None
        assert self._encoded_data is not None
        val = getattr(obj, self.field)
        if val is None:
            return
        val = str(val)
        self._data.add(val)
        self._encoded_data.add(getinput.encode_name(val))

    def get_one_key(
        self,
        prompt: str = "> ",
        default: str = "",
        callbacks: getinput.CallbackMap = {},
    ) -> Optional[str]:
        self._warm_cache()
        assert self._data is not None
        key = getinput.get_with_completion(
            self._data, prompt, default=default, history_key=self, callbacks=callbacks
        )
        if key == "":
            return None
        return key

    def get_one(
        self,
        prompt: str = "> ",
        default: str = "",
        callbacks: getinput.CallbackMap = {},
    ) -> Optional[ModelT]:
        self._warm_cache()
        assert self._data is not None
        key = getinput.get_with_completion(
            self._data, prompt, default=default, history_key=self, callbacks=callbacks
        )
        if key == "":
            return None
        elif key.isnumeric():
            val = int(key)
            return self.cls.get(id=val)
        return self.get_or_choose(key)

    def _warm_cache(self) -> None:
        if self._data is None:
            self._data = set()
            self._encoded_data = set()
            for obj in self.cls.select(self.field_obj):
                self._add_obj(obj)
