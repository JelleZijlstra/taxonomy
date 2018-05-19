import collections
import datetime
import enum
import json
import operator
import re
import sys
import time
import traceback
from typing import (
    IO,
    Any,
    Callable,
    Container,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

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
)

from . import constants, definition, ehphp, helpers, settings
from .. import adt, events, getinput
from .constants import (
    GenderArticle,
    Group,
    NomenclatureStatus,
    OccurrenceStatus,
    Rank,
    SourceLanguage,
    SpeciesNameKind,
    Status,
)
from .definition import Definition

if settings.use_sqlite:
    database = SqliteDatabase(settings.database_file)
else:
    database = MySQLDatabase(
        settings.DATABASE, user=settings.USER, passwd=settings.PASSWD, charset="utf8"
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
        print(f"saving {instance} to set {self.att_name} to {value}")
        instance.save()


peewee.FieldDescriptor.__set__ = _descriptor_set


class BaseModel(Model):
    label_field: str
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
        return "%s(%r)" % (self.__class__.__name__, self.__dict__)

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
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(
                "%s=%s" % (field, getattr(self, field))
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
                print("setting %s: %s" % (field, my_data))
                setattr(into, field, my_data)
            elif my_data != into_data:
                print("warning: dropping %s: %s" % (field, my_data))
        into.save()

    @classmethod
    def mlist(cls, attribute: str) -> Dict[Any, int]:
        sql = f"""
            SELECT {attribute}, COUNT(*)
            FROM {cls._meta.db_table}
            GROUP BY {attribute}
        """
        return dict(database.execute_sql(sql))

    def serialize(self) -> int:
        return self.id

    @classmethod
    def unserialize(cls: Type[ModelT], data: int) -> ModelT:
        return cls.get(id=data)

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
        if isinstance(field_obj, ForeignKeyField):
            return self.get_value_for_foreign_key_field(field)
        elif isinstance(field_obj, ADTField):
            return getinput.get_adt_list(
                field_obj.get_adt(),
                existing=current_value,
                completers=self.get_completers_for_adt_field(field),
            )
        elif isinstance(field_obj, CharField):
            default = "" if current_value is None else current_value
            return self.getter(field).get_one_key(prompt, default=default) or None
        elif isinstance(field_obj, TextField):
            default = "" if current_value is None else current_value
            return (
                getinput.get_line(prompt, default=default, mouse_support=True) or None
            )
        elif isinstance(field_obj, EnumField):
            default = current_value
            if default is None and field in self.field_defaults:
                default = self.field_defaults[field]
            return getinput.get_enum_member(
                field_obj.enum_cls, prompt=prompt, default=default
            )
        elif isinstance(field_obj, IntegerField):
            default = "" if current_value is None else current_value
            result = getinput.get_line(prompt, default=default, mouse_support=True)
            if result == "" or result is None:
                return None
            else:
                return int(result)
        else:
            raise ValueError(f"don't know how to fill {field}")

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        return {}

    def get_value_for_foreign_key_field(self, field: str) -> Any:
        current_val = getattr(self, field)
        return self.get_value_for_foreign_key_field_on_class(field, current_val)

    @classmethod
    def get_value_for_foreign_key_field_on_class(
        cls, field: str, current_val: Optional[Any] = None
    ) -> Any:
        field_obj = getattr(cls, field)
        foreign_cls = field_obj.rel_model
        if current_val is None:
            default = ""
        else:
            default = getattr(current_val, foreign_cls.label_field)
        getter = foreign_cls.getter(foreign_cls.label_field)
        value = getter.get_one_key(f"{field}> ", default=default)
        if value == "n":
            result = foreign_cls.create_interactively()
            print(f"created new {foreign_cls} {result}")
            return result
        elif value is None:
            return None
        else:
            return getter(value)

    @staticmethod
    def get_value_for_article_field(
        field: str, default: Optional[str] = None
    ) -> Optional[str]:
        names = ehphp.call_ehphp("get_all", {})
        return (
            getinput.get_with_completion(names, f"{field}> ", default=default or "")
            or None
        )

    def fill_field(self, field: str) -> None:
        setattr(self, field, self.get_value_for_field(field))
        self.save()

    def get_required_fields(self) -> Iterable[str]:
        return (field for field in self._meta.fields.keys() if field != "id")

    def get_empty_required_fields(self) -> Iterable[str]:
        return (
            field
            for field in self.get_required_fields()
            if getattr(self, field) is None
        )

    def fill_required_fields(self) -> None:
        for field in self.get_empty_required_fields():
            self.fill_field(field)

    @classmethod
    def create_interactively(cls: Type[ModelT]) -> ModelT:
        obj = cls.create()
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

    def __get__(self, instance: Any, instance_type: Any = None) -> EnumT:
        value = super().__get__(instance, instance_type=instance_type)
        if isinstance(value, str):
            if not isinstance(self.adt_cls, type):
                self.adt_cls = self.adt_cls()
            value = tuple(self.adt_cls.unserialize(val) for val in json.loads(value))
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
        return self.cls.get(self.field_obj == getinput.decode_name(name))

    def __call__(self, name: str) -> ModelT:
        return self.cls.get(self.field_obj == name)

    def __contains__(self, name: str) -> bool:
        self._warm_cache()
        assert self._data is not None
        return name in self._data

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

    def get_one_key(self, prompt: str = "> ", default: str = "") -> Optional[str]:
        self._warm_cache()
        assert self._data is not None
        key = getinput.get_with_completion(self._data, prompt, default=default)
        if key == "":
            return None
        return key

    def get_one(self, prompt: str = "> ", default: str = "") -> Optional[ModelT]:
        self._warm_cache()
        assert self._data is not None
        key = getinput.get_with_completion(self._data, prompt, default=default)
        if key == "":
            return None
        return getattr(self, getinput.encode_name(key))

    def _warm_cache(self) -> None:
        if self._data is None:
            self._data = set()
            self._encoded_data = set()
            for obj in self.cls.select(self.field_obj):
                self._add_obj(obj)


class _OccurrenceGetter(object):
    """For easily accessing occurrences of a taxon.

    This is exposed at taxon.at. You can access taxa as either taxon.at.Locality_Name or taxon.at(L.Locality_Name).

    """

    def __init__(self, instance: Any = None) -> None:
        self.instance = instance

    def __get__(self, instance: Any, instance_type: Any) -> "_OccurrenceGetter":
        return self.__class__(instance)

    def __getattr__(self, loc_name: str) -> "Occurrence":
        return self(Location.get(Location.name == loc_name.replace("_", " ")))

    def __call__(self, loc: "Location") -> "Occurrence":
        return self.instance.occurrences.filter(Occurrence.location == loc).get()

    def __dir__(self) -> List[str]:
        return [o.location.name.replace(" ", "_") for o in self.instance.occurrences]


class Taxon(BaseModel):
    creation_event = events.Event["Taxon"]()
    save_event = events.Event["Taxon"]()
    label_field = "valid_name"

    rank = EnumField(Rank)
    valid_name = CharField(default="")
    age = EnumField(constants.Age)
    parent = ForeignKeyField(
        "self", related_name="children", null=True, db_column="parent_id"
    )
    data = TextField(null=True)
    is_page_root = BooleanField(default=False)
    _base_name_id = IntegerField(null=True, db_column="base_name_id")

    class Meta(object):
        db_table = "taxon"

    name = property(lambda self: self.base_name)

    @property
    def base_name(self) -> "Name":
        try:
            return Name.get(Name.id == self._base_name_id)
        except Name.DoesNotExist:
            return None  # type: ignore  # too annoying to actually deal with this

    @base_name.setter
    def base_name(self, value: "Name") -> None:
        self._base_name_id = value.id
        Taxon.update(_base_name_id=value.id).where(Taxon.id == self.id).execute()
        self.save()

    def group(self) -> Group:
        return helpers.group_of_rank(self.rank)

    def sorted_names(self, exclude_valid: bool = False) -> List["Name"]:
        names: Iterable[Name] = self.names
        if exclude_valid:
            names = filter(lambda name: name.status != Status.valid, names)
        return sorted(names, key=operator.attrgetter("status", "root_name"))

    def sorted_children(self) -> List["Taxon"]:
        return sorted(self.children, key=operator.attrgetter("rank", "valid_name"))

    def sorted_occurrences(self) -> List["Occurrence"]:
        return sorted(self.occurrences, key=lambda o: o.location.name)

    def root_name(self) -> str:
        return self.valid_name.split(" ")[-1]

    def all_data(self) -> None:
        self.full_data()
        self.base_name.full_data()
        print(self.sorted_names())
        print(self.sorted_children())
        print(self.sorted_occurrences())

    def full_name(self) -> str:
        if self.rank == Rank.subgenus:
            return self.parent.valid_name + " (" + self.valid_name + ")"
        if self.rank == Rank.species_group:
            return self.parent.full_name() + " (" + self.base_name.root_name + ")"
        elif self.rank == Rank.species:
            # For nomina dubia and species inquirendae, retain the name as given.
            if self.parent.rank > Rank.genus or self.base_name.status != Status.valid:
                return self.valid_name
            parent_name = self.parent.full_name()
            if self.parent.needs_is():
                parent_name += " (?)"
            return parent_name + " " + self.base_name.root_name
        elif self.rank == Rank.subspecies:
            return self.parent.full_name() + " " + self.base_name.root_name
        else:
            return self.valid_name

    def needs_is(self) -> bool:
        if not hasattr(self, "_needs_is"):
            if self.rank == Rank.subgenus:
                self._needs_is = (
                    Taxon.select()
                    .where(Taxon.parent == self, Taxon.rank == Rank.species_group)
                    .count()
                    > 0
                )
            elif self.rank == Rank.genus:
                self._needs_is = (
                    Taxon.select()
                    .where(
                        Taxon.parent == self,
                        (Taxon.rank == Rank.subgenus)
                        | (Taxon.rank == Rank.species_group),
                    )
                    .count()
                    > 0
                )
            else:
                self._needs_is = False
        return self._needs_is

    def parent_of_rank(
        self, rank: Rank, original_taxon: Optional["Taxon"] = None
    ) -> "Taxon":
        if original_taxon is None:
            original_taxon = self
        if self.rank > rank and self.rank != Rank.unranked:
            raise ValueError(
                "%s (id = %s) has no ancestor of rank %s"
                % (original_taxon, original_taxon.id, rank.name)
            )
        elif self.rank == rank:
            return self
        else:
            return self.parent.parent_of_rank(rank, original_taxon=original_taxon)

    def has_parent_of_rank(self, rank: Rank) -> bool:
        try:
            self.parent_of_rank(rank)
        except ValueError:
            return False
        else:
            return True

    def is_child_of(self, taxon: "Taxon") -> bool:
        if self == taxon:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.is_child_of(taxon)

    def children_of_rank(
        self, rank: Rank, age: Optional[constants.Age] = None
    ) -> List["Taxon"]:
        if self.rank < rank:
            return []
        elif self.rank == rank:
            if age is None or self.age == age:
                return [self]
            else:
                return []
        else:
            out: List[Taxon] = []
            for child in self.children:
                out += child.children_of_rank(rank, age=age)
            return out

    def find_names(
        self, root_name: str, group: Optional[Group] = None, fuzzy: bool = True
    ) -> List["Taxon"]:
        """Find instances of the given root_name within the given container taxon."""
        if fuzzy:
            query = Name.root_name % root_name  # LIKE
        else:
            query = Name.root_name == root_name
        candidates = Name.filter(query)

        result = []
        # maybe I could do some internal caching here but for now this is fast enough
        for candidate in candidates:
            if group is not None and candidate.group != group:
                continue
            taxon = candidate.taxon
            while taxon.parent is not None:
                if taxon.id == self.id:
                    result.append(candidate)
                    break
                taxon = taxon.parent
        return result

    def display_extant(self) -> None:
        self.display(
            exclude_fn=lambda t: t.age != constants.Age.extant,
            name_exclude_fn=lambda n: n.status == Status.synonym,
        )

    def display(
        self,
        full: bool = False,
        max_depth: Optional[int] = None,
        file: IO[str] = sys.stdout,
        depth: int = 0,
        exclude: Container["Taxon"] = set(),
        exclude_fn: Optional[Callable[["Taxon"], bool]] = None,
        name_exclude_fn: Optional[Callable[["Name"], bool]] = None,
        show_occurrences: Optional[bool] = None,
    ) -> None:
        if show_occurrences is None:
            show_occurrences = full
        if exclude_fn is not None and exclude_fn(self):
            return
        file.write(" " * (4 * depth))
        file.write(f"{self.rank.name} {self.age.get_symbol()}{self.full_name()}\n")
        if full:
            data = {"data": self.data, "is_page_root": self.is_page_root}
            for key, value in data.items():
                if value:
                    file.write(" " * ((depth + 1) * 4))
                    file.write("%s: %s\n" % (key, value))
        for name in self.sorted_names():
            if name_exclude_fn is None or not name_exclude_fn(name):
                file.write(name.get_description(depth=depth + 1, full=full))
        if show_occurrences:
            for occurrence in self.sorted_occurrences():
                file.write(" " * ((depth + 1) * 4))
                file.write("%s\n" % (occurrence if full else occurrence.location))
        if self in exclude:
            return
        if max_depth is None or max_depth > 0:
            new_max_depth = None if max_depth is None else max_depth - 1
            for child in self.sorted_children():
                child.display(
                    file=file,
                    depth=depth + 1,
                    max_depth=new_max_depth,
                    full=full,
                    exclude=exclude,
                    exclude_fn=exclude_fn,
                    name_exclude_fn=name_exclude_fn,
                    show_occurrences=show_occurrences,
                )

    def display_parents(
        self, max_depth: Optional[int] = None, file: IO[str] = sys.stdout
    ) -> None:
        if max_depth == 0:
            return
        if max_depth is not None:
            max_depth -= 1
        if self.parent is not None:
            self.parent.display_parents(max_depth=max_depth, file=file)

        file.write("%s %s (%s)\n" % (self.rank.name, self.full_name(), self.age.name))
        file.write(self.base_name.get_description(depth=1))

    def ranked_parents(self) -> Tuple[Optional["Taxon"], Optional["Taxon"]]:
        """Returns the order-level and family-level parents of the taxon.

        The family-level parent is the one parent of family rank. The order-level parent
        is of rank order if there is one, and otherwise the first unranked taxon above the
        highest-ranked family-group taxon.

        """
        family_rank = None
        order_rank = None
        current_parent = self
        while current_parent is not None:
            parent_rank = current_parent.rank
            if parent_rank == Rank.family:
                family_rank = current_parent
            if helpers.group_of_rank(parent_rank) == Group.family:
                order_rank = None
            if parent_rank == Rank.order:
                order_rank = current_parent
                break
            if parent_rank == Rank.unranked and order_rank is None:
                order_rank = current_parent
            if parent_rank > Rank.order and parent_rank != Rank.unranked:
                break

            current_parent = current_parent.parent
        return order_rank, family_rank

    def add_static(
        self,
        rank: Rank,
        name: str,
        authority: Optional[str] = None,
        year: Union[None, str, int] = None,
        age: Optional[constants.Age] = None,
        **kwargs: Any,
    ) -> "Taxon":
        if age is None:
            age = self.age
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self)
        kwargs["group"] = helpers.group_of_rank(rank)
        kwargs["root_name"] = helpers.root_name_of_name(name, rank)
        if "status" not in kwargs:
            kwargs["status"] = Status.valid
        name_obj = Name.create(taxon=taxon, **kwargs)
        if authority is not None:
            name_obj.authority = authority
        if year is not None:
            name_obj.year = year
        name_obj.save()
        taxon.base_name = name_obj
        taxon.save()
        return taxon

    def add(self) -> "Taxon":
        rank = getinput.get_enum_member(
            Rank, default=Rank.genus if self.rank > Rank.genus else Rank.species
        )
        name = getinput.get_line("name> ", allow_none=False)
        assert name is not None
        age = getinput.get_enum_member(constants.Age, default=self.age)
        status = getinput.get_enum_member(Status, default=Status.valid)
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self)
        name_obj = Name.create(
            taxon=taxon,
            group=helpers.group_of_rank(rank),
            root_name=helpers.root_name_of_name(name, rank),
            status=status,
            nomenclature_status=NomenclatureStatus.available,
        )
        taxon.base_name = name_obj
        taxon.save()
        name_obj.fill_required_fields()
        return taxon

    def add_syn(
        self,
        root_name: str,
        authority: Optional[str] = None,
        year: Union[None, int, str] = None,
        original_name: Optional[str] = None,
        original_citation: Optional[str] = None,
        page_described: Union[None, int, str] = None,
        status: Status = Status.synonym,
        nomenclature_status: NomenclatureStatus = NomenclatureStatus.available,
        interactive: bool = True,
        **kwargs: Any,
    ) -> "Name":
        kwargs["root_name"] = root_name
        kwargs["authority"] = authority
        kwargs["year"] = year
        # included in the method signature so they autocomplete in shell
        kwargs["original_name"] = original_name
        kwargs["original_citation"] = original_citation
        kwargs["page_described"] = page_described
        kwargs["status"] = status
        kwargs["taxon"] = self
        kwargs["nomenclature_status"] = nomenclature_status
        if "group" not in kwargs:
            kwargs["group"] = self.base_name.group
        name = Name.create(**kwargs)
        if interactive:
            name.fill_required_fields()
        return name

    def add_type_identical(
        self,
        name: str,
        page_described: Union[None, int, str] = None,
        locality: Optional["Location"] = None,
        **kwargs: Any,
    ) -> "Taxon":
        """Convenience method to add a type species described in the same paper as the genus."""
        assert self.rank == Rank.genus
        assert self.base_name.type is None
        full_name = "%s %s" % (self.valid_name, name)
        if isinstance(page_described, int):
            page_described = str(page_described)
        result = self.add_static(
            Rank.species,
            full_name,
            authority=self.base_name.authority,
            year=self.base_name.year,
            original_citation=self.base_name.original_citation,
            original_name=full_name,
            page_described=page_described,
            status=self.base_name.status,
        )
        self.base_name.type = result.base_name
        self.base_name.save()
        if locality is not None:
            result.add_occurrence(locality)
        result.base_name.s(**kwargs)
        if self.base_name.original_citation is not None:
            self.base_name.fill_required_fields()
            result.base_name.fill_required_fields()
        return result

    def switch_basename(self, name: "Name") -> None:
        assert name.taxon == self, f"{name} is not a synonym of {self}"
        old_base = self.base_name
        name.status = Status.valid
        old_base.status = Status.synonym
        self.base_name = name
        self.recompute_name()

    def add_occurrence(
        self,
        location: "Location",
        paper: Optional[str] = None,
        comment: Optional[str] = None,
        status: OccurrenceStatus = OccurrenceStatus.valid,
    ) -> "Occurrence":
        if paper is None:
            paper = self.base_name.original_citation
        try:
            return Occurrence.create(
                taxon=self,
                location=location,
                source=paper,
                comment=comment,
                status=status,
            )
        except peewee.IntegrityError:
            print("DUPLICATE OCCURRENCE")
            return self.at(location)

    def syn_from_paper(
        self,
        root_name: str,
        paper: str,
        page_described: Union[None, int, str] = None,
        status: Status = Status.synonym,
        group: Optional[Group] = None,
        age: Optional[constants.Age] = None,
        interactive: bool = True,
        **kwargs: Any,
    ) -> "Name":
        authority, year = ehphp.call_ehphp("taxonomicAuthority", [paper])[0]
        result = self.add_syn(
            root_name=root_name,
            authority=authority,
            year=year,
            original_citation=paper,
            page_described=page_described,
            status=status,
            age=age,
            interactive=False,
        )
        if group is not None:
            kwargs["group"] = group
        result.s(**kwargs)
        if interactive:
            result.fill_required_fields()
        return result

    def from_paper(
        self,
        rank: Rank,
        name: str,
        paper: str,
        page_described: Union[None, int, str] = None,
        status: Status = Status.valid,
        age: Optional[constants.Age] = None,
        **override_kwargs: Any,
    ) -> "Taxon":
        authority, year = ehphp.call_ehphp("taxonomicAuthority", [paper])[0]
        result = self.add_static(
            rank=rank,
            name=name,
            original_citation=paper,
            page_described=page_described,
            original_name=name,
            authority=authority,
            year=year,
            parent=self,
            status=status,
            age=age,
        )
        result.base_name.s(**override_kwargs)
        result.base_name.fill_required_fields()
        return result

    def add_nominate(self) -> "Taxon":
        if self.rank == Rank.species:
            rank = Rank.subspecies
        elif self.rank == Rank.genus:
            rank = Rank.subgenus
        elif self.rank == Rank.tribe:
            rank = Rank.subtribe
        elif self.rank == Rank.subfamily:
            rank = Rank.tribe
        elif self.rank == Rank.family:
            rank = Rank.subfamily
        elif self.rank == Rank.superfamily:
            rank = Rank.family
        else:
            assert False, "Cannot add nominate subtaxon of %s of rank %s" % (
                self,
                self.rank.name,
            )

        taxon = Taxon.create(age=self.age, rank=rank, parent=self)
        taxon.base_name = self.base_name
        taxon.base_name.taxon = taxon
        taxon.recompute_name()
        return taxon

    def syn(self, name: Optional[str] = None, **kwargs: Any) -> Optional["Name"]:
        """Find a synonym matching the given arguments."""
        if name is not None:
            kwargs["root_name"] = name
        for candidate in self.sorted_names():
            for key, value in kwargs.items():
                if getattr(candidate, key) != value:
                    break
            else:
                return candidate
        return None

    def open_description(self) -> bool:
        return self.base_name.open_description()

    def compute_valid_name(self) -> str:
        name = self.base_name
        if name is None:
            raise Name.DoesNotExist(
                "Taxon with id %d has an invalid base_name" % self.id
            )
        if self.rank == Rank.division:
            return "%s Division" % name.root_name
        elif name.group in (Group.genus, Group.high):
            return name.root_name
        elif name.group == Group.family:
            return name.root_name + helpers.suffix_of_rank(self.rank)
        else:
            assert name.group == Group.species
            try:
                genus = self.parent_of_rank(Rank.genus)
            except ValueError:
                # if there is no genus, just use the original name
                # this may be one case where we can't rely on the computed valid name
                assert self.rank in (Rank.species, Rank.subspecies), (
                    "Taxon %s should have a genus parent" % self
                )
                # default to the original name for now. This isn't ideal because sometimes the original name
                # contains misspellings, but we don't really have a place to store that information better.
                return name.original_name
            else:
                if self.rank == Rank.species_group:
                    return "%s (%s)" % (genus.base_name.root_name, name.root_name)
                elif self.rank == Rank.species:
                    return "%s %s" % (genus.base_name.root_name, name.root_name)
                else:
                    assert self.rank == Rank.subspecies, (
                        "Unexpected rank %s" % self.rank.name
                    )
                    species = self.parent_of_rank(Rank.species)
                    return "%s %s %s" % (
                        genus.base_name.root_name,
                        species.base_name.root_name,
                        name.root_name,
                    )

    def expected_base_name(self) -> Optional["Name"]:
        """Finds the name that is expected to be the base name for this name."""
        if self.base_name.nomenclature_status == NomenclatureStatus.informal:
            return self.base_name
        names = set(self.names)
        if self.base_name.taxon != self:
            names |= set(self.base_name.taxon.names)
        group = self.base_name.group
        available_names = {
            nam
            for nam in names
            if nam.nomenclature_status == NomenclatureStatus.available
            and nam.group == group
        }
        if available_names:
            names = available_names
        if not names:
            return None
        names_and_years = sorted(
            [(nam, nam.effective_year()) for nam in names], key=lambda pair: pair[1]
        )
        selected_pair = names_and_years[0]
        if selected_pair[0] != self.base_name:
            possible = {
                nam for nam, year in names_and_years if year == selected_pair[1]
            }
            if self.base_name in possible:
                # If there are multiple names from the same year, assume we got the priority right
                return self.base_name
        return selected_pair[0]

    def check_expected_base_name(self) -> bool:
        expected = self.expected_base_name()
        if expected != self.base_name:
            print(f"{self}: expected {expected} but have {self.base_name}")
            return False
        else:
            return True

    def check_base_names(self) -> Iterable["Taxon"]:
        if not self.check_expected_base_name():
            yield self
        for child in self.children:
            yield from child.check_base_names()

    def recompute_name(self) -> None:
        new_name = self.compute_valid_name()
        if new_name != self.valid_name and new_name is not None:
            print("Changing valid name: %s -> %s" % (self.valid_name, new_name))
            self.valid_name = new_name
            self.save()

    def merge(self, into: "Taxon") -> None:
        for child in self.children:
            child.parent = into
            child.save()
        for nam in self.names:
            if nam != self.base_name:
                nam.taxon = into
                nam.save()

        self._merge_fields(into, exclude={"id", "_base_name_id"})
        self.base_name.merge(into.base_name, allow_valid=True)
        self.remove()

    def synonymize(self, to_taxon: "Taxon") -> "Name":
        if self.data is not None:
            print("Warning: removing data: %s" % self.data)
        assert self != to_taxon, "Cannot synonymize %s with itself" % self
        for child in self.children:
            child.parent = to_taxon
            child.save()
        nam = self.base_name
        nam.status = Status.synonym
        nam.save()
        for name in self.names:
            name.taxon = to_taxon
            name.save()
        for occ in self.occurrences:
            occ.taxon = to_taxon
            comment = occ.comment
            try:
                occ.add_comment("Previously under _%s_." % self.name)
                occ.save()
            except peewee.IntegrityError:
                print("dropping duplicate occurrence %s" % occ)
                existing = to_taxon.at(occ.location)
                additional_comment = "Also under _%s_ with source {%s}." % (
                    self.name,
                    occ.source,
                )
                if comment is not None:
                    additional_comment += " " + comment
                existing.add_comment(additional_comment)
        to_taxon.base_name.status = Status.valid
        self.delete_instance()
        return Name.get(Name.id == nam.id)

    def make_species_group(self) -> "Taxon":
        return self.make_parent_of_rank(Rank.species_group)

    def make_parent_of_rank(self, rank: Rank) -> "Taxon":
        if self.parent.rank == rank:
            parent = self.parent.parent
        else:
            parent = self.parent
        new_taxon = Taxon.create(rank=rank, age=self.age, parent=parent)
        new_taxon.base_name = self.base_name
        new_taxon.recompute_name()
        self.parent = new_taxon
        self.save()
        return new_taxon

    def run_on_self_and_children(self, callback: Callable[["Taxon"], object]) -> None:
        callback(self)
        for child in self.children:
            child.run_on_self_and_children(callback)

    def remove(self) -> None:
        if self.children.count() != 0:
            print("Cannot remove %s since it has unremoved children" % self)
            return
        print("Removing taxon %s" % self)
        for name in self.sorted_names():
            name.remove()
        self.delete_instance()

    def all_names(self, age: Optional[constants.Age] = None) -> Set["Name"]:
        names: Set["Name"]
        if age is not None:
            if self.age > age:
                return set()
            elif self.age == age:
                names = set(self.names)
            else:
                names = set()
        else:
            names = set(self.names)
        for child in self.children:
            names |= child.all_names(age=age)
        return names

    def names_missing_field(
        self, field: str, age: Optional[constants.Age] = None
    ) -> Set["Name"]:
        return {
            name
            for name in self.all_names(age=age)
            if getattr(name, field) is None
            and field in name.get_empty_required_fields()
        }

    def stats(self, age: Optional[constants.Age] = None) -> Dict[str, float]:
        names = self.all_names(age=age)
        counts: Dict[str, int] = collections.defaultdict(int)
        required_counts: Dict[str, int] = collections.defaultdict(int)
        counts_by_group: Dict[str, int] = collections.defaultdict(int)
        for name in names:
            counts_by_group[name.group] += 1
            for field in name.get_required_fields():
                required_counts[field] += 1
                if getattr(name, field) is not None:
                    counts[field] += 1

        total = len(names)
        output: Dict[str, float] = {"total": total}
        by_group = ", ".join(
            f"{v.name}: {counts_by_group[v]}" for v in reversed(Group)  # type: ignore
        )
        print(f"Total names: {total} ({by_group})")

        def print_percentage(num: int, total: int, label: str) -> float:
            if total == 0 or num == total:
                return 100.0
            percentage = num * 100.0 / total
            print("%s: %s of %s (%.2f%%)" % (label, num, total, percentage))
            return percentage

        overall_count = 0
        overall_required = 0
        for attribute, count in sorted(
            required_counts.items(), key=lambda i: (counts[i[0]], i[0])
        ):
            percentage = print_percentage(counts[attribute], count, attribute)
            output[attribute] = percentage
            overall_required += count
            overall_count += counts[attribute]
        if overall_required:
            output["score"] = overall_count / overall_required * 100
        else:
            output["score"] = 0.0
        print(f'Overall score: {output["score"]:.2f}')
        return output

    def fill_data_for_names(
        self,
        only_with_original: bool = True,
        min_year: Optional[int] = None,
        age: Optional[constants.Age] = None,
        field: Optional[str] = None,
    ) -> None:
        """Calls fill_required_fields() for all names in this taxon."""
        all_names = self.all_names(age=age)

        def should_include(nam: Name) -> bool:
            if nam.original_citation is None:
                return False
            if field is not None and field not in nam.get_empty_required_fields():
                return False
            if min_year is not None:
                try:
                    year = int(nam.year)
                except (ValueError, TypeError):
                    return True
                return min_year <= year
            else:
                return True

        citations = sorted(
            {nam.original_citation for nam in all_names if should_include(nam)}
        )
        for citation in citations:
            fill_data_from_paper(citation)
        if not only_with_original:
            for nam in self.all_names(age=age):
                if not should_include(nam):
                    print(nam)
                    nam.fill_required_fields()

    def fill_field(self, field: str) -> None:
        for name in self.all_names():
            if field in name.get_empty_required_fields():
                name.display()
                name.fill_field(field)

    at = _OccurrenceGetter()

    def __str__(self) -> str:
        return self.valid_name

    def __repr__(self) -> str:
        return str(self)

    def __getattr__(self, attr: str) -> "Name":
        """Returns a name belonging to this taxon with the given root_name or original_name."""
        candidates = [
            name
            for name in self.sorted_names()
            if name.root_name == attr or name.original_name == attr
        ]
        if len(candidates) == 1:
            return candidates[0]
        elif not candidates:
            raise AttributeError(attr)
        else:
            raise Name.DoesNotExist(f"Candidates: {candidates}")

    def __dir__(self) -> List[str]:
        result = set(super().__dir__())
        names = self.sorted_names()
        result |= set(name.original_name for name in names)
        result |= set(name.root_name for name in names)
        return [name for name in result if name is not None and " " not in name]


def fill_data_from_paper(paper: str, always_edit_tags: bool = False) -> None:
    opened = False

    def sort_key(nam: Name) -> Tuple[str, int]:
        try:
            return ("", int(nam.page_described))
        except (TypeError, ValueError):
            return (nam.page_described, 0)

    for nam in sorted(Name.filter(Name.original_citation == paper), key=sort_key):
        required_fields = list(nam.get_empty_required_fields())
        if required_fields:
            if not opened:
                getinput.add_to_clipboard(paper)
                ehphp.call_ehphp("openf", [paper])
                print(f"filling data from {paper}")
                opened = True
            print(nam, "described at", nam.page_described)
            nam.fill_required_fields()
        elif always_edit_tags:
            nam.fill_field("type_tags")


definition.taxon_cls = Taxon


T = TypeVar("T")


class Period(BaseModel):
    creation_event = events.Event["Period"]()
    save_event = events.Event["Period"]()
    label_field = "name"

    name = CharField()
    parent = ForeignKeyField(
        "self", related_name="children", db_column="parent_id", null=True
    )
    prev = ForeignKeyField(
        "self", related_name="next_foreign", db_column="prev_id", null=True
    )
    next = ForeignKeyField(
        "self", related_name="prev_foreign", db_column="next_id", null=True
    )
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    min_period = ForeignKeyField(
        "self", related_name="children_min", db_column="min_period_id", null=True
    )
    max_period = ForeignKeyField(
        "self", related_name="children_max", db_column="max_period_id", null=True
    )
    system = EnumField(constants.PeriodSystem)
    comment = CharField()

    @staticmethod
    def _filter_none(seq: Iterable[Optional[T]]) -> Iterable[T]:
        return (elt for elt in seq if elt is not None)

    def get_min_age(self) -> Optional[int]:
        if self.min_age is not None:
            return self.min_age
        return min(
            self._filter_none(child.get_min_age() for child in self.children),
            default=None,
        )

    def get_max_age(self) -> Optional[int]:
        if self.max_age is not None:
            return self.max_age
        return max(
            self._filter_none(child.get_max_age() for child in self.children),
            default=None,
        )

    @classmethod
    def make(
        cls,
        name: str,
        system: constants.PeriodSystem,
        parent: Optional["Period"] = None,
        next: Optional["Period"] = None,
        min_age: Optional[int] = None,
        max_age: Optional[int] = None,
        **kwargs: Any,
    ) -> "Period":
        if max_age is None and next is not None:
            max_age = next.min_age
        period = cls.create(
            name=name,
            system=system.value,
            parent=parent,
            next=next,
            min_age=min_age,
            max_age=max_age,
            **kwargs,
        )
        if next is not None:
            next.prev = period
            next.save()
        return period

    @classmethod
    def create_interactively(cls) -> "Period":
        print("creating Periods interactively only allows stratigraphic units")
        name = getinput.get_line("name> ")
        assert name is not None
        kind = getinput.get_enum_member(
            constants.PeriodSystem, "kind> ", allow_empty=False
        )
        result = cls.make_stratigraphy(name, kind)
        result.fill_required_fields()
        return result

    @classmethod
    def make_stratigraphy(
        cls,
        name: str,
        kind: constants.PeriodSystem,
        period: Optional["Period"] = None,
        parent: Optional["Period"] = None,
        **kwargs: Any,
    ) -> "Period":
        if period is not None:
            kwargs["max_period"] = kwargs["min_period"] = period
        period = cls.create(name=name, system=kind.value, parent=parent, **kwargs)
        if "next" in kwargs:
            next_period = kwargs["next"]
            next_period.prev = period
            next_period.save()
        return period

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        file.write("%s%s\n" % (" " * (depth + 4), repr(self)))
        for location in Location.filter(
            Location.max_period == self, Location.min_period == self
        ):
            location.display(full=full, depth=depth + 2, file=file)
        for location in self.locations_stratigraphy:
            location.display(full=full, depth=depth + 2, file=file)
        for period in self.children:
            period.display(full=full, depth=depth + 1, file=file)
        for period in Period.filter(
            Period.max_period == self, Period.min_period == self
        ):
            period.display(full=full, depth=depth + 1, file=file)

    def make_locality(self, region: "Region") -> "Location":
        return Location.make(self.name, region, self)

    def __repr__(self) -> str:
        properties = {}
        for field in self.fields():
            if field == "name":
                continue
            value = getattr(self, field)
            if value is None:
                continue
            if isinstance(value, Period):
                value = value.name
            properties[field] = value
        return "%s (%s)" % (
            self.name,
            ", ".join("%s=%s" % item for item in properties.items()),
        )


class Region(BaseModel):
    label_field = "name"

    name = CharField()
    comment = CharField(null=True)
    parent = ForeignKeyField(
        "self", related_name="children", db_column="parent_id", null=True
    )
    kind = EnumField(constants.RegionKind)

    @classmethod
    def make(
        cls, name: str, kind: constants.RegionKind, parent: Optional["Region"] = None
    ) -> "Region":
        region = cls.create(name=name, kind=kind, parent=parent)
        Location.make(
            name=name,
            period=Period.filter(Period.name == "Recent").get(),
            region=region,
        )
        return region

    def __repr__(self) -> str:
        out = self.name
        if self.parent:
            out += ", %s" % self.parent.name
        out += " (%s)" % self.kind
        return out

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        file.write("%s%s\n" % (" " * (depth + 4), repr(self)))
        if self.comment:
            file.write("%sComment: %s\n" % (" " * (depth + 12), self.comment))
        for location in self.locations:
            location.display(full=full, depth=depth + 4, file=file)
        for child in self.children:
            child.display(full=full, depth=depth + 4, file=file)

    def get_location(self) -> "Location":
        """Returns the corresponding Recent Location."""
        return Location.get(region=self, name=self.name)

    def all_parents(self) -> Iterable["Region"]:
        """Returns all parent regions of this region."""
        if self.parent is not None:
            yield self.parent
            yield from self.parent.all_parents()


class Location(BaseModel):
    creation_event = events.Event["Location"]()
    save_event = events.Event["Location"]()
    label_field = "name"

    name = CharField()
    min_period = ForeignKeyField(
        Period, related_name="locations_min", db_column="min_period_id", null=True
    )
    max_period = ForeignKeyField(
        Period, related_name="locations_max", db_column="max_period_id", null=True
    )
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    stratigraphic_unit = ForeignKeyField(
        Period,
        related_name="locations_stratigraphy",
        db_column="stratigraphic_unit_id",
        null=True,
    )
    region = ForeignKeyField(Region, related_name="locations", db_column="region_id")
    comment = CharField()
    latitude = CharField()
    longitude = CharField()
    location_detail = TextField()
    age_detail = TextField()
    source = TextField()

    @classmethod
    def make(
        cls,
        name: str,
        region: Region,
        period: Period,
        comment: Optional[str] = None,
        stratigraphic_unit: Optional[Period] = None,
    ) -> "Location":
        return cls.create(
            name=name,
            min_period=period,
            max_period=period,
            region=region,
            comment=comment,
            stratigraphic_unit=stratigraphic_unit,
        )

    @classmethod
    def create_interactively(cls) -> "Location":
        name = getinput.get_line("name> ")
        assert name is not None
        region = cls.get_value_for_foreign_key_field_on_class("region")
        period = cls.get_value_for_foreign_key_field_on_class("min_period")
        comment = getinput.get_line("comment> ")
        result = cls.make(name=name, region=region, period=period, comment=comment)
        result.fill_required_fields()
        return result

    def get_value_for_field(self, field: str) -> Any:
        if field == "source":
            return self.get_value_for_article_field(field)
        else:
            return super().get_value_for_field(field)

    def __repr__(self) -> str:
        age_str = ""
        if self.stratigraphic_unit is not None:
            age_str += self.stratigraphic_unit.name
        if self.max_period is not None:
            if self.stratigraphic_unit is not None:
                age_str += "; "
            age_str += self.max_period.name
            if self.min_period != self.max_period:
                age_str += "–%s" % self.min_period.name
        if self.min_age is not None and self.max_age is not None:
            age_str += "; %s–%s" % (self.max_age, self.min_age)
        return "%s (%s), %s" % (self.name, age_str, self.region.name)

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        file.write("%s%s\n" % (" " * (depth + 4), repr(self)))
        if self.comment:
            file.write("%sComment: %s\n" % (" " * (depth + 12), self.comment))
        if full:
            self.display_organized(depth=depth, file=file)
        else:
            for occurrence in sorted(self.taxa, key=lambda occ: occ.taxon.valid_name):
                file.write("%s%s\n" % (" " * (depth + 8), occurrence))

    def display_organized(self, depth: int = 0, file: IO[str] = sys.stdout) -> None:
        taxa = sorted(
            ((occ, occ.taxon.ranked_parents()) for occ in self.taxa),
            key=lambda pair: (
                "" if pair[1][0] is None else pair[1][0].valid_name,
                "" if pair[1][1] is None else pair[1][1].valid_name,
                pair[0].taxon.valid_name,
            ),
        )
        current_order = None
        current_family = None
        for occ, (order, family) in taxa:
            if order != current_order:
                current_order = order
                if order is not None:
                    file.write("%s%s\n" % (" " * (depth + 8), order))
            if family != current_family:
                current_family = family
                if family is not None:
                    file.write("%s%s\n" % (" " * (depth + 12), family))
            file.write("%s%s\n" % (" " * (depth + 16), occ))

    def make_local_unit(
        self, name: Optional[str] = None, parent: Optional[Period] = None
    ) -> Period:
        if name is None:
            name = self.name
        period = Period.make(  # type: ignore
            name,
            constants.PeriodSystem.local_unit,
            parent=parent,
            min_age=self.min_age,
            max_age=self.max_age,
            min_period=self.min_period,
            max_period=self.max_period,
        )
        self.min_period = self.max_period = period
        self.save()
        return period


class SpeciesNameComplex(BaseModel):
    """Groups of species-group names of the same derivation or nature.

    See ICZN Articles 11.9.1 and 31.

    """
    creation_event = events.Event["SpeciesNameComplex"]()
    save_event = events.Event["SpeciesNameComplex"]()
    label_field = "label"

    label = CharField()
    stem = CharField()
    kind = EnumField(SpeciesNameKind)
    masculine_ending = CharField()
    feminine_ending = CharField()
    neuter_ending = CharField()
    comment = CharField()

    class Meta(object):
        db_table = "species_name_complex"

    def __repr__(self) -> str:
        if any(
            ending != ""
            for ending in (
                self.masculine_ending,
                self.feminine_ending,
                self.neuter_ending,
            )
        ):
            return (
                f"{self.label} ({self.kind.name}, -{self.masculine_ending}, -{self.feminine_ending}, -{self.neuter_ending})"
            )
        else:
            return f"{self.label} ({self.kind.name})"

    def self_apply(self, dry_run: bool = True) -> List["Name"]:
        return self.apply_to_ending(self.label, dry_run=dry_run)

    def apply_to_ending(
        self,
        ending: str,
        dry_run: bool = True,
        interactive: bool = False,
        full_name_only: bool = True,
    ) -> List["Name"]:
        """Adds the name complex to all names with a specific ending."""
        names = [
            name
            for name in Name.filter(
                Name.group == Group.species,
                Name._name_complex_id >> None,
                Name.root_name % f"*{ending}",
            )
            if name.root_name.endswith(ending)
        ]
        print(f"found {len(names)} names with -{ending} to apply {self}")
        for name in names:
            print(name)
            if not dry_run:
                name.name_complex = self
        if interactive:
            if getinput.yes_no("apply?"):
                for name in names:
                    name.name_complex = self
                dry_run = False
        if not dry_run:
            saved_endings = list(self.endings)
            if not any(e.ending == ending for e in saved_endings):
                print(f"saving ending {ending}")
                self.make_ending(ending, full_name_only=full_name_only)
        return names

    def get_stem_from_name(self, name: str) -> str:
        """Applies the group to a genus name to get the name's stem."""
        assert self.stem.endswith(self.masculine_ending)
        stem = self.stem[: -len(self.masculine_ending)]
        for ending in (self.masculine_ending, self.feminine_ending, self.neuter_ending):
            if name.endswith(stem + ending):
                if ending == "":
                    return name
                else:
                    return name[: -len(ending)]
        raise ValueError(f"could not extract stem from {name} using {self}")

    def get_forms(self, name: str) -> Iterable[str]:
        if self.kind == SpeciesNameKind.adjective:
            stem = self.get_stem_from_name(name)
            for ending in (
                self.masculine_ending,
                self.feminine_ending,
                self.neuter_ending,
            ):
                yield stem + ending
        else:
            yield name

    def get_names(self) -> List["Name"]:
        return list(
            Name.filter(Name._name_complex_id == self.id, Name.group == Group.species)
        )

    def make_ending(
        self, ending: str, comment: Optional[str] = "", full_name_only: bool = False
    ) -> "SpeciesNameEnding":
        return SpeciesNameEnding.get_or_create(
            name_complex=self,
            ending=ending,
            comment=comment,
            full_name_only=full_name_only,
        )

    def remove(self) -> None:
        for nam in self.get_names():
            print("removing name complex from", nam)
            nam.name_complex = None
            nam.save()
        for ending in self.endings:
            print("removing ending", ending)
            ending.delete_instance()
        print("removing complex", self)
        self.delete_instance()

    @classmethod
    def make(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        kind: SpeciesNameKind,
        comment: Optional[str] = None,
        masculine_ending: str = "",
        feminine_ending: str = "",
        neuter_ending: str = "",
    ) -> "SpeciesNameComplex":
        return cls.create(
            label=label,
            stem=stem,
            kind=kind,
            comment=comment,
            masculine_ending=masculine_ending,
            feminine_ending=feminine_ending,
            neuter_ending=neuter_ending,
        )

    @classmethod
    def _get_or_create(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        kind: SpeciesNameKind,
        comment: Optional[str] = None,
        masculine_ending: str = "",
        feminine_ending: str = "",
        neuter_ending: str = "",
    ) -> "SpeciesNameComplex":
        try:
            return cls.get(cls.label == label, cls.stem == stem, cls.kind == kind)
        except peewee.DoesNotExist:
            print("creating new name complex with label", label)
            return cls.make(
                label=label,
                stem=stem,
                kind=kind,
                comment=comment,
                masculine_ending=masculine_ending,
                feminine_ending=feminine_ending,
                neuter_ending=neuter_ending,
            )

    @classmethod
    def by_label(cls, label: str) -> "SpeciesNameComplex":
        complexes = list(cls.filter(cls.label == label))
        if len(complexes) == 1:
            return complexes[0]
        else:
            raise ValueError(f"found {complexes} with label {label}")

    @classmethod
    def of_kind(cls, kind: SpeciesNameKind) -> "SpeciesNameComplex":
        """Indeclinable name of a particular kind."""
        return cls._get_or_create(kind.name, kind=kind)

    @classmethod
    def ambiguous(
        cls, stem: str, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        """For groups of names that are ambiguously nouns in apposition (Art. 31.2.2)."""
        return cls._get_or_create(
            stem, stem=stem, kind=SpeciesNameKind.ambiguous_noun, comment=comment
        )

    @classmethod
    def adjective(
        cls,
        stem: str,
        comment: Optional[str],
        masculine_ending: str,
        feminine_ending: str,
        neuter_ending: str,
        auto_apply: bool = False,
    ) -> "SpeciesNameComplex":
        """Name based on a Latin adjective."""
        snc = cls._get_or_create(
            stem,
            stem=stem,
            kind=SpeciesNameKind.adjective,
            comment=comment,
            masculine_ending=masculine_ending,
            feminine_ending=feminine_ending,
            neuter_ending=neuter_ending,
        )
        if auto_apply:
            snc.self_apply(dry_run=False)
        return snc

    @classmethod
    def first_declension(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "us", "a", "um", auto_apply=auto_apply)

    @classmethod
    def third_declension(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "is", "is", "e", auto_apply=auto_apply)

    @classmethod
    def invariant(
        cls, stem: str, auto_apply: bool = True, comment: Optional[str] = None
    ) -> "SpeciesNameComplex":
        return cls.adjective(stem, comment, "", "", "", auto_apply=auto_apply)

    @classmethod
    def create_interactively(cls) -> "SpeciesNameComplex":
        kind = getinput.get_with_completion(
            [
                "ambiguous",
                "adjective",
                "first_declension",
                "third_declension",
                "invariant",
            ],
            "kind> ",
        )
        stem = getinput.get_line("stem> ")
        assert stem is not None
        comment = getinput.get_line("comment> ")
        if kind == "adjective":
            masculine = getinput.get_line("masculine_ending> ")
            feminine = getinput.get_line("feminine_ending> ")
            neuter = getinput.get_line("neuter_ending> ")
            assert masculine is not None
            assert feminine is not None
            assert neuter is not None
            return cls.adjective(stem, comment, masculine, feminine, neuter)
        else:
            return getattr(cls, kind)(stem=stem, comment=comment)


class NameComplex(BaseModel):
    """Group of genus-group names with the same derivation."""
    creation_event = events.Event["NameComplex"]()
    save_event = events.Event["NameComplex"]()
    label_field = "label"

    label = CharField()
    stem = CharField()
    source_language = EnumField(SourceLanguage)
    code_article = EnumField(GenderArticle)
    gender = EnumField(constants.Gender)
    comment = CharField()
    stem_remove = CharField(null=False)
    stem_add = CharField(null=False)

    class Meta(object):
        db_table = "name_complex"

    def __repr__(self) -> str:
        return (
            f'{self.label} ({self.code_article.name}, {self.gender.name}, -{self.stem_remove or ""}+{self.stem_add or ""})'
        )

    def self_apply(self, dry_run: bool = True) -> List["Name"]:
        return self.apply_to_ending(self.label, dry_run=dry_run)

    def apply_to_ending(self, ending: str, dry_run: bool = True) -> List["Name"]:
        """Adds the name complex to all names with a specific ending."""
        names = [
            name
            for name in Name.filter(
                Name.group == Group.genus,
                Name._name_complex_id >> None,
                Name.root_name % f"*{ending}",
            )
            if name.root_name.endswith(ending)
        ]
        print(f"found {len(names)} names with -{ending} to apply {self}")
        output = []
        for name in names:
            if name.gender is not None and name.gender != self.gender:
                print(
                    f"ignoring {name} because its gender {name.gender} does not match"
                )
                output.append(name)
            else:
                print(name)
                if not dry_run:
                    name.name_complex = self
                    name.save()
        if not dry_run:
            saved_endings = list(self.endings)
            if not any(e.ending == ending for e in saved_endings):
                print(f"saving ending {ending}")
                self.make_ending(ending)
        return output

    def get_stem_from_name(self, name: str) -> str:
        """Applies the group to a genus name to get the name's stem."""
        if self.stem_remove:
            if not name.endswith(self.stem_remove):
                raise ValueError(f"{name} does not end with {self.stem_remove}")
            name = name[: -len(self.stem_remove)]
        return name + self.stem_add

    def make_ending(self, ending: str, comment: Optional[str] = "") -> "NameEnding":
        return NameEnding.create(name_complex=self, ending=ending, comment=comment)

    def get_names(self) -> List["Name"]:
        return list(
            Name.filter(Name._name_complex_id == self.id, Name.group == Group.genus)
        )

    @classmethod
    def make(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        source_language: SourceLanguage = SourceLanguage.other,
        code_article: GenderArticle,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        return cls.create(
            label=label,
            stem=stem,
            source_language=source_language,
            code_article=code_article,
            gender=gender,
            comment=comment,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def _get_or_create(
        cls,
        label: str,
        *,
        stem: Optional[str] = None,
        source_language: SourceLanguage,
        code_article: GenderArticle,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        try:
            return cls.get(
                cls.label == label,
                cls.source_language == source_language,
                cls.code_article == code_article,
                cls.gender == gender,
            )
        except peewee.DoesNotExist:
            print("creating new name complex with label", label)
            return cls.make(
                label=label,
                stem=stem,
                source_language=source_language,
                code_article=code_article,
                gender=gender,
                comment=comment,
                stem_remove=stem_remove,
                stem_add=stem_add,
            )

    @classmethod
    def by_label(cls, label: str) -> "NameComplex":
        complexes = list(cls.filter(cls.label == label))
        if len(complexes) == 1:
            return complexes[0]
        else:
            raise ValueError("found {complexes} with label {label}")

    @classmethod
    def latin_stem(
        cls,
        stem: str,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Latin dictionary with a specific gender."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_1,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def greek_stem(
        cls,
        stem: str,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Greek dictionary with a specific gender."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def latinized_greek(
        cls,
        stem: str,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a word found in a Greek dictionary, but with a changed suffix."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_3,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def bad_transliteration(
        cls,
        stem: str,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name based on a Greek word, but with incorrect transliteration."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.bad_transliteration,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def common_gender(
        cls,
        stem: str,
        gender: constants.Gender = constants.Gender.masculine,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Name of common gender in Latin, which defaults to masculine."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_4_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def oides_name(
        cls,
        stem: str,
        gender: constants.Gender = constants.Gender.masculine,
        comment: Optional[str] = None,
    ) -> "NameComplex":
        """Names ending in -oides and a few other endings default to masculine unless the author treated it otherwise."""
        if stem not in ("ites", "oides", "ides", "odes", "istes"):
            raise ValueError("Art. 30.1.4.4 only applies to a limited set of stems")
        if gender != constants.Gender.masculine:
            label = f"{stem}_{gender.name}"
        else:
            label = stem
        return cls._get_or_create(
            label,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.greek,
            code_article=GenderArticle.art30_1_4_4,
            stem_remove="es",
            stem_add="",
        )

    @classmethod
    def latin_changed_ending(
        cls,
        stem: str,
        gender: constants.Gender,
        comment: Optional[str] = None,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Based on a Latin word with a changed ending. Comment must specify the original word."""
        return cls._get_or_create(
            stem,
            stem=stem,
            gender=gender,
            comment=comment,
            source_language=SourceLanguage.latin,
            code_article=GenderArticle.art30_1_4_5,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def expressly_specified(
        cls, gender: constants.Gender, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Gender expressly specified by the author."""
        label = cls._make_label(
            f"expressly_specified_{gender.name}", stem_remove, stem_add
        )
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_2,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def indicated(
        cls, gender: constants.Gender, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Gender indicated by an adjectival species name."""
        label = cls._make_label(f"indicated_{gender.name}", stem_remove, stem_add)
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_3,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def defaulted_masculine(
        cls, stem_remove: str = "", stem_add: str = ""
    ) -> "NameComplex":
        """Defaulted to masculine as a non-Western name."""
        label = cls._make_label("defaulted_masculine", stem_remove, stem_add)
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=constants.Gender.masculine,
            code_article=GenderArticle.art30_2_4,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @classmethod
    def defaulted(
        cls,
        gender: constants.Gender,
        ending: str,
        stem_remove: str = "",
        stem_add: str = "",
    ) -> "NameComplex":
        """Defaulted to feminine or neuter as a non-Western name with a specific ending."""
        if gender == constants.Gender.masculine:
            assert False, "use defaulted_masculine instead"
        elif gender == constants.Gender.feminine:
            assert ending == "a", "only -a endings default to feminine"
        elif gender == constants.Gender.neuter:
            assert ending in (
                "um",
                "on",
                "u",
            ), "only -um, -on, and -u endings default to neuter"
        label = cls._make_label(
            f"defaulted_{gender.name}_{ending}", stem_remove, stem_add
        )
        return cls._get_or_create(
            label,
            source_language=SourceLanguage.other,
            gender=gender,
            code_article=GenderArticle.art30_2_4,
            stem_remove=stem_remove,
            stem_add=stem_add,
        )

    @staticmethod
    def _make_label(base_label: str, stem_remove: str, stem_add: str) -> str:
        if stem_remove or stem_add:
            base_label += "_stem"
        if stem_remove:
            base_label += f"_{stem_remove}"
        if stem_add:
            base_label += f"_{stem_add}"
        return base_label

    @classmethod
    def create_interactively(cls) -> "NameComplex":
        kind = getinput.get_with_completion(
            [
                "latin_stem",
                "greek_stem",
                "latinized_greek",
                "bad_transliteration",
                "common_gender",
                "latin_changed_ending",
                "expressly_specified",
                "indicated",
                "defaulted_masculine",
                "defaulted",
            ],
            "kind> ",
        )
        method = getattr(cls, kind)
        if kind in (
            "latin_stem",
            "greek_stem",
            "latinized_greek",
            "bad_transliteration",
            "common_gender",
            "latin_changed_ending",
        ):
            stem = getinput.get_line("stem> ")
            gender = getinput.get_enum_member(constants.Gender, "gender> ")
            comment = getinput.get_line("comment> ")
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(
                stem=stem,
                gender=gender,
                comment=comment,
                stem_remove=stem_remove,
                stem_add=stem_add,
            )
            nc.self_apply()
            if getinput.yes_no("self-apply?"):
                nc.self_apply(dry_run=False)
        elif kind in ("expressly_specified", "indicated"):
            gender = getinput.get_enum_member(constants.Gender, "gender> ")
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(gender=gender, stem_remove=stem_remove, stem_add=stem_add)
        elif kind == "defaulted_masculine":
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(stem_remove=stem_remove, stem_add=stem_add)
        elif kind == "defaulted":
            gender = getinput.get_enum_member(constants.Gender, "gender> ")
            ending = getinput.get_line("ending> ")
            stem_remove = getinput.get_line("stem_remove> ")
            stem_add = getinput.get_line("stem_add> ")
            nc = method(
                gender=gender, ending=ending, stem_remove=stem_remove, stem_add=stem_add
            )
        else:
            assert False, f"bad kind {kind}"
        return nc


class NameEnding(BaseModel):
    """Name ending that is mapped to a NameComplex."""
    label_field = "ending"

    name_complex = ForeignKeyField(
        NameComplex, related_name="endings", db_column="name_complex_id"
    )
    ending = CharField()
    comment = CharField()

    class Meta(object):
        db_table = "name_ending"


class SpeciesNameEnding(BaseModel):
    """Name ending that is mapped to a SpeciesNameComplex."""
    label_field = "ending"

    name_complex = ForeignKeyField(
        SpeciesNameComplex, related_name="endings", db_column="name_complex_id"
    )
    ending = CharField()
    comment = CharField()
    full_name_only = BooleanField(default=False)

    class Meta(object):
        db_table = "species_name_ending"

    @classmethod
    def get_or_create(
        cls,
        name_complex: SpeciesNameComplex,
        ending: str,
        comment: Optional[str] = None,
        full_name_only: bool = False,
    ) -> "SpeciesNameEnding":
        try:
            return cls.get(
                cls.name_complex == name_complex,
                cls.ending == ending,
                cls.full_name_only == full_name_only,
            )
        except peewee.DoesNotExist:
            print("creating new name ending", ending, " for ", name_complex)
            return cls.create(
                name_complex=name_complex,
                ending=ending,
                comment=comment,
                full_name_only=full_name_only,
            )


class Collection(BaseModel):
    creation_event = events.Event["Collection"]()
    save_event = events.Event["Collection"]()
    label_field = "label"

    label = CharField()
    name = CharField()
    location = ForeignKeyField(
        Region, related_name="collections", db_column="location_id"
    )
    comment = CharField(null=True)

    def __repr__(self) -> str:
        return f"{self.name} ({self.label})"

    @classmethod
    def by_label(cls, label: str) -> "Collection":
        colls = list(cls.filter(cls.label == label))
        if len(colls) == 1:
            return colls[0]
        else:
            raise ValueError(f"found {colls} with label {label}")

    @classmethod
    def get_or_create(
        cls, label: str, name: str, location: Region, comment: Optional[str] = None
    ) -> "Collection":
        try:
            return cls.by_label(label)
        except ValueError:
            return cls.create(
                label=label, name=name, location=location, comment=comment
            )

    @classmethod
    def create_interactively(cls: Type[ModelT]) -> ModelT:
        label = getinput.get_line("label> ")
        name = getinput.get_line("name> ")
        location = cls.get_value_for_foreign_key_field_on_class("location")
        obj = cls.create(label=label, name=name, location=location)
        obj.fill_required_fields()
        return obj


class Name(BaseModel):
    creation_event = events.Event["Name"]()
    save_event = events.Event["Name"]()
    label_field = "original_name"
    field_defaults = {
        "genus_type_kind": constants.TypeSpeciesDesignation.original_designation,
        "species_type_kind": constants.SpeciesGroupType.holotype,
        "nomenclature_status": NomenclatureStatus.available,
        "status": Status.valid,
    }

    # Basic data
    group = EnumField(Group)
    root_name = CharField()
    status = EnumField(Status)
    taxon = ForeignKeyField(Taxon, related_name="names", db_column="taxon_id")
    original_name = CharField(null=True)
    # Original name, with corrections for issues like capitalization and diacritics. Should not correct incorrect original spellings
    # for other reasons (e.g., prevailing usage). Consider a case where Gray (1825) names _Mus Somebodyi_, then Gray (1827) spells it
    # _Mus Somebodii_ and all subsequent authors follow this usage, rendering it a justified emendation. In this case, the 1825 name
    # should have original_name _Mus Somebodyi_, corrected original name _Mus somebodyi_, and root name _somebodii_. The 1827 name
    # should be listed as a justified emendation.
    corrected_original_name = CharField(null=True)
    nomenclature_status = EnumField(
        NomenclatureStatus, default=NomenclatureStatus.available
    )

    # Citation and authority
    authority = CharField(null=True)
    original_citation = CharField(null=True)
    page_described = CharField(null=True)
    verbatim_citation = CharField(null=True)
    year = CharField(null=True)  # redundant with data for the publication itself

    # Gender and stem
    stem = CharField(null=True)  # redundant with name complex?
    gender = EnumField(constants.Gender)  # for genus group; redundant with name complex
    _name_complex_id = IntegerField(null=True, db_column="name_complex_id")

    # Types
    type = ForeignKeyField(
        "self", null=True, db_column="type_id", related_name="typified_names"
    )  # for family and genus group
    verbatim_type = CharField(null=True)  # deprecated
    type_locality = ForeignKeyField(
        Location,
        related_name="type_localities",
        db_column="type_locality_id",
        null=True,
    )
    type_locality_description = TextField(null=True)
    type_specimen = CharField(null=True)
    collection = ForeignKeyField(
        Collection, null=True, db_column="collection_id", related_name="type_specimens"
    )
    type_specimen_source = CharField(null=True)
    genus_type_kind = EnumField(constants.TypeSpeciesDesignation, null=True)
    species_type_kind = EnumField(constants.SpeciesGroupType, null=True)
    type_tags = ADTField(lambda: TypeTag, null=True)

    # Miscellaneous data
    data = TextField(null=True)
    nomenclature_comments = TextField(null=True)
    other_comments = TextField(null=True)  # deprecated
    taxonomy_comments = TextField(null=True)
    _definition = CharField(null=True, db_column="definition")
    tags = ADTField(lambda: Tag, null=True)

    class Meta(object):
        db_table = "name"

    @property
    def name_complex(self) -> Union[None, NameComplex, SpeciesNameComplex]:
        if self._name_complex_id is None:
            return None
        if self.group == Group.species:
            return SpeciesNameComplex.get(id=self._name_complex_id)
        elif self.group == Group.genus:
            return NameComplex.get(id=self._name_complex_id)
        else:
            raise TypeError(f"{self} cannot have a name complex")

    @name_complex.setter
    def name_complex(self, nc: Union[None, NameComplex, SpeciesNameComplex]) -> None:
        if nc is not None:
            if self.group == Group.species:
                if not isinstance(nc, SpeciesNameComplex):
                    raise TypeError(f"{nc} must be a SpeciesNameComplex")
            elif self.group == Group.genus:
                if not isinstance(nc, NameComplex):
                    raise TypeError(f"{nc} must be a NameComplex")
            else:
                raise TypeError(f"cannot set name_complex")
            self._name_complex_id = nc.id
        else:
            self._name_complex_id = None

    @property
    def definition(self) -> Optional[Definition]:
        data = self._definition
        if data is None:
            return None
        else:
            return Definition.unserialize(data)

    @definition.setter
    def definition(self, defn: Definition) -> None:
        if defn is None:
            self._definition = None
        else:
            self._definition = defn.serialize()

    def infer_corrected_original_name(self) -> Optional[str]:
        if not self.original_name or self.group not in (Group.genus, Group.species):
            return None
        original_name = (
            self.original_name.replace("(?)", "")
            .replace("?", "")
            .replace("æ", "ae")
            .replace("ë", "e")
            .replace("í", "i")
            .replace("ï", "i")
            .replace("á", "a")
            .replace('"', "")
            .replace("'", "")
            .replace("ř", "r")
            .replace("é", "e")
            .replace("š", "s")
            .replace("á", "a")
            .replace("ć", "c")
        )
        original_name = re.sub(r"\s+", " ", original_name).strip()
        original_name = re.sub(r"([a-z]{2})-([a-z]{2})", r"\1\2", original_name)
        if self.group == Group.genus:
            if re.match(r"^[A-Z][a-z]+$", original_name):
                return original_name
            match = re.match(r"^[A-Z][a-z]+ \(([A-Z][a-z]+)\)$", original_name)
            if match:
                return match.group(1)
        elif self.group == Group.species:
            if re.match(r"^[A-Z][a-z]+( [a-z]+){1,2}$", original_name):
                return original_name
            if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", original_name):
                genus, species = original_name.split()
                return f"{genus} {species.lower()}"
            match = re.match(
                r"^(?P<genus>[A-Z][a-z]+)( \([A-Z][a-z]+\))? (?P<species>[A-Z]?[a-z]+)((,? var\.)? (?P<subspecies>[A-Z]?[a-z]+))?$",
                original_name,
            )
            if match:
                name = f'{match.group("genus")} {match.group("species").lower()}'
                if match.group("subspecies"):
                    name += " " + match.group("subspecies").lower()
                return name
        return None

    def get_value_for_field(self, field: str) -> Any:
        if (
            field == "collection"
            and self.collection is None
            and self.type_specimen is not None
        ):
            coll_name = self.type_specimen.split()[0]
            getter = Collection.getter("label")
            if coll_name in getter:
                coll = getter(coll_name)
                print(f"inferred collection to be {coll} from {self.type_specimen}")
                return coll
            return super().get_value_for_field(field)
        elif field == "corrected_original_name":
            inferred = self.infer_corrected_original_name()
            if inferred is not None:
                print(
                    f"inferred corrected_original_name to be {inferred!r} from {self.original_name!r}"
                )
                return inferred
            else:
                return super().get_value_for_field(field)
        elif field == "type_tags":
            if self.type_locality_description is not None:
                print(self.type_locality_description)
            if self.type_locality is not None:
                print(self.type_locality)
            return super().get_value_for_field(field)
        elif field == "original_citation":
            return self.get_value_for_article_field(field)
        elif field == "type_specimen_source":
            return self.get_value_for_article_field(
                field, default=self.original_citation
            )
        elif field == "type":
            typ = self.get_value_for_foreign_key_field("type")
            print(f"type: {typ}")
            if typ is None:
                return None
            elif getinput.yes_no("Is this correct? "):
                return typ
            else:
                raise EOFError
        elif field == "name_complex":
            if self.group == Group.genus:
                return self.get_name_complex(NameComplex)
            elif self.group == Group.species:
                value = self.get_name_complex(SpeciesNameComplex)
                if value is not None and value.kind.is_single_complex():
                    value.apply_to_ending(self.root_name, interactive=True)
                return value
            else:
                raise TypeError("cannot have name complex")
        else:
            return super().get_value_for_field(field)

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:

        def original_name_completer(p: str, d: Optional[str]) -> Optional[Name]:
            return Name.getter("original_name").get_one(p, default=d or "")

        def collection_completer(p: str, d: Optional[str]) -> Optional[Name]:
            return Collection.getter("label").get_one(p, default=d or "")

        for field_name, tag_cls in [("type_tags", TypeTag), ("tags", Tag)]:
            if field == field_name:
                completers: Dict[
                    Tuple[Type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():  # type: ignore
                    for attribute, typ in tag._attributes.items():
                        if typ is Name:
                            completers[(tag, attribute)] = original_name_completer
                        elif typ is Collection:
                            completers[(tag, attribute)] = collection_completer
                        elif typ is str and attribute in ("source", "opinion"):
                            completers[
                                (tag, attribute)
                            ] = self._completer_for_source_field
                return completers
        return {}

    def _completer_for_source_field(self, prompt: str, default: str) -> str:
        return self.get_value_for_article_field(prompt[:-2], default=default) or ""

    def get_empty_required_fields(self) -> Iterable[str]:
        fields = []
        for field in super().get_empty_required_fields():
            fields.append(field)
            yield field
        if fields and self.group == Group.species and "type_tags" not in fields:
            # Always make the user edit type_tags if some other field was unfilled.
            yield "type_tags"

    @staticmethod
    def get_name_complex(model_cls: Type[BaseModel]) -> Optional[BaseModel]:
        getter = model_cls.getter("label")
        value = getter.get_one_key("name_complex> ")
        if value is None:
            return None
        elif value == "n":
            return model_cls.create_interactively()
        else:
            return model_cls.by_label(value)

    def add_additional_data(self, new_data: str) -> None:
        """Add data to the "additional" field within the "data" field"""
        data = json.loads(self.data)
        if "additional" not in data:
            data["additional"] = []
        data["additional"].append(new_data)
        self.data = json.dumps(data)
        self.save()

    def add_data(self, field: str, value: Any, concat_duplicate: bool = False) -> None:
        if self.data is None or self.data == "":
            data: Dict[str, Any] = {}
        else:
            data = json.loads(self.data)
        if field in data:
            if concat_duplicate:
                existing = data[field]
                if isinstance(existing, list):
                    value = existing + [value]
                else:
                    value = [existing, value]
            else:
                raise ValueError(f"{field} is already in {data}")
        data[field] = value
        self.data = json.dumps(data)

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = self.tags + (tag,)

    def add_type_tag(self, tag: adt.ADT) -> None:
        if self.type_tags is None:
            self.type_tags = [tag]
        else:
            self.type_tags = self.type_tags + (tag,)

    def has_type_tag(self, tag_cls: Type[adt.ADT]) -> bool:
        if self.type_tags is None:
            return False
        for tag in self.type_tags:
            if isinstance(tag, tag_cls):
                return True
        return False

    def add_included(self, species: "Name", comment: str = "") -> None:
        assert isinstance(species, Name)
        self.add_type_tag(TypeTag.IncludedSpecies(species, comment))

    def add_comment(
        self,
        kind: Optional[constants.CommentKind] = None,
        text: Optional[str] = None,
        source: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
        return NameComment.create_interactively(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def description(self) -> str:
        if self.original_name:
            out = self.original_name
        else:
            out = self.root_name
        if self.authority:
            out += " %s" % self.authority
        if self.year:
            out += ", %s" % self.year
        out += " (= %s)" % self.taxon.valid_name
        return out

    def is_unavailable(self) -> bool:
        return not self.nomenclature_status.can_preoccupy()

    def numeric_page_described(self) -> int:
        if self.page_described is None:
            return 0
        match = re.match(r"^(\d+)", self.page_described)
        if match:
            return int(match.group(1))
        else:
            return 0

    def numeric_year(self) -> int:
        if self.year is None:
            return 0
        elif "-" in self.year:
            return int(self.year.split("-")[-1])
        else:
            return int(self.year)

    def make_variant(
        self, status: NomenclatureStatus, of_name: "Name", comment: Optional[str] = None
    ) -> None:
        if self.nomenclature_status != NomenclatureStatus.available:
            raise ValueError(f"{self} is {self.nomenclature_status.name}")
        self.add_tag(STATUS_TO_TAG[status](name=of_name, comment=comment))
        self.nomenclature_status = status  # type: ignore
        self.save()

    def add_variant(
        self,
        root_name: str,
        status: NomenclatureStatus = NomenclatureStatus.variant,
        paper: Optional[str] = None,
        page_described: Optional[str] = None,
        original_name: Optional[str] = None,
        *,
        interactive: bool = True,
    ) -> "Name":
        if paper is not None:
            nam = self.taxon.syn_from_paper(root_name, paper, interactive=False)
            nam.original_name = original_name
            nam.nomenclature_status = status
        else:
            nam = self.taxon.add_syn(
                root_name,
                nomenclature_status=status,
                original_name=original_name,
                interactive=False,
            )
        tag_cls = STATUS_TO_TAG[status]
        nam.page_described = page_described
        nam.add_tag(tag_cls(self, ""))
        if interactive:
            nam.fill_required_fields()
        return nam

    def preoccupied_by(self, name: "Name", comment: Optional[str] = None) -> None:
        self.add_tag(Tag.PreoccupiedBy(name, comment))
        if self.nomenclature_status == NomenclatureStatus.available:
            self.nomenclature_status = NomenclatureStatus.preoccupied  # type: ignore
        else:
            print(f"not changing status because it is {self.nomenclature_status}")
        self.save()

    def conserve(self, opinion: str, comment: Optional[str] = None) -> None:
        self.add_tag(Tag.Conserved(opinion, comment))

    def get_authors(self) -> List[str]:
        return re.split(r", | & ", re.sub(r"et al\.$", "", self.authority))

    def set_authors(self, authors: List[str]) -> None:
        self.authority = helpers.unsplit_authors(authors)

    def effective_year(self) -> int:
        """Returns the effective year of validity for this name.

        Defaults to the year after the current year if the year is unknown or invalid.

        """
        if self.year is None:
            return datetime.datetime.now().year + 1
        if self.year == "in press":
            return datetime.datetime.now().year
        else:
            year_str = self.year[-4:]
            try:
                return int(year_str)
            except ValueError:
                # invalid year
                return datetime.datetime.now().year + 1

    def get_description(
        self,
        full: bool = False,
        depth: int = 0,
        include_data: bool = False,
        include_taxon: bool = False,
    ) -> str:
        if self.original_name is None:
            out = self.root_name
        else:
            out = self.original_name
        if self.authority is not None:
            out += " %s" % self.authority
        if self.year is not None:
            out += ", %s" % self.year
        if self.page_described is not None:
            out += ":%s" % self.page_described
        if self.original_citation is not None:
            out += " {%s}" % self.original_citation
        if self.type is not None:
            kind = f"; {self.genus_type_kind.name}" if self.genus_type_kind else ""
            out += f" (type: {self.type}{kind})"
        statuses = []
        if self.status != Status.valid:
            statuses.append(self.status)
        if self.nomenclature_status != NomenclatureStatus.available:
            statuses.append(self.nomenclature_status)
        if statuses:
            out += f' ({", ".join(status.name for status in statuses)})'
        if full and (
            self.original_name is not None
            or self.stem is not None
            or self.gender is not None
            or self.definition is not None
        ):
            parts = []
            if self.original_name is not None:
                parts.append(f"root: {self.root_name}")
            if (
                self.corrected_original_name is not None
                and self.corrected_original_name != self.original_name
            ):
                parts.append(f"corrected: {self.corrected_original_name}")
            if self.name_complex is not None:
                parts.append(f"name complex: {self.name_complex}")
            else:
                if self.stem is not None:
                    parts.append("stem: %s" % self.stem)
                if self.gender is not None:
                    parts.append(constants.Gender(self.gender).name)
            if self.definition is not None:
                parts.append(str(self.definition))
            out += " (%s)" % "; ".join(parts)
        if include_taxon:
            out += f" (={self.taxon})"
        knowledge_level = self.knowledge_level()
        if knowledge_level == 0:
            intro_line = getinput.red(out)
        elif knowledge_level == 1:
            intro_line = getinput.blue(out)
        else:
            intro_line = getinput.green(out)
        result = " " * ((depth + 1) * 4) + intro_line + "\n"
        if full:
            data = {
                "nomenclature_comments": self.nomenclature_comments,
                "other_comments": self.other_comments,
                "taxonomy_comments": self.taxonomy_comments,
                "verbatim_type": self.verbatim_type,
                "verbatim_citation": self.verbatim_citation,
                "type_locality_description": self.type_locality_description,
                "tags": sorted(self.tags) if self.tags else None,
            }
            if include_data:
                data["data"] = self.data
            type_info = []
            if self.species_type_kind is not None:
                type_info.append(self.species_type_kind.name)
            if self.type_specimen is not None:
                type_info.append(self.type_specimen)
            if self.collection is not None:
                type_info.append(f"in {self.collection}")
            if self.type_specimen_source is not None:
                type_info.append(f"{{{self.type_specimen_source}}}")
            if self.type_locality is not None:
                type_info.append(f"from {self.type_locality.name}")
            if type_info:
                data["type"] = "; ".join(type_info)
            result = "".join(
                [result]
                + [
                    " " * ((depth + 2) * 4) + "%s: %s\n" % (key, value)
                    for key, value in data.items()
                    if value
                ]
                + [
                    " " * ((depth + 2) * 4) + str(tag) + "\n"
                    for tag in (self.type_tags or [])
                ]
                + [
                    " " * ((depth + 2) * 4) + comment.get_description() + "\n"
                    for comment in self.comments
                    if include_data
                    or comment.kind != constants.CommentKind.structured_quote
                ]
            )
        return result

    def display(self, full: bool = True, include_data: bool = False) -> None:
        print(
            self.get_description(
                full=full, include_data=include_data, include_taxon=True
            )
        )

    def knowledge_level(self, verbose: bool = False) -> int:
        """Returns whether all necessary attributes of the name have been filled in."""
        required_fields = set(self.get_required_fields())
        if "original_citation" in required_fields and self.original_citation is None:
            if verbose:
                print("0 because no original citation")
            return 0
        for field in required_fields:
            if getattr(self, field) is None:
                if verbose:
                    print(f"1 because {field} is missing")
                return 1
        if verbose:
            print("2 because all fields are set")
        return 2

    def get_required_fields(self) -> Iterable[str]:
        if (
            self.status == Status.spurious
            or self.nomenclature_status == NomenclatureStatus.informal
        ):
            return
        yield "original_name"
        if (
            self.group in (Group.genus, Group.species)
            and self.original_name is not None
            and self.nomenclature_status
            != NomenclatureStatus.not_published_with_a_generic_name
        ):
            yield "corrected_original_name"

        yield "authority"
        yield "year"
        yield "page_described"
        yield "original_citation"
        if self.original_citation is None:
            yield "verbatim_citation"

        if (
            self.group in (Group.genus, Group.species)
            and self.nomenclature_status.requires_name_complex()
        ):
            yield "name_complex"

        if self.nomenclature_status.requires_type():
            if self.group in (Group.family, Group.genus):
                yield "type"
            if self.group == Group.species:
                yield "type_locality"
                # 75 is a special Collection that indicates there is no preserved specimen.
                if self.collection is None or (self.collection.id != 75):
                    yield "type_specimen"
                yield "collection"
                if self.type_specimen is not None or self.collection is not None:
                    yield "type_specimen_source"
                    yield "species_type_kind"
                yield "type_tags"
            if self.group == Group.genus:
                if self.type is not None:
                    yield "genus_type_kind"
                if self.original_citation is not None and (
                    self.type is None
                    or self.genus_type_kind is None
                    or self.genus_type_kind
                    == constants.TypeSpeciesDesignation.subsequent_designation
                ):
                    # for originally included species
                    yield "type_tags"

    def validate(
        self,
        status: Status = Status.valid,
        parent: Optional[Taxon] = None,
        rank: Optional[Rank] = None,
    ) -> Taxon:
        assert self.status not in (
            Status.valid,
            Status.nomen_dubium,
            Status.species_inquirenda,
        )
        old_taxon = self.taxon
        parent_group = helpers.group_of_rank(old_taxon.rank)
        if self.group == Group.species and parent_group != Group.species:
            if rank is None:
                rank = Rank.species
            if parent is None:
                parent = old_taxon
        elif self.group == Group.genus and parent_group != Group.genus:
            if rank is None:
                rank = Rank.genus
            if parent is None:
                parent = old_taxon
        elif self.group == Group.family and parent_group != Group.family:
            if rank is None:
                rank = Rank.family
            if parent is None:
                parent = old_taxon
        else:
            if rank is None:
                rank = old_taxon.rank
            if parent is None:
                parent = old_taxon.parent
        new_taxon = Taxon.create(
            rank=rank, parent=parent, age=old_taxon.age, valid_name=""
        )
        new_taxon.base_name = self
        new_taxon.valid_name = new_taxon.compute_valid_name()
        new_taxon.save()
        self.taxon = new_taxon
        self.status = status  # type: ignore
        self.save()
        return new_taxon

    def merge(self, into: "Name", allow_valid: bool = False) -> None:
        if not allow_valid:
            assert self.status in (
                Status.synonym,
                Status.dubious,
            ), f"Can only merge synonymous names (not {self})"
        self._merge_fields(into, exclude={"id"})
        self.remove()

    def open_description(self) -> bool:
        if self.original_citation is None:
            print("%s: original citation unknown" % self.description())
        else:
            try:
                ehphp.call_ehphp("openf", [self.original_citation])
            except ehphp.EHPHPError:
                pass
        return True

    def remove(self) -> None:
        print("Deleting name: " + self.description())
        self.delete_instance()

    def original_valid(self) -> None:
        assert self.original_name is None
        assert self.status == Status.valid
        self.original_name = self.taxon.valid_name

    def compute_gender(self) -> None:
        assert (
            self.group == Group.species
        ), "Cannot compute gender outside the species group"
        genus = self.taxon.parent_of_rank(Rank.genus)
        gender = genus.base_name.gender
        if gender is None:
            print("Parent genus %s does not have gender set" % genus)
            return
        computed = helpers.convert_gender(self.root_name, gender)
        if computed != self.root_name:
            print("Modifying root_name: %s -> %s" % (self.root_name, computed))
            self.root_name = computed
            self.save()

    def __str__(self) -> str:
        return self.description()

    def __repr__(self) -> str:
        return self.description()

    def set_paper(
        self,
        paper: str,
        page_described: Union[None, int, str] = None,
        original_name: Optional[int] = None,
        force: bool = False,
        **kwargs: Any,
    ) -> None:
        authority, year = ehphp.call_ehphp("taxonomicAuthority", [paper])[0]
        if original_name is None and self.status == Status.valid:
            original_name = self.taxon.valid_name
        attributes = [
            ("authority", authority),
            ("year", year),
            ("original_citation", paper),
            ("page_described", page_described),
            ("original_name", original_name),
        ]
        for label, value in attributes:
            if value is None:
                continue
            current_value = getattr(self, label)
            if current_value is not None:
                if current_value != value and current_value != str(value):
                    print(
                        "Warning: %s does not match (given as %s, paper has %s)"
                        % (label, current_value, value)
                    )
                    if force:
                        setattr(self, label, value)
            else:
                setattr(self, label, value)
        self.s(**kwargs)
        self.fill_required_fields()
        self.save()

    def detect_and_set_type(
        self, verbatim_type: Optional[str] = None, verbose: bool = False
    ) -> bool:
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        if verbose:
            print("=== Detecting type for %s from %s" % (self, verbatim_type))
        candidates = self.detect_type(verbatim_type=verbatim_type, verbose=verbose)
        if candidates is None or not candidates:
            print(
                "Verbatim type %s for name %s could not be recognized"
                % (verbatim_type, self)
            )
            return False
        elif len(candidates) == 1:
            if verbose:
                print("Detected type: %s" % candidates[0])
            self.type = candidates[0]
            self.save()
            return True
        else:
            print(
                "Verbatim type %s for name %s yielded multiple possible names: %s"
                % (verbatim_type, self, candidates)
            )
            return False

    def detect_type(
        self, verbatim_type: Optional[str] = None, verbose: bool = False
    ) -> List["Name"]:

        def cleanup(name: str) -> str:
            return re.sub(
                r"\s+",
                " ",
                name.strip().rstrip(".").replace("<i>", "").replace("</i>", ""),
            )

        steps = [
            lambda verbatim: verbatim,
            lambda verbatim: re.sub(r"\([^)]+\)", "", verbatim),
            lambda verbatim: re.sub(r"=.*$", "", verbatim),
            lambda verbatim: re.sub(r"\(.*$", "", verbatim),
            lambda verbatim: re.sub(r"\[.*$", "", verbatim),
            lambda verbatim: re.sub(r",.*$", "", verbatim),
            lambda verbatim: self._split_authority(verbatim)[0],
            lambda verbatim: verbatim.split()[1] if " " in verbatim else verbatim,
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.Gender.masculine
            ),
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.Gender.feminine
            ),
            lambda verbatim: helpers.convert_gender(verbatim, constants.Gender.neuter),
        ]
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        candidates = None
        for step in steps:
            new_verbatim = cleanup(step(verbatim_type))
            if verbatim_type != new_verbatim or candidates is None:
                if verbose:
                    print("Trying verbatim type: %s" % new_verbatim)
                verbatim_type = new_verbatim
                candidates = self.detect_type_from_verbatim_type(verbatim_type)
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _split_authority(verbatim_type: str) -> Tuple[str, Optional[str]]:
        # if there is an uppercase letter following an all-lowercase word (the species name),
        # the authority is included
        find_authority = re.match(r"^(.* [a-z]+) ([A-Z+].+)$", verbatim_type)
        if find_authority:
            return find_authority.group(1), find_authority.group(2)
        else:
            return verbatim_type, None

    def detect_type_from_verbatim_type(self, verbatim_type: str) -> List["Name"]:

        def _filter_by_authority(
            candidates: List["Name"], authority: Optional[str]
        ) -> List["Name"]:
            if authority is None:
                return candidates
            split = re.split(r", (?=\d)", authority, maxsplit=1)
            if len(split) == 1:
                author, year = authority, None
            else:
                author, year = split
            result = []
            for candidate in candidates:
                if candidate.authority != author:
                    continue
                if year is not None and candidate.year != year:
                    continue
                result.append(candidate)
            return result

        parent = self.taxon
        if self.group == Group.family:
            verbatim = verbatim_type.split(maxsplit=1)
            if len(verbatim) == 1:
                type_name, authority = verbatim[0], None
            else:
                type_name, authority = verbatim
            return _filter_by_authority(
                parent.find_names(verbatim[0], group=Group.genus), authority
            )
        else:
            type_name, authority = self._split_authority(verbatim_type)
            if " " not in type_name:
                root_name = type_name
                candidates = Name.filter(
                    Name.root_name == root_name, Name.group == Group.species
                )
                find_abbrev = False
            else:
                match = re.match(r"^[A-Z]\. ([a-z]+)$", type_name)
                find_abbrev = bool(match)
                if match:
                    root_name = match.group(1)
                    candidates = Name.filter(
                        Name.root_name == root_name, Name.group == Group.species
                    )
                else:
                    candidates = Name.filter(
                        Name.original_name == type_name, Name.group == Group.species
                    )
            # filter by authority first because it's cheaper
            candidates = _filter_by_authority(candidates, authority)
            candidates = [
                candidate
                for candidate in candidates
                if candidate.taxon.is_child_of(parent)
            ]
            # if we failed to find using the original_name, try the valid_name
            if not candidates and not find_abbrev:
                candidates = (
                    Name.filter(Name.status == Status.valid)
                    .join(Taxon)
                    .where(Taxon.valid_name == type_name)
                )
                candidates = _filter_by_authority(candidates, authority)
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.taxon.is_child_of(parent)
                ]
            return candidates

    @classmethod
    def find_name(
        cls,
        name: str,
        rank: Optional[Rank] = None,
        authority: Optional[str] = None,
        year: Union[None, int, str] = None,
    ) -> "Name":
        """Find a Name object corresponding to the given information."""
        if rank is None:
            group = None
            initial_lst = cls.select().where(cls.root_name == name)
        else:
            group = helpers.group_of_rank(rank)
            if group == Group.family:
                root_name = helpers.strip_rank(name, rank, quiet=True)
            else:
                root_name = name
            initial_lst = cls.select().where(
                cls.root_name == root_name, cls.group == group
            )
        for nm in initial_lst:
            if authority is not None and nm.authority and nm.authority != authority:
                continue
            if year is not None and nm.year and nm.year != year:
                continue
            if group == Group.family:
                if (
                    nm.original_name
                    and nm.original_name != name
                    and initial_lst.count() > 1
                ):
                    continue
            return nm
        raise cls.DoesNotExist


class Occurrence(BaseModel):
    taxon = ForeignKeyField(Taxon, related_name="occurrences", db_column="taxon_id")
    location = ForeignKeyField(Location, related_name="taxa", db_column="location_id")
    comment = CharField()
    status = EnumField(OccurrenceStatus, default=OccurrenceStatus.valid)
    source = CharField()

    def add_comment(self, new_comment: str) -> None:
        if self.comment is None:
            self.comment = new_comment
        else:
            self.comment += " " + new_comment
        self.save()

    def __repr__(self) -> str:
        out = "%s in %s (%s%s)" % (
            self.taxon,
            self.location,
            self.source,
            "; " + self.comment if self.comment else "",
        )
        if self.status != OccurrenceStatus.valid:
            out = "[%s] %s" % (self.status.name.upper(), out)
        return out


class NameComment(BaseModel):
    name = ForeignKeyField(Name, related_name="comments", db_column="name_id")
    kind = EnumField(constants.CommentKind)
    date = IntegerField()
    text = TextField()
    source = CharField()
    page = TextField()

    class Meta:
        db_table = "name_comment"

    @classmethod
    def make(
        cls,
        name: Name,
        kind: constants.CommentKind,
        text: str,
        source: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
        return cls.create(
            name=name,
            kind=kind,
            text=text,
            date=int(time.time()),
            source=source,
            page=page,
        )

    @classmethod
    def create_interactively(
        cls,
        name: Optional[Name] = None,
        kind: Optional[constants.CommentKind] = None,
        text: Optional[str] = None,
        source: Optional[str] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
        if name is None:
            name = cls.get_value_for_foreign_key_field_on_class("name")
        assert name is not None
        if kind is None:
            kind = getinput.get_enum_member(
                constants.CommentKind, prompt="kind> ", allow_empty=False
            )
        if text is None:
            text = getinput.get_line(prompt="text> ")
        assert text is not None
        if source is None:
            source = cls.get_value_for_article_field("source")
            if page is None:
                page = getinput.get_line(prompt="page> ")
        return cls.make(name=name, kind=kind, text=text, source=source, page=page)

    def get_description(self) -> str:
        components = [
            self.kind.name,
            datetime.datetime.fromtimestamp(self.date).strftime("%b %d, %Y %H:%M:%S"),
        ]
        if self.source:
            components.append(
                f"{{{self.source}}}:{self.page}" if self.page else f"{{{self.source}}}"
            )
        return f'{self.text} ({"; ".join(components)})'


class Tag(adt.ADT):
    PreoccupiedBy(name=Name, comment=str, tag=1)  # type: ignore
    UnjustifiedEmendationOf(name=Name, comment=str, tag=2)  # type: ignore
    JustifiedEmendationOf(name=Name, comment=str, tag=3)  # type: ignore
    IncorrectSubsequentSpellingOf(name=Name, comment=str, tag=4)  # type: ignore
    NomenNovumFor(name=Name, comment=str, tag=5)  # type: ignore
    # If we don't know which of 2-4 to use
    VariantOf(name=Name, comment=str, tag=6)  # type: ignore
    # "opinion" is a reference to an Article containing an ICZN Opinion
    PartiallySuppressedBy(opinion=str, comment=str, tag=7)  # type: ignore
    FullySuppressedBy(opinion=str, comment=str, tag=8)  # type: ignore
    TakesPriorityOf(name=Name, comment=str, tag=9)  # type: ignore
    # ICZN Art. 23.9. The reference is to the nomen protectum relative to which precedence is reversed.
    NomenOblitum(name=Name, comment=str, tag=10)  # type: ignore
    MandatoryChangeOf(name=Name, comment=str, tag=11)  # type: ignore
    # Conserved by placement on the Official List.
    Conserved(opinion=str, comment=str, tag=12)  # type: ignore
    IncorrectOriginalSpellingOf(name=Name, comment=str, tag=13)  # type: ignore
    # selection as the correct original spelling
    SelectionOfSpelling(source=str, comment=str, tag=14)  # type: ignore
    SubsequentUsageOf(name=Name, comment=str, tag=15)  # type: ignore
    SelectionOfPriority(over=Name, source=str, comment=str, tag=16)  # type: ignore
    # Priority reversed by ICZN opinion
    ReversalOfPriority(over=Name, opinion=str, comment=str, tag=17)  # type: ignore


STATUS_TO_TAG = {
    NomenclatureStatus.unjustified_emendation: Tag.UnjustifiedEmendationOf,
    NomenclatureStatus.justified_emendation: Tag.JustifiedEmendationOf,
    NomenclatureStatus.incorrect_subsequent_spelling: Tag.IncorrectSubsequentSpellingOf,
    NomenclatureStatus.variant: Tag.VariantOf,
    NomenclatureStatus.mandatory_change: Tag.MandatoryChangeOf,
    NomenclatureStatus.nomen_novum: Tag.NomenNovumFor,
    NomenclatureStatus.incorrect_original_spelling: Tag.IncorrectOriginalSpellingOf,
    NomenclatureStatus.subsequent_usage: Tag.SubsequentUsageOf,
    NomenclatureStatus.preoccupied: Tag.PreoccupiedBy,
}


class TypeTag(adt.ADT):
    Collector(name=str, tag=1)  # type: ignore
    Date(date=str, tag=2)  # type: ignore
    Gender(gender=constants.SpecimenGender, tag=3)  # type: ignore
    Age(age=constants.SpecimenAge, tag=4)  # type: ignore
    Organ(organ=constants.Organ, detail=str, condition=str, tag=5)  # type: ignore
    Altitude(altitude=str, unit=constants.AltitudeUnit, tag=6)  # type: ignore
    Coordinates(latitude=str, longitude=str, tag=7)  # type: ignore
    # Authoritative description for a disputed type locality. Should be rarely used.
    TypeLocality(text=str, tag=8)  # type: ignore
    StratigraphyDetail(text=str, tag=9)  # type: ignore
    Habitat(text=str, tag=10)  # type: ignore
    Host(name=str, tag=11)  # type: ignore
    # 12 is unused
    # subsequent designation of the type (for a genus)
    TypeDesignation(source=str, type=Name, comment=str, tag=13)  # type: ignore
    # like the above, but by the Commission (and therefore trumping everything else)
    CommissionTypeDesignation(opinion=str, type=Name, tag=14)  # type: ignore
    LectotypeDesignation(  # type: ignore
        source=str, lectotype=str, valid=bool, comment=str, tag=15
    )
    NeotypeDesignation(  # type: ignore
        source=str, neotype=str, valid=bool, comment=str, tag=16
    )
    # more information on the specimen
    SpecimenDetail(text=str, source=str, tag=17)  # type: ignore
    # phrasing of the type locality in a particular source
    LocationDetail(text=str, source=str, tag=18)  # type: ignore
    # an originally included species in a genus without an original type designation
    IncludedSpecies(name=Name, comment=str, tag=19)  # type: ignore
    # repository that holds some of the type specimens
    Repository(repository=Collection, tag=20)  # type: ignore
