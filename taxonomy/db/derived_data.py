"""

Implementation of pre-computed derived data.

"""
from dataclasses import dataclass
import enum
from functools import lru_cache
import pickle
from typing import Any, Dict, Generic, Optional, TypeVar, Type, Union
from typing import Protocol
import typing_inspect
import taxonomy

from .. import config

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

settings = config.get_options()

ObjectData = dict[str, Any]  # derived data for a single object, keys are field names
ModelData = dict[int, ObjectData]  # keys are object ids
DerivedData = dict[str, ModelData]  # keys are model call signs
CachedData = dict[str, Any]


class SetLater:
    pass


class SingleComputeFunc(Protocol[T_co]):
    def __call__(self, model: "taxonomy.db.models.base.BaseModel") -> T_co:
        ...


class ComputeAllFunc(Protocol[T]):
    def __call__(self) -> dict[int, T]:
        ...


class _LazyTypeArg(Protocol[T_co]):
    def __call__(self) -> type[T_co]:
        ...


@dataclass
class LazyType(Generic[T]):
    typ: _LazyTypeArg[T]


@dataclass
class DerivedField(Generic[T]):
    name: str
    typ: type[T] | LazyType[T]
    compute: SingleComputeFunc[T] | None = None
    compute_all: ComputeAllFunc[T] | None = None
    pull_on_miss: bool = True

    def get_value(
        self, model: "taxonomy.db.models.base.BaseModel", force_recompute: bool = False
    ) -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if not force_recompute and self.name in object_data:
                return self.deserialize(object_data[self.name], self.get_type())
            assert (
                self.compute is not None
            ), "compute must be set for pull-on-miss field"
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return value
        else:
            return self.deserialize(object_data.get(self.name), self.get_type())

    def get_raw_value(
        self, model: "taxonomy.db.models.base.BaseModel", force_recompute: bool = False
    ) -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if not force_recompute and self.name in object_data:
                return object_data[self.name]
            assert (
                self.compute is not None
            ), "compute must be set for pull-on-miss field"
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return object_data[self.name]
        else:
            return object_data.get(self.name)  # type: ignore

    def set_value(self, model: "taxonomy.db.models.base.BaseModel", value: T) -> None:
        data = load_derived_data()
        data.setdefault(model.call_sign, {}).setdefault(model.id, {})[
            self.name
        ] = self.serialize(value)

    def serialize(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self.serialize(elt) for elt in value]
        if isinstance(value, enum.Enum):
            return value.value
        if isinstance(value, taxonomy.db.models.base.BaseModel):
            return value.id
        return value

    def deserialize(self, serialized: Any, typ: Any) -> Any:
        if serialized is None:
            return None
        if isinstance(typ, type):
            if issubclass(typ, taxonomy.db.models.base.BaseModel):
                # Not select_valid(), we'll filter out deleted names the next time
                # we regenerate the derived data.
                return typ.select().filter(typ.id == serialized).get()
            elif issubclass(typ, enum.Enum):
                return typ(serialized)
        if (
            typing_inspect.is_generic_type(typ)
            and typing_inspect.get_origin(typ) is list
        ):
            (arg_type,) = typing_inspect.get_args(typ)
            return [self.deserialize(elt, arg_type) for elt in serialized]
        return serialized

    def get_type(self) -> type[T]:
        if isinstance(self.typ, LazyType):
            self.typ = self.typ.typ()
        return self.typ

    def compute_and_store_all(
        self, model_cls: type["taxonomy.db.models.base.BaseModel"]
    ) -> None:
        data = load_derived_data()
        model_data = data.setdefault(model_cls.call_sign, {})
        if self.compute_all is not None:
            field_data = self.compute_all()
            # First remove all data for this field
            for _, object_data in model_data.items():
                if self.name in object_data:
                    del object_data[self.name]
        else:
            assert self.compute is not None
            field_data = {obj.id: self.compute(obj) for obj in model_cls.select_valid()}
        for model_id, value in field_data.items():
            object_data = model_data.setdefault(model_id, {})
            object_data[self.name] = self.serialize(value)


@lru_cache
def load_derived_data() -> DerivedData:
    try:
        with settings.derived_data_filename.open("rb") as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        return {}


def write_derived_data(data: DerivedData) -> None:
    with settings.derived_data_filename.open("wb") as f:
        pickle.dump(data, f)


@lru_cache
def load_cached_data() -> CachedData:
    try:
        with settings.cached_data_filename.open("rb") as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        return {}


def write_cached_data(data: CachedData) -> None:
    with settings.cached_data_filename.open("wb") as f:
        pickle.dump(data, f)
