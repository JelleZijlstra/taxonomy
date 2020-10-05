"""

Implementation of pre-computed derived data.

"""
from dataclasses import dataclass
from functools import lru_cache
import pickle
from typing import Any, Dict, Generic, Optional, TypeVar, Type
from typing_extensions import Protocol

from .. import config

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

settings = config.get_options()

ObjectData = Dict[str, Any]  # derived data for a single object, keys are field names
ModelData = Dict[int, ObjectData]  # keys are object ids
DerivedData = Dict[str, ModelData]  # keys are model call signs


class SetLater:
    pass


class SingleComputeFunc(Protocol[T_co]):
    def __call__(self, model: "models.base.BaseModel") -> T_co:
        ...


class ComputeAllFunc(Protocol[T]):
    def __call__(self) -> Dict[int, T]:
        ...


@dataclass
class DerivedField(Generic[T]):
    name: str
    typ: Type[T]
    compute: Optional[SingleComputeFunc[T]] = None
    compute_all: Optional[ComputeAllFunc[T]] = None
    pull_on_miss: bool = True

    def get_value(self, model: "models.base.BaseModel") -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if self.name in object_data:
                return self.deserialize(object_data[self.name])
            print(f"Cache miss on {model} {self.name}")
            assert self.compute is not None, "compute must be set for pull-on-miss field"
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return value
        else:
            return self.deserialize(object_data.get(self.name))

    def get_raw_value(self, model: "models.base.BaseModel") -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if self.name in object_data:
                return object_data[self.name]
            print(f"Cache miss on {model} {self.name}")
            assert self.compute is not None, "compute must be set for pull-on-miss field"
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return object_data[self.name]
        else:
            return object_data.get(self.name)  # type: ignore

    def set_value(self, model: "models.base.BaseModel", value: T) -> None:
        data = load_derived_data()
        data.setdefault(model.call_sign, {}).setdefault(model.id, {})[
            self.name
        ] = self.serialize(value)

    def serialize(self, value: T) -> Any:
        if isinstance(value, models.base.BaseModel):
            return value.id
        return value

    def deserialize(self, serialized: Any) -> T:
        if (
            serialized is not None
            and isinstance(self.typ, type)
            and issubclass(self.typ, models.base.BaseModel)
        ):
            return self.typ.select_valid().filter(self.typ.id == serialized).get()
        return serialized

    def compute_and_store_all(self, model_cls: Type["models.base.BaseModel"]) -> None:
        data = load_derived_data()
        model_data = data.setdefault(model_cls.call_sign, {})
        if self.compute_all is not None:
            field_data = self.compute_all()
        else:
            assert self.compute is not None
            field_data = {obj.id: self.compute(obj) for obj in model_cls.select_valid()}
        for model_id, value in field_data.items():
            object_data = model_data.setdefault(model_id, {})
            object_data[self.name] = self.serialize(value)


@lru_cache()
def load_derived_data() -> DerivedData:
    try:
        with settings.derived_data_filename.open("rb") as f:
            return pickle.load(f)
    except (FileNotFoundError, EOFError):
        return {}


def write_derived_data(data: DerivedData) -> None:
    with settings.derived_data_filename.open("wb") as f:
        pickle.dump(data, f)


from . import models
