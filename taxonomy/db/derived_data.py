"""

Implementation of pre-computed derived data.

"""
from dataclasses import dataclass
from functools import lru_cache
import pickle
from typing import Any, Callable, Dict, Generic, Optional, TypeVar, Type

from .. import config

T = TypeVar("T")

settings = config.get_options()

ObjectData = Dict[str, Any]  # derived data for a single object, keys are field names
ModelData = Dict[int, ObjectData]  # keys are object ids
DerivedData = Dict[str, ModelData]  # keys are model call signs


class SetLater:
    pass


@dataclass
class DerivedField:
    name: str
    typ: Type[T]
    compute: Callable[["models.base.BaseModel"], T]
    pull_on_miss: bool = True

    def get_value(self, model: "models.baseBaseModel") -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if self.name in object_data:
                return self.deserialize(object_data[self.name])
            print(f"Cache miss on {model} {self.name}")
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return value
        else:
            return self.deserialize(object_data.get(self.name))

    def get_raw_value(self, model: "models.baseBaseModel") -> T:
        data = load_derived_data()
        model_data = data.setdefault(model.call_sign, {})
        object_data = model_data.setdefault(model.id, {})
        if self.pull_on_miss:
            if self.name in object_data:
                return object_data[self.name]
            print(f"Cache miss on {model} {self.name}")
            value = self.compute(model)
            object_data[self.name] = self.serialize(value)
            return object_data[self.name]
        else:
            return object_data.get(self.name)

    def set_value(self, model: "models.base.BaseModel", value: T) -> None:
        data = load_derived_data()
        data.setdefault(model.call_sign, {}).setdefault(model.id, {})[self.name] = self.serialize(value)

    def serialize(self, value: T) -> Any:
        if isinstance(value, models.base.BaseModel):
            return value.id
        return value

    def deserialize(self, serialized: Any) -> T:
        if serialized is not None and isinstance(self.typ, type) and issubclass(self.typ, models.base.BaseModel):
            return self.typ.select_valid().filter(self.typ.id == serialized).get()
        return serialized


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
