import clorm
from pyanalyze.annotations import type_from_runtime
from pyanalyze.value import Value


def class_attribute_transformer(field_cls: clorm.Field) -> tuple[Value, Value] | None:
    if not isinstance(field_cls, clorm.Field):
        return None
    val = type_from_runtime(field_cls.full_type)
    return val, val
