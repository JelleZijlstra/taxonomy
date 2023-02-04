import peewee
from pyanalyze.value import GenericValue, KnownValue, TypedValue, Value

from taxonomy.db.models.base import ADTField, EnumField


def class_attribute_transformer(field_cls: peewee.Field) -> tuple[Value, Value] | None:
    if not isinstance(field_cls, peewee.Field):
        return None
    val: Value
    if isinstance(field_cls, EnumField):
        val = TypedValue(field_cls.enum_cls)
    elif isinstance(field_cls, ADTField):
        val = GenericValue(tuple, [TypedValue(field_cls.adt_cls())])
    elif isinstance(field_cls, (peewee.TextField, peewee.CharField)):
        val = TypedValue(str)
    elif isinstance(field_cls, peewee.AutoField):
        val = TypedValue(int)
    elif isinstance(field_cls, peewee.ForeignKeyField):
        val = TypedValue(field_cls.rel_model)
    elif isinstance(field_cls, peewee.BooleanField):
        val = TypedValue(bool)
    elif isinstance(field_cls, peewee.IntegerField):
        val = TypedValue(int)
    else:
        raise NotImplementedError(field_cls)

    if field_cls.null:
        val = val | KnownValue(None)
    return val, val
