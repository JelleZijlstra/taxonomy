import peewee


def treat_class_attribute_as_any(cls_val: object) -> bool:
    # TODO: use https://github.com/quora/pyanalyze/pull/585
    return isinstance(cls_val, peewee.Field)
