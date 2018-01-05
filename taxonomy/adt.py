import builtins
import collections.abc
import sys
from typing import Any, Iterable, Type


class _ADTMember:
    def __init__(self, name: str) -> None:
        self.name = name
        self.called = False

    def __call__(self, *, tag: int, **kwargs: Type[Any]) -> None:
        self.tag = tag
        self.kwargs = kwargs
        self.called = True

    def __repr__(self) -> str:
        return f'<_ADTMember: name={self.name}, called={self.called}>'


class _ADTNamespace(collections.MutableMapping):
    def __init__(self, globals) -> None:
        self._globals = globals
        self._mapping = {}

    def __getitem__(self, key):
        if key in self._mapping:
            return self._mapping[key]
        elif key in self._globals:
            return self._globals[key]
        elif hasattr(builtins, key):
            return getattr(builtins, key)
        member = _ADTMember(key)
        self._mapping[key] = member
        return member

    def __contains__(self, key):
        return key in self._mapping

    def __setitem__(self, key, value):
        self._mapping[key] = value

    def __delitem__(self, key):
        del self._mapping[key]

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)


def _adt_member_eq(self: Any, other: Any) -> Any:
    if not isinstance(other, type(self)):
        return NotImplemented
    for attr in self._attributes.keys():
        if getattr(self, attr) != getattr(other, attr):
            return False
    return True


class _ADTMeta(type):
    @classmethod
    def __prepare__(cls, name, bases):
        return _ADTNamespace(sys._getframe(1).f_globals)

    def __new__(cls, name, bases, ns):
        if '_is_member' in ns and ns['_is_member']:
            return super().__new__(cls, name, bases, ns)
        members = {}
        for key, value in list(ns.items()):
            if isinstance(value, _ADTMember):
                members[key] = value
                del ns[key]
        new_cls = super().__new__(cls, name, bases, dict(ns.items()))
        new_cls._tag_to_member = {}
        if name in members and not members[name].called:
            del members[name]
            has_self_cls = True
        else:
            has_self_cls = False
        for member in members.values():
            if not member.called:
                raise TypeError(f'incomplete member {member}')
            has_args = bool(member.kwargs)
            attrs = {}
            member_ns = {
                '_attributes': attrs,
                '_tag': member.tag,
                '_has_args': has_args,
                '_is_member': True,
                '__eq__': _adt_member_eq,
            }
            if has_args:
                for key, value in member.kwargs.items():
                    if value in (int, str, float, bool, list):
                        attrs[key] = value
                    elif isinstance(value, type) and hasattr(value, 'serialize') and hasattr(value, 'unserialize'):
                        attrs[key] = value
                    elif has_self_cls and isinstance(value, _ADTMember) and value.name == name:
                        attrs[key] = new_cls
                    else:
                        raise TypeError(f'unsupported type {value}')
                lines = ''.join(f'    self.{attr} = {attr}\n' for attr in member.kwargs.keys())
                code = f'def __init__(self, {", ".join(member.kwargs.keys())}):\n{lines}'
                new_ns = {}
                exec(code, {}, new_ns)
                member_ns['__init__'] = new_ns['__init__']
            member_cls = type(member.name, (new_cls,), member_ns)
            if not has_args:
                cls_obj = member_cls
                member_cls = cls_obj()

                def __init__(self) -> None:
                    raise TypeError(f'cannot instantiate {member_cls}')
                cls_obj.__init__ = __init__
            new_cls._tag_to_member[member.tag] = member_cls
            setattr(new_cls, member.name, member_cls)
        return new_cls


class ADT(metaclass=_ADTMeta):
    def _get_attributes(self) -> Iterable[Any]:
        for attr in self._attributes.keys():
            yield getattr(self, attr)

    def serialize(self) -> Any:
        if self._has_args:
            args = []
            for value in self._get_attributes():
                if hasattr(value, 'serialize'):
                    args.append(value.serialize())
                else:
                    args.append(value)
            return [self._tag, *args]
        else:
            return [self._tag]

    @classmethod
    def unserialize(cls, value):
        tag = value[0]
        member_cls = cls._tag_to_member[tag]
        if member_cls._has_args:
            args = []
            for value, serialized in zip(member_cls._attributes.values(), value[1:]):
                if hasattr(value, 'unserialize'):
                    args.append(value.unserialize(serialized))
                else:
                    args.append(serialized)
            return member_cls(*args)
        else:
            return member_cls

    def __repr__(self) -> str:
        member_name = type(self).__name__
        if not self._has_args:
            return member_name
        else:
            args = ', '.join(map(repr, self._get_attributes()))
            return f'{member_name}({args})'
