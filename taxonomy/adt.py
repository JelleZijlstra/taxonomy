import builtins
import collections.abc
import enum
import sys
from typing import (TYPE_CHECKING, Any, Dict, Iterable, Iterator, List,
                    MutableMapping, Type, TypeVar)

BASIC_TYPES = (int, str, float, bool, list)


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


class _ADTNamespace(MutableMapping[str, Any]):
    def __init__(self, globals: Dict[str, Any]) -> None:
        self._globals = globals
        self._mapping: Dict[str, _ADTMember] = {}

    def __getitem__(self, key: str) -> Any:
        if key in self._mapping:
            return self._mapping[key]
        elif key in self._globals:
            return self._globals[key]
        elif hasattr(builtins, key):
            return getattr(builtins, key)
        elif key.startswith('__'):
            raise KeyError(key)
        member = _ADTMember(key)
        self._mapping[key] = member
        return member

    def __contains__(self, key: object) -> bool:
        return key in self._mapping

    def __setitem__(self, key: str, value: Any) -> None:
        self._mapping[key] = value

    def __delitem__(self, key: str) -> None:
        del self._mapping[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping)

    def __len__(self) -> int:
        return len(self._mapping)


def _adt_member_eq(self: Any, other: Any) -> Any:
    if not isinstance(other, self._adt_cls):
        return NotImplemented
    if not isinstance(other, type(self)):
        return False
    for attr in self._attributes.keys():
        if getattr(self, attr) != getattr(other, attr):
            return False
    return True


def _none_safe_lt(left: Any, right: Any) -> bool:
    if left is None:
        return right is not None
    elif right is None:
        return False
    else:
        return left < right


def _adt_member_lt(self: Any, other: Any) -> Any:
    if not isinstance(other, self._adt_cls):
        return NotImplemented
    if not isinstance(other, type(self)):
        return type(self).__name__ < type(other).__name__
    for attr in self._attributes.keys():
        if _none_safe_lt(getattr(self, attr), getattr(other, attr)):
            return True
    return False


def _adt_member_hash(self: Any) -> int:
    return hash((type(self), tuple(getattr(self, attr) for attr in self._attributes)))


class _ADTMeta(type):
    @classmethod
    def __prepare__(cls, name: str, bases: Any) -> _ADTNamespace:
        return _ADTNamespace(sys._getframe(1).f_globals)

    def __new__(cls, name: str, bases: Any, ns: Any) -> Type[Any]:
        if '_is_member' in ns and ns['_is_member']:
            return super().__new__(cls, name, bases, ns)
        members = {}
        for key, value in list(ns.items()):
            if isinstance(value, _ADTMember):
                members[key] = value
                del ns[key]
        new_cls = super().__new__(cls, name, bases, dict(ns.items(), _members=tuple(members.keys())))
        new_cls._tag_to_member = {}  # type: ignore
        if name in members and not members[name].called:
            del members[name]
            has_self_cls = True
        else:
            has_self_cls = False
        for member in members.values():
            if not member.called:
                raise TypeError(f'incomplete member {member}')
            has_args = bool(member.kwargs)
            attrs: Dict[str, Type[Any]] = {}
            member_ns = {
                '_attributes': attrs,
                '_tag': member.tag,
                '_has_args': has_args,
                '_is_member': True,
                '_adt_cls': new_cls,
                '__eq__': _adt_member_eq,
                '__lt__': _adt_member_lt,
                '__hash__': _adt_member_hash,
            }
            if has_args:
                for key, value in member.kwargs.items():
                    if value in BASIC_TYPES:
                        attrs[key] = value
                    elif isinstance(value, type) and issubclass(value, enum.IntEnum):
                        attrs[key] = value
                    elif isinstance(value, type) and hasattr(value, 'serialize') and hasattr(value, 'unserialize'):
                        attrs[key] = value
                    elif has_self_cls and isinstance(value, _ADTMember) and value.name == name:
                        attrs[key] = new_cls
                    else:
                        raise TypeError(f'unsupported type {value}')
                lines = ''.join(f'    self.{attr} = {attr}\n' for attr in member.kwargs.keys())
                code = f'def __init__(self, {", ".join(member.kwargs.keys())}):\n{lines}'
                new_ns: Dict[str, Any] = {}
                exec(code, {}, new_ns)
                member_ns['__init__'] = new_ns['__init__']
            member_cls = type(member.name, (new_cls,), member_ns)
            if not has_args:
                cls_obj = member_cls
                member_cls = cls_obj()

                def __init__(self: object) -> None:
                    raise TypeError(f'cannot instantiate {member_cls}')
                cls_obj.__init__ = __init__  # type: ignore
            new_cls._tag_to_member[member.tag] = member_cls  # type: ignore
            setattr(new_cls, member.name, member_cls)
        return new_cls


_ADTT = TypeVar('_ADTT', bound='ADT')

if TYPE_CHECKING:
    class _ADTBase(Any):
        pass
else:
    class _ADTBase:
        pass


class ADT(_ADTBase, metaclass=_ADTMeta):
    _attributes: Dict[str, Type[Any]]
    _has_args: bool
    _tag: int
    _tag_to_member: Dict[int, Type[Any]]

    def _get_attributes(self) -> Iterable[Any]:
        for attr in self._attributes.keys():
            yield getattr(self, attr)

    def serialize(self) -> Any:
        if self._has_args:
            args = []
            for value in self._get_attributes():
                if hasattr(value, 'serialize'):
                    args.append(value.serialize())
                elif isinstance(value, enum.IntEnum):
                    args.append(value.value)
                else:
                    args.append(value)
            return [self._tag, *args]
        else:
            return [self._tag]

    @classmethod
    def unserialize(cls: Type[_ADTT], value: List[Any]) -> _ADTT:
        tag = value[0]
        member_cls = cls._tag_to_member[tag]
        if member_cls._has_args:
            args = []
            for arg_type, serialized in zip(member_cls._attributes.values(), value[1:]):
                if hasattr(arg_type, 'unserialize'):
                    args.append(arg_type.unserialize(serialized))
                elif isinstance(arg_type, type) and issubclass(arg_type, enum.IntEnum):
                    args.append(arg_type(serialized))
                else:
                    args.append(serialized)
            return member_cls(*args)
        else:
            return member_cls  # type: ignore

    def __repr__(self) -> str:
        member_name = type(self).__name__
        if not self._has_args:
            return member_name
        else:
            args = ', '.join(map(repr, self._get_attributes()))
            return f'{member_name}({args})'
