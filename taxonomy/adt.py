import builtins
import enum
import functools
import operator
import sys
import typing
from collections.abc import Callable, Iterable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar

BASIC_TYPES: tuple[type[Any], ...] = (int, str, float, bool)


class _ADTMember:
    def __init__(self, name: str) -> None:
        self.name = name
        self.called = False

    def __call__(self, *, tag: int, **kwargs: type[Any]) -> None:
        self.tag = tag
        self.kwargs = kwargs
        self.called = True

    def __repr__(self) -> str:
        return f"<_ADTMember: name={self.name}, called={self.called}>"


class _ADTNamespace(MutableMapping[str, Any]):
    def __init__(self, globals_dict: dict[str, Any]) -> None:
        self._globals = globals_dict
        self._mapping: dict[str, _ADTMember] = {}

    def __getitem__(self, key: str) -> Any:
        if key in self._mapping:
            return self._mapping[key]
        elif key in self._globals:
            return self._globals[key]
        elif hasattr(builtins, key):
            return getattr(builtins, key)
        elif key.startswith("__"):
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
    return all(getattr(self, attr) == getattr(other, attr) for attr in self._attributes)


def _none_safe_lt(left: Any, right: Any) -> bool:
    if left is None:
        return right is not None
    elif right is None:
        return False
    elif {type(left), type(right)} == {int, str}:
        return str(left) < str(right)
    else:
        return left < right


def _adt_member_lt(self: Any, other: Any) -> Any:
    if not isinstance(other, self._adt_cls):
        return NotImplemented
    if not isinstance(other, type(self)):
        return type(self).__name__ < type(other).__name__
    for attr in self._attributes:
        left_attr = getattr(self, attr)
        right_attr = getattr(other, attr)
        if left_attr == right_attr:
            continue
        return _none_safe_lt(left_attr, right_attr)
    return False


def _adt_member_hash(self: Any) -> int:
    return hash((type(self), tuple(getattr(self, attr) for attr in self._attributes)))


def _adt_member_replace(self: Any, **kwargs: Any) -> Any:
    new_dict = dict(self.__dict__)
    for key, value in kwargs.items():
        if key not in new_dict:
            raise TypeError(f"{type(self)} does not support field {key}")
        new_dict[key] = value
    return type(self)(**new_dict)


class _ADTMeta(type):
    @classmethod
    def __prepare__(cls, name: str, bases: Any) -> _ADTNamespace:  # type: ignore[override]
        return _ADTNamespace(sys._getframe(1).f_globals)

    def __new__(cls, name: str, bases: Any, ns: Any) -> Any:
        if "_is_member" in ns and ns["_is_member"]:  # noqa: RUF019
            return super().__new__(cls, name, bases, ns)
        members = {}
        for key, value in list(ns.items()):
            if isinstance(value, _ADTMember):
                members[key] = value
                del ns[key]
        new_cls = super().__new__(
            cls, name, bases, dict(ns.items(), _members=tuple(members))
        )
        new_cls._tag_to_member = {}  # type: ignore[attr-defined]
        if name in members and not members[name].called:
            del members[name]
            has_self_cls = True
        else:
            has_self_cls = False
        constructors = []
        for member in members.values():
            if not member.called:
                raise TypeError(f"incomplete member {member}")
            has_args = bool(member.kwargs)
            attrs: dict[str, type[Any]] = {}
            annotations: dict[str, Any] = {}
            required_attrs: set[str] = set()
            optional_attrs: set[str] = set()
            member_ns = {
                "_attributes": attrs,
                "_tag": member.tag,
                "_has_args": has_args,
                "_is_member": True,
                "_adt_cls": new_cls,
                "__eq__": _adt_member_eq,
                "__lt__": _adt_member_lt,
                "__hash__": _adt_member_hash,
                "replace": _adt_member_replace,
                "__annotations__": annotations,
                "__required_attrs__": required_attrs,
                "__optional_attrs__": optional_attrs,
            }
            if has_args:
                for key, value in member.kwargs.items():
                    origin = typing.get_origin(value)
                    if origin is typing.Required:
                        (value,) = typing.get_args(value)
                        required = True
                    elif origin is typing.NotRequired:
                        (value,) = typing.get_args(value)
                        required = False
                    else:
                        required = True
                    if required:
                        required_attrs.add(key)
                    else:
                        optional_attrs.add(key)
                    if value in BASIC_TYPES:
                        typ = value
                    elif isinstance(value, type) and issubclass(value, enum.IntEnum):
                        typ = value
                    elif (
                        isinstance(value, type)
                        and hasattr(value, "serialize")
                        and hasattr(value, "unserialize")
                    ):
                        typ = value
                    elif (
                        has_self_cls
                        and isinstance(value, _ADTMember)
                        and value.name == name
                    ):
                        typ = new_cls
                    else:
                        raise TypeError(f"unsupported type {value}")
                    attrs[key] = typ
                    if required:
                        annotations[key] = typ
                    else:
                        annotations[key] = typ | None
                lines = "".join(f"    self.{attr} = {attr}\n" for attr in member.kwargs)
                code = f'def __init__(self, {", ".join(member.kwargs)}):\n{lines}'
                new_ns: dict[str, Any] = {}
                exec(code, {}, new_ns)
                init = new_ns["__init__"]
                init.__annotations__.update(annotations)
                member_ns["__init__"] = init
                member_ns["__match_args__"] = tuple(annotations)
            member_cls: Any = functools.total_ordering(
                type(member.name, (new_cls,), member_ns)
            )
            if not has_args:
                cls_obj = member_cls
                member_cls = cls_obj()

                def make_init(inner_member_cls: object) -> Callable[[object], None]:
                    def __init__(self: object) -> None:
                        raise TypeError(f"cannot instantiate {inner_member_cls}")

                    return __init__

                cls_obj.__init__ = make_init(member_cls)
            constructors.append(Literal[member_cls])
            new_cls._tag_to_member[member.tag] = member_cls  # type: ignore[attr-defined]
            setattr(new_cls, member.name, member_cls)
        if constructors:
            new_cls._Constructors = functools.reduce(operator.or_, constructors)  # type: ignore[attr-defined]
        return new_cls


_ADTT = TypeVar("_ADTT", bound="ADT")

if TYPE_CHECKING:

    class _ADTBase(Any):
        pass

else:

    class _ADTBase:
        pass


class ADT(_ADTBase, metaclass=_ADTMeta):
    _attributes: dict[str, Any]
    _has_args: bool
    _tag: int
    _tag_to_member: dict[int, type[Any]]

    def _get_attributes(self) -> Iterable[Any]:
        for attr in self._attributes:
            yield getattr(self, attr)

    def serialize(self) -> Any:
        if self._has_args:
            args = []
            for value in self._get_attributes():
                if hasattr(value, "serialize"):
                    args.append(value.serialize())
                elif isinstance(value, enum.IntEnum):
                    args.append(value.value)
                else:
                    args.append(value)
            return [self._tag, *args]
        else:
            return [self._tag]

    @classmethod
    def unserialize(cls, value: list[Any]) -> Self:
        tag = value[0]
        member_cls = cls._tag_to_member[tag]
        if member_cls._has_args:
            args: list[Any] = []
            for arg_type, serialized in zip(
                member_cls._attributes.values(), value[1:], strict=False
            ):
                if hasattr(arg_type, "unserialize"):
                    if serialized is None:
                        args.append(None)
                    else:
                        args.append(arg_type.unserialize(serialized))
                elif (
                    serialized is not None
                    and isinstance(arg_type, type)
                    and issubclass(arg_type, enum.IntEnum)
                ):
                    args.append(arg_type(serialized))
                else:
                    args.append(serialized)
            return member_cls(*args)
        else:
            return member_cls  # type: ignore[return-value]

    def __repr__(self) -> str:
        member_name = type(self).__name__
        if not self._has_args:
            return member_name
        else:
            args = ", ".join(map(repr, self._get_attributes()))
            return f"{member_name}({args})"


def replace(adt: _ADTT, **overrides: Any) -> _ADTT:
    args = {}
    tag_type = type(adt)
    for arg_name in tag_type._attributes:
        try:
            val = overrides[arg_name]
        except KeyError:
            val = getattr(adt, arg_name)
        args[arg_name] = val
    return tag_type(**args)
