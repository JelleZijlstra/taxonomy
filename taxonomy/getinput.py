"""

Helpers for retrieving user input.

"""
import difflib
import enum
import functools
import itertools
import re
import shutil
import subprocess
import sys
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import prompt_toolkit

from . import adt


T = TypeVar("T")
Completer = Callable[[str, Any], T]
CompleterMap = Mapping[Tuple[Type[adt.ADT], str], Completer[Any]]
CallbackMap = Mapping[str, Callable[[], object]]
ADTOrInstance = Union[adt.ADT, Type[adt.ADT]]

RED = 31
GREEN = 32
BLUE = 34


class StopException(Exception):
    pass


def _color(code: int) -> str:
    return "{}[{}m".format(chr(27), code)


def _colored_text(text: str, code: int) -> str:
    return "{}{}{}".format(_color(code), text, _color(0))


def red(text: str) -> str:
    return _colored_text(text, RED)


def green(text: str) -> str:
    return _colored_text(text, GREEN)


def blue(text: str) -> str:
    return _colored_text(text, BLUE)


def get_line(
    prompt: str,
    validate: Optional[Callable[[str], bool]] = None,
    callbacks: CallbackMap = {},
    should_stop: Callable[[str], bool] = lambda _: False,
    allow_none: bool = True,
    mouse_support: bool = False,
    default: str = "",
    history_key: Optional[object] = None,
    validator: Optional[prompt_toolkit.validation.Validator] = None,
    completer: Optional[prompt_toolkit.completion.Completer] = None,
    allow_clear: bool = True,
) -> Optional[str]:
    if history_key is None:
        history_key = prompt
    history = _get_history(history_key)
    if completer is None and callbacks:
        completer = _Completer(callbacks.keys())
    while True:
        try:
            flush()
            line = prompt_toolkit.prompt(
                message=prompt,
                default=default,
                mouse_support=mouse_support,
                history=history,
                validator=validator,
                completer=completer,
            )
        except EOFError:
            raise StopException()
        if line in callbacks:
            callbacks[line]()
            continue
        if should_stop(line):
            return None
        if validate is not None and not validate(line):
            continue
        if not allow_none and line == "":
            continue
        if allow_clear and line == "clear":
            history.strings = [""]
        return line


def yes_no(
    prompt: str, default: Optional[bool] = None, callbacks: CallbackMap = {}
) -> bool:
    positive = {"y", "yes"}
    negative = {"n", "no"}
    default_str = "y" if default is True else ("n" if default is False else "")
    result = get_line(
        prompt + "> ",
        validate=lambda line: line.lower() in positive | negative,
        default=default_str,
        callbacks=callbacks,
    )
    return result is not None and result.lower() in positive


def choose_one(
    options: Sequence[T],
    *,
    message: str = "Choose one: ",
    allow_empty: bool = True,
    display_fn: Callable[[T], str] = str,
    history_key: object = None,
) -> Optional[T]:
    for i, option in enumerate(options):
        print(f"{i}: {display_fn(option)}")
    choices = [str(i) for i in range(len(options))]
    if history_key is None:
        history_key = tuple(options)
    choice = get_with_completion(
        options=choices,
        message=message,
        disallow_other=True,
        history_key=history_key,
        allow_empty=allow_empty,
    )
    if not choice:
        return None
    return options[int(choice)]


class _Completer(prompt_toolkit.completion.Completer):
    def __init__(self, strings: Iterable[str]) -> None:
        self.strings = sorted(strings)

    def get_completions(
        self,
        document: prompt_toolkit.document.Document,
        complete_event: prompt_toolkit.completion.CompleteEvent,
    ) -> Iterable[prompt_toolkit.completion.Completion]:
        # This might be faster with a prefix tree but I'm lazy.
        text = document.text
        for string in self.strings:
            if string.startswith(text):
                yield prompt_toolkit.completion.Completion(string[len(text) :])


def get_with_completion(
    options: Iterable[str],
    message: str = "> ",
    *,
    default: str = "",
    history_key: Optional[object] = None,
    disallow_other: bool = False,
    allow_empty: bool = True,
    callbacks: CallbackMap = {},
) -> Optional[str]:
    if history_key is None:
        history_key = (tuple(options), message)
    validator: Optional[prompt_toolkit.validation.Validator]
    if disallow_other:
        validator = _FixedValidator(
            [*options, *callbacks, ""] if allow_empty else [*options, *callbacks]
        )
    else:
        validator = None
    return get_line(
        completer=_Completer(itertools.chain(options, callbacks.keys())),
        prompt=message,
        default=default,
        history_key=history_key,
        validator=validator,
        callbacks=callbacks,
        allow_none=allow_empty,
    )


EnumT = TypeVar("EnumT", bound=enum.Enum)


# return type is not Optional; this is not strictly true because the user could pass in
# allow_empty=True, but it is good enough until we have literal types.
@overload
def get_enum_member(
    enum_cls: Type[EnumT],
    prompt: str = "> ",
    *,
    default: Optional[EnumT] = None,
    allow_empty: bool,
    callbacks: CallbackMap = {},
) -> EnumT:
    ...  # noqa


@overload  # noqa
def get_enum_member(
    enum_cls: Type[EnumT],
    prompt: str = "> ",
    *,
    default: Optional[EnumT] = None,
    callbacks: CallbackMap = {},
) -> Optional[EnumT]:
    ...  # noqa


def get_enum_member(  # noqa
    enum_cls: Type[EnumT],
    prompt: str = "> ",
    *,
    default: Optional[EnumT] = None,
    allow_empty: bool = True,
    callbacks: CallbackMap = {},
) -> Optional[EnumT]:
    if default is None:
        default_str = ""
    else:
        default_str = default.name
    options = [v.name for v in enum_cls]
    choice = get_with_completion(
        options,
        prompt,
        default=default_str,
        history_key=enum_cls,
        disallow_other=True,
        allow_empty=allow_empty,
        callbacks=callbacks,
    )
    if choice == "" or choice is None:
        return None
    return enum_cls[choice]


_ADT_LIST_BUILTINS = ["r", "remove_all", "h"]


def get_adt_list(
    adt_cls: Type[adt.ADT],
    existing: Optional[Iterable[adt.ADT]] = None,
    completers: CompleterMap = {},
    callbacks: CallbackMap = {},
    show_existing: bool = False,
) -> Tuple[ADTOrInstance, ...]:
    out: List[ADTOrInstance] = []
    if existing is not None:
        out += existing
        if show_existing:
            print("existing:")
            for line in display_tags("  ", existing, show_indexes=True):
                print(line, end="")
    name_to_cls = {}
    for member_name in adt_cls._members:
        name_to_cls[member_name.lower()] = getattr(adt_cls, member_name)
    while True:
        options = [
            *name_to_cls.keys(),
            *_ADT_LIST_BUILTINS,
            *map(str, range(len(out))),
            *[f"r{i}" for i in range(len(out))],
            *callbacks,
        ]
        member = get_with_completion(
            options,
            message=f"{adt_cls.__name__}> ",
            history_key=adt_cls,
            disallow_other=not callbacks,
        )
        if member is not None and member in callbacks:
            callbacks[member]()
        elif member == "p":
            for line in display_tags("", out, show_indexes=True):
                print(line, end="")
            continue
        elif member == "h":
            print(f'options: {", ".join(name_to_cls.keys())}')
        elif not member:
            print(f"new tags: {out}")
            return tuple(out)
        elif member.isnumeric():
            index = int(member)
            if index >= len(out):
                print(f"{index} is out of range")
            else:
                existing_member = out[index]
                if existing_member._has_args:
                    out[index] = _get_adt_member(
                        type(existing_member),  # type: ignore
                        existing=existing_member,
                        completers=completers,
                    )
        elif member == "r" or member == "remove_all":
            if yes_no("Are you sure you want to remove all tags? "):
                out[:] = []
        elif member.startswith("r") and member[1:].isnumeric():
            index = int(member[1:])
            if index >= len(out):
                print(f"{index} is out of range")
            else:
                print("removing member:", out[index])
                del out[index]
        elif member in name_to_cls:
            out.append(_get_adt_member(name_to_cls[member], completers=completers))
        else:
            print(f"unrecognized command: {member}")


def display_tags(
    spacing: str,
    tags: Optional[Iterable[Union[adt.ADT, Type[adt.ADT]]]],
    show_indexes: bool = False,
) -> Iterable[str]:
    if tags is None:
        return
    tags = list(tags)
    if show_indexes:
        tags = tags
    else:
        tags = sorted(tags)
    for i, tag in enumerate(tags):
        if show_indexes:
            index = f"{i}: "
        else:
            index = ""
        if isinstance(tag, type):
            # tag without arguments
            yield f"{spacing}{index}{tag.__name__}\n"
        else:
            yield f"{spacing}{index}{type(tag).__name__}\n"
            for attr in tag._attributes:
                value = getattr(tag, attr)
                if isinstance(value, str):
                    value = re.sub(r"\s+", " ", value).strip()
                yield f"{spacing}  {attr}: {value!s}\n"


def _get_adt_member(
    member_cls: Type[adt.ADT],
    existing: Optional[ADTOrInstance] = None,
    completers: CompleterMap = {},
) -> ADTOrInstance:
    if not member_cls._has_args:
        return member_cls
    args: Dict[str, Any] = {}
    for arg_name, typ in member_cls._attributes.items():
        existing_value = getattr(existing, arg_name, None)
        if (member_cls, arg_name) in completers:
            args[arg_name] = completers[(member_cls, arg_name)](
                f"{arg_name}> ", existing_value
            )
        elif isinstance(typ, type) and issubclass(typ, enum.IntEnum):
            args[arg_name] = get_enum_member(
                typ, prompt=f"{arg_name}> ", default=existing_value, allow_empty=False
            )
        elif typ is bool:
            args[arg_name] = yes_no(f"{arg_name}> ", default=existing_value)
        elif typ in adt.BASIC_TYPES:
            args[arg_name] = typ(  # type: ignore
                get_line(
                    f"{arg_name}> ",
                    history_key=(member_cls, arg_name),
                    default=existing_value or "",
                )
            )
        else:
            assert False, f"do not know how to fill {arg_name} of type {typ}"
    return member_cls(**args)


def add_to_clipboard(data: str) -> None:
    subprocess.run(["pbcopy"], check=True, input=data.encode("utf-8"))


def append_history(key: object, history_entry: str) -> None:
    _append(_get_history(key), history_entry)


@functools.lru_cache(maxsize=None)
def _get_history(key: object) -> prompt_toolkit.history.InMemoryHistory:
    history = prompt_toolkit.history.InMemoryHistory()
    _append(history, "")
    return history


def _append(history: prompt_toolkit.history.InMemoryHistory, entry: str) -> None:
    if hasattr(history, "append"):
        history.append(entry)
    else:
        history.append_string(entry)


class _FixedValidator(prompt_toolkit.validation.Validator):
    """Validator that only allows a fixed set of strings."""

    def __init__(self, options: Iterable[str]) -> None:
        self.options = set(options)

    def validate(self, document: prompt_toolkit.document.Document) -> None:
        if document.text not in self.options:
            raise prompt_toolkit.validation.ValidationError


# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r"[^A-Za-z0-9 ]")
_decode_re = re.compile(r"  (\d+) ")


def encode_name(name: str) -> str:
    return _encode_re.sub(lambda m: "__%d_" % ord(m.group()), name).replace(" ", "_")


def decode_name(name: str) -> str:
    return _decode_re.sub(lambda m: chr(int(m.group(1))), name.replace("_", " "))


def flush() -> None:
    # Flush standard streams before we call into prompt_toolkit, because otherwise
    # output sometimes does not show up.
    sys.stdout.flush()
    sys.stderr.flush()


def show(obj: object) -> None:
    flush()
    print(obj, flush=True)


def print_scores(data: Sequence[Tuple[str, float]]) -> None:
    if not data:
        return
    width = shutil.get_terminal_size().columns
    label_width = max(len(label) for label, _ in data)
    chart_width = width - label_width - 1
    for label, value in data:
        line = (
            label
            + " " * (label_width - len(label) + 1)
            + "#" * int(value * chart_width)
        )
        print(line)


def print_header(obj: object) -> None:
    flush()
    obj_str = str(obj)
    print(f"/={'=' * len(obj_str)}=\\", flush=True)
    print(f"| {obj_str} |", flush=True)
    print(f"\\={'=' * len(obj_str)}=/", flush=True)


def indent(text: str, width: int) -> str:
    spacing = " " * width
    return "".join(f"{spacing}{line}\n" for line in text.splitlines())


def print_diff(a: Sequence[Any], b: Sequence[Any]) -> None:
    matcher = difflib.SequenceMatcher(a=a, b=b)
    for opcode, a_lo, a_hi, b_lo, b_hi in matcher.get_opcodes():
        if opcode == "equal":
            continue
        for i in range(a_lo, a_hi):
            print(f"- {a[i]}")
        for i in range(b_lo, b_hi):
            print(f"+ {b[i]}")
