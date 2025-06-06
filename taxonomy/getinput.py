"""Helpers for retrieving user input."""

import difflib
import enum
import functools
import itertools
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import zip_longest
from typing import Any, Literal, TypeVar, overload

import prompt_toolkit.completion
import prompt_toolkit.document
import prompt_toolkit.history
import prompt_toolkit.validation

from . import adt

T = TypeVar("T")
Completer = Callable[[str, Any], T]
CompleterMap = Mapping[tuple[type[adt.ADT], str], Completer[Any]]
CallbackMap = Mapping[str, Callable[[], object]]
ADTOrInstance = adt.ADT | type[adt.ADT]
MemberCallbackMap = Mapping[str, Callable[[Mapping[str, Any]], object]]
PerMemberCallbackMap = Mapping[type[adt.ADT], MemberCallbackMap]


RED = 31
GREEN = 32
YELLOW = 33
BLUE = 34


class StopException(Exception):
    pass


def _color(code: int) -> str:
    return f"{chr(27)}[{code}m"


def _colored_text(text: str, code: int) -> str:
    return f"{_color(code)}{text}{_color(0)}"


def red(text: str) -> str:
    return _colored_text(text, RED)


def green(text: str) -> str:
    return _colored_text(text, GREEN)


def yellow(text: str) -> str:
    return _colored_text(text, YELLOW)


def blue(text: str) -> str:
    return _colored_text(text, BLUE)


def italicize(text: str) -> str:
    return f"{chr(27)}[3m{text}{chr(27)}[0m"


def get_line(
    prompt: str,
    *,
    validate: Callable[[str], bool] | None = None,
    callbacks: CallbackMap = {},
    should_stop: Callable[[str], bool] = lambda _: False,
    allow_none: bool = True,
    mouse_support: bool = False,
    default: str = "",
    history_key: object | None = None,
    validator: prompt_toolkit.validation.Validator | None = None,
    completer: prompt_toolkit.completion.Completer | None = None,
    allow_clear: bool = True,
) -> str | None:
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
            raise StopException from None
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
            # TODO: Does this work? Do we need a replacement?
            history.strings = [""]
        return line
    assert False, "should never get here"


def yes_no(
    prompt: str, default: bool | None = None, callbacks: CallbackMap = {}
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
    callbacks: CallbackMap = {},
) -> T | None:
    """Choose one option by picking its index.

    This is usually less pleasant than using `choose_one_by_name`, but it
    makes sense to use it when there are many options that look visually
    similar, or when the string representation of the option is annoying
    to type.

    """
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
        callbacks=callbacks,
    )
    if not choice:
        return None
    return options[int(choice)]


def choose_one_by_name(
    options: Sequence[T],
    *,
    message: str = "Choose one: ",
    allow_empty: bool = True,
    display_fn: Callable[[T], str] = str,
    history_key: object = None,
    callbacks: CallbackMap = {},
    print_choices: bool = True,
    default: T | None = None,
    decode_fn: Callable[[str], T | None] | None = None,
) -> T | None:
    choices = {display_fn(option): option for option in options}
    if print_choices:
        for display in choices:
            print(display)
    if history_key is None:
        history_key = tuple(options)
    while True:
        choice = get_with_completion(
            options=choices,
            message=message,
            disallow_other=False,
            history_key=history_key,
            allow_empty=allow_empty,
            callbacks=callbacks,
            default=display_fn(default) if default is not None else "",
        )
        if not choice:
            return None
        if choice in choices:
            return choices[choice]
        if decode_fn is not None:
            decoded = decode_fn(choice)
            if decoded is not None:
                return decoded


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


class _CallbackCompleter(prompt_toolkit.completion.Completer):
    def __init__(
        self,
        strings: Iterable[str],
        lazy_strings: Callable[[], Iterable[str]],
        max_completions: int = 100,
    ) -> None:
        self.strings = sorted(strings)
        self.lazy_strings = lazy_strings
        self.max_completions = max_completions

    def get_completions(
        self,
        document: prompt_toolkit.document.Document,
        complete_event: prompt_toolkit.completion.CompleteEvent,
    ) -> Iterable[prompt_toolkit.completion.Completion]:
        # This might be faster with a prefix tree but I'm lazy.
        text = document.text
        for s in sorted(self._get_unsorted_completions(text)):
            yield prompt_toolkit.completion.Completion(s[len(text) :])

    def _get_unsorted_completions(self, query: str) -> Iterable[str]:
        num_yielded = 0
        for string in self.strings:
            if string.startswith(query):
                yield string
                num_yielded += 1
                if num_yielded >= self.max_completions:
                    return
        for string in self.lazy_strings():
            if string.startswith(query):
                yield string
                num_yielded += 1
                if num_yielded >= self.max_completions:
                    return


def get_with_lazy_completion(
    message: str = "> ",
    *,
    options_provider: Callable[[], Iterable[str]],
    is_valid: Callable[[str], bool],
    default: str = "",
    history_key: object,
    disallow_other: bool = False,
    allow_empty: bool = True,
    callbacks: CallbackMap = {},
) -> str | None:
    validator: prompt_toolkit.validation.Validator | None
    if disallow_other:

        def callback(text: str) -> bool:
            return (text in callbacks) or is_valid(text)

        validator = _CallbackValidator(callback)
    else:
        validator = None
    return get_line(
        completer=_CallbackCompleter(callbacks, options_provider),
        prompt=message,
        default=default,
        history_key=history_key,
        validator=validator,
        callbacks=callbacks,
        allow_none=allow_empty,
    )


def get_with_completion(
    options: Iterable[str],
    message: str = "> ",
    *,
    default: str = "",
    history_key: object | None = None,
    disallow_other: bool = False,
    allow_empty: bool = True,
    callbacks: CallbackMap = {},
) -> str | None:
    if history_key is None:
        history_key = (tuple(options), message)
    validator: prompt_toolkit.validation.Validator | None
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


@overload
def get_enum_member(
    enum_cls: type[EnumT],
    prompt: str = "> ",
    *,
    default: EnumT | None = None,
    allow_empty: Literal[False],
    callbacks: CallbackMap = {},
) -> EnumT:
    raise NotImplementedError


@overload
def get_enum_member(
    enum_cls: type[EnumT],
    prompt: str = "> ",
    *,
    default: EnumT | None = None,
    allow_empty: bool = ...,
    callbacks: CallbackMap = {},
) -> EnumT | None:
    raise NotImplementedError


def get_enum_member(
    enum_cls: type[EnumT],
    prompt: str = "> ",
    *,
    default: EnumT | None = None,
    allow_empty: bool = True,
    callbacks: CallbackMap = {},
) -> EnumT | None:
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


def _make_split_title(title: str) -> list[str]:
    split_title = title.split()
    for i, word in enumerate(split_title):
        if repr(word) != f"'{word}'":
            print(f"{i}: {word} ({word!r})")
        else:
            print(f"{i}: {word}")
    return split_title


def _unite_title(split_title: Iterable[str]) -> str:
    return re.sub(r"\s+", " ", " ".join(split_title).strip())


def _smart_upper(word: str) -> str:
    match = re.fullmatch(r"([_\(]*)(.)(.*)", word)
    if match is None:
        return word
    return f"{match.group(1)}{match.group(2).upper()}{match.group(3)}"


def _smart_divide(word: str) -> str:
    # capital letter not at beginning of word
    word = re.sub(r"(?<=[a-z,\.\)])(?=[A-Z])", " ", word)
    # left parenthesis not at beginning of word
    word = re.sub(r"(?<!^)(?=\()", " ", word)
    # comma followed by letter
    word = re.sub(r"(?<=,)(?=[a-zA-Z])", " ", word)
    # right parenthesis not at end of word
    return re.sub(r"(?<=\))(?![$,])", " ", word)


_WordRangeCommand = Callable[[int, int], object]


@dataclass(kw_only=True)
class _WordEditor:
    existing: str
    save_handler: Callable[[str], object] | None
    callbacks: CallbackMap
    words: list[str] = field(init=False)
    word_range_commands: dict[str, _WordRangeCommand] = field(init=False)

    def __post_init__(self) -> None:
        self.words = _make_split_title(self.existing)
        self.word_range_commands = {
            "l": self.make_looper(str.lower),
            "u": self.make_looper(_smart_upper),
            # Remove words
            "r": self.make_looper(lambda word: ""),
            # Remove text before a dash
            "d": self.make_looper(lambda word: re.sub(r"^.*—", "", word)),
            "v": self.make_looper(_smart_divide),
            # deitalicize
            "j": self.make_looper(lambda word: word.replace("_", "")),
            "i": self.italicize_range,
            "e": self.make_looper(self.edit_word),
            "t": self.merge_words,
        }
        callbacks: dict[str, Callable[[], object]] = {
            **self.callbacks,
            "p": self.print_title,
            "c": self.recompute,
        }
        if self.save_handler is not None:
            callbacks["S"] = self.save_and_continue
        self.callbacks = callbacks

    def edit_word(self, word: str) -> str:
        line = get_line(
            "word> ", allow_none=False, callbacks=self.callbacks, default=word
        )
        if line is None:
            return ""
        # allow setting words that match commands
        return line.lstrip("\\")

    def merge_words(self, start: int, end: int) -> None:
        if start == end:
            end += 1
        if end >= len(self.words):
            print("Word out of range")
            return
        self.words[start] += "".join(self.words[start + 1 : end + 1])
        for i in range(start + 1, end + 1):
            self.words[i] = ""

    def save_and_continue(self) -> None:
        if self.save_handler is not None:
            new_title = _unite_title(self.words)
            print("New title:", new_title)
            self.save_handler(new_title)

    def print_title(self) -> None:
        print(_unite_title(self.words))

    def recompute(self) -> None:
        self.words = _make_split_title(_unite_title(self.words))

    def italicize_range(self, start: int, end: int) -> None:
        for i in range(start, end + 1):
            self.words[i] = self.words[i].replace("_", "")
        self.words[start] = "_" + self.words[start]
        self.words[end] += "_"

    def make_looper(self, f: Callable[[str], str]) -> Callable[[int, int], None]:
        def handler(start: int, end: int) -> None:
            for i in range(start, end + 1):
                self.words[i] = f(self.words[i])

        return handler

    def run(self) -> str:
        print("Current title:", self.existing)
        while True:
            line = get_line(
                prompt="edittitle> ", callbacks=self.callbacks, allow_none=False
            )
            if line is None:
                continue
            if line == "s":
                new_title = _unite_title(self.words)
                print("New title:", new_title)
                return new_title
            if line == "a":
                return self.existing
            if match := re.fullmatch(r"([a-z]+)(\d+)", line):
                command = match.group(1)
                index = int(match.group(2))
                if not (0 <= index < len(self.words)):
                    print(f"Index out of range: {index}")
                    continue
                if command in self.word_range_commands:
                    self.word_range_commands[command](index, index)
                    continue
            if match := re.fullmatch(r"([a-z]+)(\d+)-(\d+)", line):
                command = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3))
                if start > end or start < 0 or end >= len(self.words):
                    print(f"Invalid range: {start}-{end}")
                    continue
                if command in self.word_range_commands:
                    self.word_range_commands[command](start, end)
                    continue
            print(f"Unrecognized command: {line!r}")
        assert False, "unreachable"


def edit_by_word(
    existing: str,
    *,
    save_handler: Callable[[str], object] | None = None,
    callbacks: CallbackMap = {},
) -> str:
    return _WordEditor(
        existing=existing, save_handler=save_handler, callbacks=callbacks
    ).run()


_ADT_LIST_BUILTINS = ["r", "remove_all", "h", "undo"]


def get_adt_list(
    adt_cls: type[adt.ADT],
    *,
    existing: Iterable[ADTOrInstance] | None = None,
    completers: CompleterMap = {},
    callbacks: CallbackMap = {},
    show_existing: bool = False,
    prompt: str | None = None,
    get_existing: Callable[[], Iterable[ADTOrInstance]] | None = None,
    set_existing: Callable[[Sequence[ADTOrInstance]], None] | None = None,
    member_callbacks: PerMemberCallbackMap = {},
) -> tuple[ADTOrInstance, ...]:
    if prompt is None:
        prompt = adt_cls.__name__
    name_to_cls = {}
    for member_name in adt_cls._members:
        name_to_cls[member_name.lower()] = getattr(adt_cls, member_name)
    undo_stack: list[list[ADTOrInstance]] = []
    while True:
        out: list[ADTOrInstance] = []
        if existing is None and get_existing is not None:
            existing = get_existing()
        if existing is not None:
            out += existing
            if show_existing:
                print("existing:")
                for line in display_tags("  ", existing, show_indexes=True):
                    print(line, end="")
                show_existing = False
        existing = None
        if not undo_stack or out != undo_stack[-1]:
            undo_stack.append(list(out))
        options = [
            *name_to_cls.keys(),
            *_ADT_LIST_BUILTINS,
            *map(str, range(len(out))),
            *[f"r{i}" for i in range(len(out))],
            *callbacks,
        ]
        member = get_with_completion(
            options,
            message=f"{prompt}> ",
            history_key=adt_cls,
            disallow_other=not callbacks,
        )
        if member is not None and member in callbacks:
            callbacks[member]()
            continue  # don't call set_existing
        elif member == "p":
            for line in display_tags("", out, show_indexes=True):
                print(line, end="")
        elif member == "h":
            print(f'options: {", ".join(name_to_cls.keys())}')
        elif member == "undo":
            if undo_stack:
                out = undo_stack.pop()
            else:
                print("already at earliest edit")
        elif not member:
            print(f"new tags: {out}")
            return tuple(out)
        elif member.isnumeric() or (member.startswith("-") and member[1:].isnumeric()):
            index = int(member)
            try:
                existing_member = out[index]
            except IndexError:
                print(f"{index} is out of range")
            else:
                if existing_member._has_args:
                    out[index] = get_adt_member(
                        type(existing_member),  # type: ignore[arg-type]
                        existing=existing_member,
                        completers=completers,
                        callbacks=member_callbacks.get(type(existing_member), {}),  # type: ignore[arg-type]
                    )
        elif member in ("r", "remove_all"):
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
            member_cls = name_to_cls[member]
            out.append(
                get_adt_member(
                    member_cls,
                    completers=completers,
                    callbacks=member_callbacks.get(member_cls, {}),
                )
            )
        else:
            print(f"unrecognized command: {member}")
        if set_existing is not None:
            set_existing(out)
    assert False, "should never get here"


def display_tags(
    spacing: str,
    tags: Iterable[adt.ADT | type[adt.ADT]] | None,
    *,
    show_indexes: bool = True,
) -> Iterable[str]:
    if tags is None:
        return
    tags = list(tags)
    if not show_indexes:
        tags = sorted(tags)
    for i, tag in enumerate(tags):
        if show_indexes:
            index = f" ({i})"
        else:
            index = ""
        if isinstance(tag, type):
            # tag without arguments
            yield f"{spacing}{tag.__name__}{index}\n"
        else:
            yield f"{spacing}{type(tag).__name__}{index}\n"
            for attr in tag._attributes:
                value = getattr(tag, attr)
                if value is None or value == "":
                    continue
                if isinstance(value, str):
                    value = re.sub(r"\s+", " ", value).strip()
                if isinstance(value, enum.Enum):
                    value = value.name
                yield f"{spacing}  {attr}: {value!s}\n"


def _get_adt_member_field(
    *,
    member_cls: type[adt.ADT],
    existing: ADTOrInstance | None = None,
    completers: CompleterMap = {},
    arg_name: str,
) -> Any:
    typ = adt.unwrap_type(member_cls._attributes[arg_name])
    is_required = arg_name in member_cls.__required_attrs__
    existing_value = getattr(existing, arg_name, None)
    if (member_cls, arg_name) in completers:
        while True:
            value = completers[(member_cls, arg_name)](f"{arg_name}> ", existing_value)
            if value is not None or not is_required:
                break
        return value
    elif isinstance(typ, type) and issubclass(typ, enum.IntEnum):
        return get_enum_member(
            typ,
            prompt=f"{arg_name}> ",
            default=existing_value,
            allow_empty=not is_required,
        )
    elif typ is bool:
        return yes_no(f"{arg_name}> ", default=existing_value)
    elif typ in adt.BASIC_TYPES:
        assert isinstance(typ, type)
        while True:
            value = get_line(
                f"{arg_name}> ",
                history_key=(member_cls, arg_name),
                default="" if existing_value is None else str(existing_value),
                allow_none=not is_required,
            )
            try:
                converted_value = typ(value)
            except Exception:
                print(f"Invalid value for {arg_name}: {value!r}")
                continue
            else:
                return converted_value
        assert False, "unreachable"
    else:
        assert False, f"do not know how to fill {arg_name} of type {typ}"


def _wrap_member_callback_map(
    map: MemberCallbackMap, args: dict[str, Any]
) -> CallbackMap:
    if not map:
        return {}
    return {
        key: lambda callback=callback: callback(args)  # type: ignore[misc]
        for key, callback in map.items()
    }


def get_adt_member(
    member_cls: type[adt.ADT],
    existing: ADTOrInstance | None = None,
    completers: CompleterMap = {},
    callbacks: MemberCallbackMap = {},
) -> ADTOrInstance:
    if not member_cls._has_args:
        return member_cls
    args: dict[str, Any] = {}
    if existing is not None:
        for arg_name in member_cls._attributes:
            args[arg_name] = getattr(existing, arg_name)
    for arg_name in member_cls._attributes:
        is_required = arg_name in member_cls.__required_attrs__
        if not is_required:
            continue
        args[arg_name] = _get_adt_member_field(
            member_cls=member_cls,
            existing=existing,
            completers=completers,
            arg_name=arg_name,
        )
    if member_cls.__optional_attrs__:
        attrs = sorted(member_cls._attributes)
        while True:
            print(f"Optional attributes: {', '.join(attrs)}")
            choice = get_with_completion(
                [*attrs, "empty", "p"],
                "add optional attributes> ",
                history_key=member_cls,
                allow_empty=True,
                callbacks=_wrap_member_callback_map(callbacks, args),
            )
            if not choice:
                break
            if choice in member_cls._attributes:
                args[choice] = _get_adt_member_field(
                    member_cls=member_cls,
                    existing=existing,
                    completers=completers,
                    arg_name=choice,
                )
            elif choice == "empty":
                field_to_empty = get_with_completion(
                    attrs,
                    "attribute to empty> ",
                    history_key=member_cls,
                    allow_empty=True,
                )
                if (
                    field_to_empty is not None
                    and field_to_empty in member_cls.__optional_attrs__
                ):
                    print(f"Emptying {field_to_empty}: {args[field_to_empty]!r}")
                    args[field_to_empty] = None
                else:
                    print(f"Invalid choice: {field_to_empty!r}")
            elif choice == "p":
                print("Current values:")
                for attr in member_cls._attributes:
                    value = args.get(attr)
                    if value is None or value == "":
                        continue
                    print(f"  {attr}: {value!r}")
            else:
                print(f"invalid choice: {choice!r}")
    return member_cls(**args)


def add_to_clipboard(data: str) -> None:
    subprocess.run(["pbcopy"], check=True, input=data.encode("utf-8"))


def append_history(key: object, history_entry: str) -> None:
    _append(_get_history(key), history_entry)


@functools.cache
def _get_history(key: object) -> prompt_toolkit.history.InMemoryHistory:
    history = prompt_toolkit.history.InMemoryHistory()
    _append(history, "")
    return history


def _append(history: prompt_toolkit.history.InMemoryHistory, entry: str) -> None:
    history.append_string(entry)


class _FixedValidator(prompt_toolkit.validation.Validator):
    """Validator that only allows a fixed set of strings."""

    def __init__(self, options: Iterable[str]) -> None:
        self.options = set(options)

    def validate(self, document: prompt_toolkit.document.Document) -> None:
        if document.text not in self.options:
            raise prompt_toolkit.validation.ValidationError


class _CallbackValidator(prompt_toolkit.validation.Validator):
    """Validator that uses a callback."""

    def __init__(self, is_valid: Callable[[str], bool]) -> None:
        self.is_valid = is_valid

    def validate(self, document: prompt_toolkit.document.Document) -> None:
        if not self.is_valid(document.text):
            raise prompt_toolkit.validation.ValidationError


# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r"[^A-Za-z0-9 ]")
_decode_re = re.compile(r"  (\d+) ")


def encode_name(name: str) -> str:
    return _encode_re.sub(lambda m: f"__{ord(m.group())}__", name).replace(" ", "_")


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


def print_scores(data: Sequence[tuple[str, float]]) -> None:
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


def diff_strings(a: str, b: str) -> None:
    diff_line = []
    for a_c, b_c in zip_longest(a, b, fillvalue=""):
        if a_c == b_c:
            diff_line.append(" ")
        else:
            diff_line.append("!")
    print(a)
    print(b)
    print("".join(diff_line))


def print_every_n(it: Iterable[T], *, label: str, n: int = 1000) -> Iterator[T]:
    i = 0
    for i, obj in enumerate(it, start=1):
        if i % n == 0:
            print(f"{i} {label}...")
        yield obj
    print(f"Finished processing {i} {label}")


def print_table(rows: Sequence[Sequence[str]], *, spacing: int = 2) -> None:
    if not rows:
        return
    num_cols = len(rows[0])
    col_widths = [0] * num_cols
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))
    for row in rows:
        for i, cell in enumerate(row):
            print(cell.ljust(col_widths[i]), end=" " * spacing)
        print()
