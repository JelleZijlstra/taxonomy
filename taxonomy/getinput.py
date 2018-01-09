import cmd
import enum
import functools
import prompt_toolkit
import re
import subprocess
from typing import overload, Callable, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, TypeVar

from . import adt


RED = 31
GREEN = 32
BLUE = 34


class StopException(Exception):
    pass


def _color(code: int) -> str:
    return '%s[%sm' % (chr(27), code)


def _colored_text(text: str, code: int) -> str:
    return '%s%s%s' % (_color(code), text, _color(0))


def red(text: str) -> str:
    return _colored_text(text, RED)


def green(text: str) -> str:
    return _colored_text(text, GREEN)


def blue(text: str) -> str:
    return _colored_text(text, BLUE)


def get_line(prompt: str, validate: Optional[Callable[[str], bool]] = None,
             handlers: Mapping[str, Callable[[str], bool]] = {},
             should_stop: Callable[[str], bool] = lambda _: False,
             allow_none: bool = True, mouse_support: bool = False,
             default: str = '', history_key: Optional[object] = None) -> Optional[str]:
    if history_key is None:
        history_key = prompt
    history = _get_history(history_key)
    while True:
        try:
            line = prompt_toolkit.prompt(message=prompt, default=default, mouse_support=mouse_support, history=history)
        except EOFError:
            raise StopException()
        if line in handlers:
            handlers[line](line)
            continue
        if should_stop(line):
            return None
        if validate is not None and not validate(line):
            continue
        if not allow_none and line == '':
            continue
        return line


def yes_no(prompt: str) -> bool:
    positive = {'y', 'yes'}
    negative = {'n', 'no'}
    result = get_line(prompt + '> ', validate=lambda line: line.lower() in positive | negative)
    return result is not None and result.lower() in positive


class _Completer(prompt_toolkit.completion.Completer):
    def __init__(self, strings: Iterable[str]) -> None:
        self.strings = sorted(strings)

    def get_completions(self, document: prompt_toolkit.document.Document,
                        complete_event: prompt_toolkit.completion.CompleteEvent) -> Iterable[prompt_toolkit.completion.Completion]:
        # This might be faster with a prefix tree but I'm lazy.
        text = document.text
        for string in self.strings:
            if string.startswith(text):
                yield prompt_toolkit.completion.Completion(string[len(text):])


def get_with_completion(options: Iterable[str], message: str = '> ', *, default: str = '',
                        history_key: Optional[object] = None, disallow_other: bool = False,
                        allow_empty: bool = True) -> str:
    if history_key is None:
        history_key = (tuple(options), message)
    validator: Optional[prompt_toolkit.validation.Validator]
    if disallow_other:
        validator = _FixedValidator([*options, ''] if allow_empty else options)
    else:
        validator = None
    return prompt_toolkit.prompt(
        completer=_Completer(options),
        message=message,
        default=default,
        history=_get_history(history_key),
        validator=validator,
    )

EnumT = TypeVar('EnumT', bound=enum.Enum)

# return type is not Optional; this is not strictly true because the user could pass in
# allow_empty=True, but it is good enough until we have literal types.
@overload
def get_enum_member(enum_cls: Type[EnumT], prompt: str = '> ', *, default: Optional[EnumT] = None, allow_empty: bool = True) -> EnumT: ...
@overload
def get_enum_member(enum_cls: Type[EnumT], prompt: str = '> ', *, default: Optional[EnumT] = None) -> Optional[EnumT]: ...

def get_enum_member(enum_cls: Type[EnumT], prompt: str = '> ', *, default: Optional[EnumT] = None,
                    allow_empty: bool = True) -> Optional[EnumT]:
    if default is None:
        default_str = ''
    else:
        default_str = default.name
    options = [v.name for v in enum_cls]
    choice = get_with_completion(options, prompt, default=default_str, history_key=enum_cls, disallow_other=True,
                                 allow_empty=allow_empty)
    if choice == '':
        return None
    return enum_cls[choice]


def get_adt_list(adt_cls: Type[adt.ADT], existing: Optional[Iterable[adt.ADT]] = None) -> Tuple[adt.ADT, ...]:
    out: List[adt.ADT] = []
    if existing is not None:
        out += existing
        print(f'existing: {existing}')
    name_to_cls = {}
    for member_name in adt_cls._members:
        name_to_cls[member_name.lower()] = getattr(adt_cls, member_name)
    print(f'options: {", ".join(name_to_cls.keys())}')
    while True:
        options = [*name_to_cls.keys(), 'p']
        member = get_with_completion(options, message=f'{adt_cls.__name__}> ', history_key=adt_cls,
                                     disallow_other=True)
        if member == 'p':
            print(f'current: {out}')
            continue
        elif member == '':
            print(f'new tags: {out}')
            return tuple(out)
        member_cls = name_to_cls[member]
        args = {}
        for arg_name, typ in member_cls._attributes.items():
            if isinstance(typ, type) and issubclass(typ, enum.IntEnum):
                args[arg_name] = get_enum_member(typ, prompt=f'{arg_name}> ')  # type: ignore
            elif typ in adt.BASIC_TYPES:
                args[arg_name] = typ(get_line(f'{arg_name}> '))
            else:
                assert False, f'do not know how to fill {arg_name} of type {typ}'
        out.append(member_cls(**args))


def add_to_clipboard(data: str) -> None:
    subprocess.run(['pbcopy'], check=True, input=data.encode('utf-8'))


@functools.lru_cache(maxsize=None)
def _get_history(key: object) -> prompt_toolkit.history.InMemoryHistory:
    history = prompt_toolkit.history.InMemoryHistory()
    history.append('')
    return history


class _FixedValidator(prompt_toolkit.validation.Validator):
    """Validator that only allows a fixed set of strings."""
    def __init__(self, options: Iterable[str]) -> None:
        self.options = set(options)

    def validate(self, obj: prompt_toolkit.document.Document) -> None:
        if obj.text not in self.options:
            raise prompt_toolkit.validation.ValidationError


# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r'[^A-Za-z0-9 ]')
_decode_re = re.compile(r'  (\d+) ')


def encode_name(name: str) -> str:
    return _encode_re.sub(lambda m: '__%d_' % ord(m.group()), name).replace(' ', '_')


def decode_name(name: str) -> str:
    return _decode_re.sub(lambda m: chr(int(m.group(1))), name.replace('_', ' '))
