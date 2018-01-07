import cmd
import enum
import prompt_toolkit
import re
import subprocess
from typing import Callable, Iterable, Mapping, Optional, Sequence, Type


RED = 31
GREEN = 32


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


def get_line(prompt: str, validate: Optional[Callable[[str], bool]] = None,
             handlers: Mapping[str, Callable[[str], bool]] = {},
             should_stop: Callable[[str], bool] = lambda _: False) -> Optional[str]:
    class CmdLoop(cmd.Cmd):
        def default(self, line: str) -> bool:
            return False

        def postcmd(self, stop: object, line: str) -> bool:
            if line == 'EOF':
                raise StopException()
            elif line in handlers:
                return False
            elif should_stop(line):
                self.result = None
                return True
            elif validate is None or validate(line):
                self.result = line
                return True
            else:
                print('Invalid input')
                return False

    if handlers is not None:
        for key, fn in handlers.items():
            def make_handler(fn: Callable[[str], bool]) -> Callable[[object, str], bool]:
                return lambda self, line: fn(line)
            setattr(CmdLoop, 'do_%s' % key, make_handler(fn))

    loop = CmdLoop()
    loop.prompt = '> '
    loop.cmdloop(prompt)
    return loop.result


def yes_no(prompt: str) -> bool:
    positive = {'y', 'yes'}
    negative = {'n', 'no'}
    result = get_line(prompt, validate=lambda line: line.lower() in positive | negative)
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


def get_with_completion(options: Iterable[str], message: str = '> ', *, default: str = '') -> str:
    return prompt_toolkit.prompt(
        completer=_Completer(options),
        message=message,
        default=default,
    )


def get_enum_member(enum_cls: Type[enum.Enum], prompt: str = '> ', default: Optional[enum.Enum] = None) -> Optional[enum.Enum]:
    if default is None:
        default_str = ''
    else:
        default_str = default.name
    options = [v.name for v in enum_cls]
    choice = get_with_completion(options, prompt, default=default_str)
    if choice == '':
        return None
    return enum_cls[choice]


def add_to_clipboard(data: str) -> None:
    subprocess.run(['pbcopy'], check=True, input=data.encode('utf-8'))


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
