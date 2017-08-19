import cmd
from typing import Callable, Mapping, Optional


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
             handlers: Mapping[str, Callable[[str], bool]] = None, should_stop: Callable[[str], bool] = lambda _: False) -> Optional[str]:
    class CmdLoop(cmd.Cmd):
        def default(self, line: str) -> None:
            return

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
    return result.lower() in positive
