"""Tools for terminal user interfaces.

Provides functions for things like editing long strings and creating menus.

Deprecated; prefer getinput.py for new code.

"""

import functools
from collections.abc import Callable, Iterable
from typing import Any

import prompt_toolkit.completion
import prompt_toolkit.document
import prompt_toolkit.history


class EndOfInput(Exception):
    pass


def _default_valid_function(command: str, options: dict[str, str]) -> bool:
    if options:
        return command in options
    else:
        return True


Processor = Callable[[str, Any], bool]


def make_callback(cb: Callable[[], Any], result: bool = True) -> Processor:
    def processor(cmd: str, data: Any) -> bool:
        cb()
        return result

    return processor


def stop_callback(message: str) -> Processor:
    def processor(cmd: str, data: Any) -> bool:
        raise EndOfInput(message)

    return processor


def menu(
    head: str = "",  # menu heading
    *,
    prompt: str = "> ",  # prompt to show to user
    headasprompt: bool = False,  # whether to use the heading as the prompt
    options: dict[str, str] = {},  # dictionary of option to description
    helpinfo: str | None = None,  # information to show when the user types help
    # Whether to make the help command available. (If set to true, commands beginning with "help"
    # will not get returned.)
    helpcommand: bool = True,
    # Function to determine validity of command
    validfunction: Callable[[str, dict[str, str]], bool] = _default_valid_function,
    # Array of callbacks to execute when a given option is called. These function take the
    # command given and the data produced by processcommand as arguments and they should return
    # either true (indicating that menu should continue) or false (indicating that menu should
    # return).
    process: dict[str, Processor] = {},
    # Function used to process the command after input. This function may take a second
    # reference argument of data that is given to processcommand or to the caller. This function
    # may return None if the command is invalid.
    processcommand: Callable[[str], tuple[str | None, Any]] | None = None,
    initialtext: str = "",  # Initial text shown in the menu
    completions: Iterable[str] = (),  # Strings we should autocomplete
) -> tuple[str, Any]:
    if not headasprompt:
        if head:
            print(head)

    def print_options() -> None:
        if not options:
            return
        print("Options available:")
        for cmd, desc in options.items():
            print(f"- {cmd!r}: {desc}")

    history = _get_history((head, prompt, tuple(options)))
    message = head if headasprompt else prompt
    completer_options = tuple(options.keys()) + tuple(completions)

    while True:
        cmd = get_line(
            default=initialtext,
            history=history,
            message=message,
            options=completer_options,
        )

        # provide help if necessary
        if helpcommand:
            if cmd == "help":
                if helpinfo:
                    print(helpinfo)
                print_options()
                continue
            # help about a specific command
            if cmd.startswith("help "):
                option = cmd[5:]
                if option in options:
                    print(f"{option}: {options[option]}")
                else:
                    print(f"option {option} does not exist")
                continue

        if processcommand is not None and cmd not in options:
            new_cmd, data = processcommand(cmd)
        else:
            new_cmd = cmd
            data = None
        # return command if valid
        if new_cmd is not None and (
            new_cmd in options or validfunction(new_cmd, options)
        ):
            if new_cmd in process:
                if not process[new_cmd](new_cmd, data):
                    return new_cmd, data
            else:
                return new_cmd, data
        else:
            print(f"Invalid value {cmd}")
    assert False, "should never get here"


def get_line(
    message: str = "> ",
    *,
    default: str = "",
    history: prompt_toolkit.history.History | None = None,
    options: Iterable[str] | None = None,
) -> str:
    completer: _Completer | None
    if options is not None:
        completer = _Completer(options)
    else:
        completer = None
    if history is None:
        history = _get_history((message, default))
    try:
        return prompt_toolkit.prompt(
            message=message, default=default, history=history, completer=completer
        )
    except EOFError:
        raise EndOfInput from None


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


@functools.cache
def _get_history(key: object) -> prompt_toolkit.history.InMemoryHistory:
    history = prompt_toolkit.history.InMemoryHistory()
    history.store_string("")
    return history
