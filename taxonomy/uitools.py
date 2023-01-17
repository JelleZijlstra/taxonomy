"""Tools for terminal user interfaces.

Provides functions for things like editing long strings and creating menus.

Deprecated; prefer getinput.py for new code.

"""
import functools
import re
from typing import Any, NamedTuple
from collections.abc import Callable, Iterable, Sequence

import prompt_toolkit

# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r"[^A-Za-z ]")
_decode_re = re.compile(r"_(\d+)_")


def _encode_name(name: str) -> str:
    return _encode_re.sub(lambda m: "_%d_" % ord(m.group()), name).replace(" ", "_")


def _decode_name(name: str) -> str:
    return _decode_re.sub(lambda m: chr(int(m.group(1))), name).replace("_", " ")


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
        raise EndOfInput() from None


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


_SaveHandler = Callable[[str, bool], Any]


class Callback(NamedTuple):
    code: str
    description: str
    callback: Callable[[], Any]


def edittitle(
    existing_title: str,
    *,
    save_handler: _SaveHandler = lambda title, full: print(title),
    callbacks: Sequence[Callback] = (),
    get_title: Callable[[], str] = lambda: "",
) -> None:
    print("Current title: " + existing_title)

    # function to create the internal title array
    def make_split(title: str) -> list[str]:
        split_title = title.split()
        for i, word in enumerate(split_title):
            print(f"{i}: {word}")
        return split_title

    # the array to hold the title
    split_title = make_split(existing_title)

    # and another to convert it back into a good title
    def unite(split_title: list[str]) -> str:
        return re.sub(r"\s+", " ", " ".join(split_title).strip())

    # smartly convert a word to lowercase
    tolower = str.lower

    # and uppercase
    def toupper(word: str) -> str:
        if len(word) > 1 and word[0] == "(":
            return word[0] + word[1].upper() + word[2:]
        else:
            return word[0].upper() + word[1:]

    def looper(f: Callable[[str], str]) -> Callable[[str, Any], bool]:
        def handler(cmd: str, data: Any) -> bool:
            if not isinstance(data, tuple):
                print("No range given")
                return True
            if len(data) != 2:
                print("Invalid argument")
                return True
            for i in range(data[0], data[1] + 1):
                if i >= len(split_title):
                    print("Out of range")
                    return True
                split_title[i] = f(split_title[i])
            return True

        return handler

    def processcommand(cmd: str) -> tuple[str | None, Any]:
        cmd = cmd.strip()
        if cmd.isalpha():
            return cmd, []
        match = re.match(r"^([a-z])\s*(\d+)\s*-\s*(\d+)\s*$", cmd)
        if match:
            beg = int(match[2])
            end = int(match[3])
            if beg > end:
                print("Range invalid: beginning > end")
                return None, None
            elif end > len(split_title):
                print(f"Range invalid: no word {end}")
                return None, None
            elif beg > len(split_title):
                print(f"Range invalid: no word {beg}")
                return None, None
            return match[1], (beg, end)
        match = re.match(r"^([a-z])\s*(\d+)$", cmd)
        if match:
            n = int(match[2])
            if n > len(split_title):
                print(f"Range invalid: no word {n}")
                return None, None
            return match[1], (n, n)
        else:
            return None, None

    def italicizer(cmd: str, data: Any) -> bool:
        if not isinstance(data, tuple) or len(data) != 2:
            print("Invalid argument")
            return True
        start, end = data
        if start >= len(split_title) or end >= len(split_title):
            print("Out of range")
            return True
        for i in range(start, end + 1):
            split_title[i] = split_title[i].replace("_", "")
        split_title[start] = "_" + split_title[start]
        split_title[end] += "_"
        return True

    def deitalicizer(cmd: str, data: Any) -> bool:
        if not isinstance(data, tuple) or len(data) != 2:
            print("Invalid argument")
            return True
        start, end = data
        if start >= len(split_title) or end >= len(split_title):
            print("Out of range")
            return True
        for i in range(start, end + 1):
            split_title[i] = split_title[i].replace("_", "")
        return True

    def edit_word(cmd: str, data: Any) -> bool:
        if not isinstance(data, tuple) or len(data) != 2 or data[0] != data[1]:
            print("Invalid argument")
            return True

        index = data[0]
        if index >= len(split_title):
            print("Out of range")
            return True
        print(f"Current value of word {index}: {split_title[index]}")
        split_title[index] = get_line(default=split_title[index], message="New value: ")
        return True

    def merge_words(cmd: str, data: Any) -> bool:
        if not isinstance(data, tuple) or len(data) != 2 or data[0] != data[1]:
            print("Invalid argument")
            return True
        index = data[0]
        if index >= len(split_title):
            print("Out of range")
            return True
        split_title[index] += split_title[index + 1]
        split_title[index + 1] = ""
        return True

    def smart_divide(word: str) -> str:
        # capital letter not at beginning of word
        word = re.sub(r"(?<=[a-z,\.\)])(?=[A-Z])", " ", word)
        # left parenthesis not at beginning of word
        word = re.sub(r"(?<!^)(?=\()", " ", word)
        # comma followed by letter
        word = re.sub(r"(?<=,)(?=[a-zA-Z])", " ", word)
        # right parenthesis not at end of word
        word = re.sub(r"(?<=\))(?![$,])", " ", word)
        return word

    def whole_title(cmd: str, data: Any) -> bool:
        nonlocal split_title
        ret = edit_whole_title(new=unite(split_title), save_handler=save_handler)
        if ret is None:
            return False
        else:
            split_title = make_split(get_title())
            return True

    def print_title(cmd: str, data: Any) -> bool:
        print(unite(split_title))
        return True

    def recompute(cmd: str, data: Any) -> bool:
        nonlocal split_title
        split_title = make_split(unite(split_title))
        return True

    def make_handler(cb: Callable[[], Any]) -> Callable[[str, Any], bool]:
        def handler(cmd: str, data: Any) -> bool:
            cb()
            return True

        return handler

    def saver(cmd: str, data: Any, return_value: bool = False) -> bool:
        save_handler(unite(split_title), True)
        return return_value

    menu(
        prompt="edittitle> ",
        options={
            "l": "Make a word lowercase",
            "u": "Make a word uppercase",
            "i": "Italicize a word",
            "j": "Remove italics from a word",
            "e": "Edit an individual word",
            "t": "Merge a word with the next word",
            "r": "Remove a word",
            "v": "Smartly divide an individual word",
            "w": "Edit the whole title",
            "p": "Preview the edited title",
            "c": "Recalculate the words",
            "a": "Quit this file without saving changes",
            "s": "Save the changed title",
            "S": "Save and continue editing",
            "d": "Remove the part of the word before a dash",
            **{cb.code: cb.description for cb in callbacks},
        },
        processcommand=processcommand,
        process={
            "l": looper(tolower),
            "u": looper(toupper),
            "i": italicizer,
            "j": deitalicizer,
            "d": looper(lambda word: re.sub(r"^.*—", "", word)),
            "e": edit_word,
            "t": merge_words,
            "r": looper(lambda word: ""),
            "v": looper(smart_divide),
            "w": whole_title,
            "p": print_title,
            "c": recompute,
            "a": lambda *args: False,
            "s": saver,
            "S": functools.partial(saver, return_value=True),
            **{cb.code: make_handler(cb.callback) for cb in callbacks},
        },
    )


def edit_whole_title(new: str, *, save_handler: _SaveHandler) -> bool:
    print("Current title: " + new)
    options = {
        "r": "Save new title, return to word-by-word editing",
        "b": "Do not save title, return to word-by-word editing",
        "s": "Save new title",
        "a": "Do not save new title",
        "p": "Preview title",
        "e": "Edit title",
    }

    def processcommand(cmd: str) -> tuple[str | None, Any]:
        if cmd in options:
            return (cmd, None)
        else:
            # else pretend it is the 'e' command
            return ("e", cmd)

    def r_handler(cmd: str, data: Any) -> bool:
        save_handler(new, False)
        return False

    def e_handler(cmd: str, data: Any) -> bool:
        nonlocal new
        if not data:
            data = get_line("New title: ")
        new = data
        return True

    def p_handler(cmd: str, data: Any) -> bool:
        print(new)
        return True

    result, _ = menu(
        prompt="editWholeTitle> ",
        options=options,
        processcommand=processcommand,
        process={
            "r": r_handler,
            "b": lambda *args: False,
            "s": r_handler,
            "a": lambda *args: False,
            "p": p_handler,
            "e": e_handler,
        },
    )
    return result in ("r", "b")


@functools.cache
def _get_history(key: object) -> prompt_toolkit.history.InMemoryHistory:
    history = prompt_toolkit.history.InMemoryHistory()
    history.store_string("")
    return history
