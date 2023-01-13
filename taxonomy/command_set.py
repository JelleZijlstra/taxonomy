"""

Registration for a set of commands to be used in the shell.

"""
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

Command = Callable[..., object]
CommandT = TypeVar("CommandT", bound=Command)


@dataclass
class CommandSet:
    name: str
    description: str
    commands: list[Command] = field(default_factory=list, init=False)

    def register(self, command: CommandT) -> CommandT:
        self.commands.append(command)
        return command
