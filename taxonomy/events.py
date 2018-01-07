"""Events used to broadcast updates in the database."""

from typing import Callable, Generic, List, TypeVar, TYPE_CHECKING

T = TypeVar('T')


class Event(Generic[T]):
    def __init__(self) -> None:
        self.handlers: List[Callable[[T], object]] = []

    def on(self, callback: Callable[[T], object]) -> None:
        self.handlers.append(callback)

    def trigger(self, args: T) -> None:
        for handler in self.handlers:
            handler(args)
