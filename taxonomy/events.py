"""Events used to broadcast updates in the database."""

from typing import Callable, Generic, List, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from .db.models import Taxon, Name, Period, Locality

T = TypeVar('T')

class Event(Generic[T]):
    def __init__(self):
        self.handlers = []  # type: List[Callable[[T], object]]

    def on(self, callback: Callable[[T], object]) -> None:
        self.handlers.append(callback)

    def trigger(self, args: T) -> None:
        for handler in self.handlers:
            handler(args)


on_new_taxon = Event['Taxon']()
on_new_name = Event['Name']()
on_taxon_save = Event['Taxon']()
on_name_save = Event['Name']()
on_period_save = Event['Period']()
on_locality_save = Event['Location']()
