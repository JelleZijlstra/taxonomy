"""Events used to broadcast updates in the database."""

class Event(object):
    def __init__(self):
        self.handlers = []

    def on(self, callback):
        self.handlers.append(callback)

    def trigger(self, args):
        for handler in self.handlers:
            handler(args)


on_new_taxon = Event()
on_new_name = Event()
on_taxon_save = Event()
on_name_save = Event()
on_period_save = Event()
on_locality_save = Event()
