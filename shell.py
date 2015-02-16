import db.constants
import db.helpers
import db.models
from db.models import Name, Taxon
import getinput

import collections
import functools
import IPython
import re


class _ShellNamespace(dict):
    def __missing__(self, key):
        try:
            return getattr(__builtins__, key)
        except AttributeError:
            # make names accessible
            return taxon(key)


ns = _ShellNamespace({
    'Taxon': Taxon,
    'Name': Name,
    'constants': db.constants,
    'helpers': db.helpers,
})
ns.update(db.constants.__dict__)


def command(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except getinput.StopException:
            return None

    ns[fn.__name__] = wrapper
    return wrapper


@command
def taxon(name):
    """Finds a taxon with the given name."""
    name = name.replace('_', ' ')
    try:
        return Taxon.filter(Taxon.valid_name == name)[0]
    except IndexError:
        raise LookupError(name)


@command
def n(name):
    """Finds names with the given root name or original name."""
    return list(Name.filter((Name.root_name == name) | (Name.original_name == name)))


@command
def add_original_names():
    for name in Name.select():
        if name.original_citation and not name.original_name:
            message = u'Name {} is missing an original name, but has original citation {{{}}}:{}'.format(
                name.description(), name.original_citation, name.page_described)
            name.original_name = getinput.get_line(
                message, handlers={'o': lambda _: name.open_description()}
            )
            if not name.page_described:
                name.page_described = getinput.get_line(
                    'Enter page described', handlers={'o': lambda _: name.open_description()}, should_stop=lambda line: line == 's'
                )
            name.save()


@command
def add_page_described():
    for name in Name.filter(Name.original_citation != None, Name.page_described >> None, Name.year != 'in press').order_by(Name.original_citation):
        message = 'Name %s is missing page described, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        name.page_described = getinput.get_line(
            message, handlers={'o': lambda _: name.open_description()}, should_stop=lambda line: line == 's'
        )
        name.save()


@command
def add_types():
    for name in Name.filter(Name.original_citation != None, Name.year > '1930').order_by(Name.original_citation):
        message = 'Name %s is missing type, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        print(message)


@command
def find_rank_mismatch():
    for taxon in Taxon.select():
        expected_group = db.helpers.group_of_rank(taxon.rank)
        if expected_group != taxon.base_name.group:
            print("Group mismatch for %s: rank %s but group %s" % \
                (taxon, db.constants.string_of_rank(taxon.rank), db.constants.string_of_group(taxon.base_name.group)))


@command
def print_percentages():
    attributes = ['original_name', 'original_citation', 'page_described', 'authority', 'year']
    parent_of_taxon = {}

    def _find_parent(taxon):
        if taxon.is_page_root:
            return taxon.id
        elif taxon.id in parent_of_taxon:
            return parent_of_taxon[taxon.id]
        else:
            return _find_parent(taxon.parent)

    for taxon in Taxon.select():
        parent_of_taxon[taxon.id] = _find_parent(taxon)

    counts_of_parent = collections.defaultdict(lambda: collections.defaultdict(int))
    for name in Name.select():
        parent_id = parent_of_taxon[name.taxon.id]
        counts_of_parent[parent_id]['total'] += 1
        for attribute in attributes:
            if getattr(name, attribute) is not None:
                counts_of_parent[parent_id][attribute] += 1

    for parent_id, data in counts_of_parent.items():
        parent = Taxon.filter(Taxon.id == parent_id)[0]
        print("FILE", parent)
        total = data['total']
        del data['total']
        print("Total", total)
        for attribute in attributes:
            percentage = data[attribute] * 100.0 / total
            print("%s: %s (%.2f%%)" % (attribute, data[attribute], percentage))


@command
def detect_types(max_count=None):
    """Converts verbatim_types into references to the actual names."""
    count = 0
    successful_count = 0
    for name in Name.filter(Name.verbatim_type != None):
        if name.group in (db.constants.GROUP_HIGH, db.constants.GROUP_SPECIES):
            continue
        if max_count is not None and count >= max_count:
            break
        count += 1
        candidates = name.detect_type()
        if candidates is None or len(candidates) == 0:
            print("Verbatim type %s for name %s could not be recognized" % (name.verbatim_type, name))
        elif len(candidates) == 1:
            successful_count += 1
            name.type = candidates[0]
            name.save()
        else:
            print("Verbatim type %s for name %s yielded multiple possible names: %s" % (name.verbatim_type, name, candidates))
    print("Success: %d/%d" % (successful_count, count))


def run_shell():
    IPython.start_ipython(user_ns=ns)


if __name__ == '__main__':
    run_shell()
