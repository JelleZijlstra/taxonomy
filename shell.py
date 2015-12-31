#!/usr/bin/env python3.4

import db.constants
import db.definition
import db.detection
import db.helpers
import db.models
from db.models import Name, Taxon
import events
import getinput

import collections
import functools
import IPython
import re


# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r'[^A-Za-z ]')
_decode_re = re.compile(r'_(\d+)_')


def _encode_name(name):
    return _encode_re.sub(lambda m: '_%d_' % ord(m.group()), name).replace(' ', '_')


def _decode_name(name):
    return _decode_re.sub(lambda m: chr(int(m.group(1))), name).replace('_', ' ')


class _ShellNamespace(dict):
    def __missing__(self, key):
        try:
            return getattr(__builtins__, key)
        except AttributeError:
            # make names accessible
            return taxon(key)

    def keys(self):
        keys = set(super(_ShellNamespace, self).keys())
        keys |= set(dir(__builtins__))
        if not hasattr(self, '_names'):
            self._names = set(
                _encode_name(taxon.valid_name)
                for taxon in Taxon.select(Taxon.valid_name)
                if taxon.valid_name is not None
            )
        return keys | self._names

    def __delitem__(self, key):
        if super(_ShellNamespace, self).__contains__(key):
            super(_ShellNamespace, self).__delitem__(key)

    def clear_cache(self):
        del self._names

    def add_name(self, taxon):
        if hasattr(self, '_names') and taxon.valid_name is not None:
            self._names.add(taxon.valid_name.replace(' ', '_'))


class _NameGetter(object):
    def __init__(self, cls, field):
        self.cls = cls
        self.field = field
        self.field_obj = getattr(cls, field)
        self._data = None

    def __dir__(self):
        result = set(super().__dir__())
        if self._data is None:
            self._data = set()
            for obj in self.cls.select(self.field_obj):
                self._add_obj(obj)
        return result | self._data

    def __getattr__(self, name):
        return self.cls.filter(self.field_obj == _decode_name(name)).get()

    def __call__(self, name):
        return self.__getattr__(name)

    def clear_cache(self):
        self._data = None

    def add_name(self, nam):
        if self._data is not None:
            self._add_obj(nam)

    def _add_obj(self, obj):
        val = getattr(obj, self.field)
        if val is None:
            return
        self._data.add(_encode_name(val))


ns = _ShellNamespace({
    'constants': db.constants,
    'helpers': db.helpers,
    'definition': db.definition,
    'Branch': db.definition.Branch,
    'Node': db.definition.Node,
    'Apomorphy': db.definition.Apomorphy,
    'Other': db.definition.Other,
    'N': _NameGetter(Name, 'root_name'),
    'L': _NameGetter(db.models.Location, 'name'),
    'P': _NameGetter(db.models.Period, 'name'),
    'R': _NameGetter(db.models.Region, 'name'),
})
ns.update(db.constants.__dict__)

for model in db.models.BaseModel.__subclasses__():
    ns[model.__name__] = model

events.on_new_taxon.on(ns.add_name)
events.on_taxon_save.on(ns.add_name)
events.on_name_save.on(ns['N'].add_name)


def command(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except getinput.StopException:
            return None

    ns[fn.__name__] = wrapper
    return wrapper


# Shell internal commands

@command
def clear_cache():
    """Clears the autocomplete cache."""
    ns.clear_cache()


# Lookup

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


# Maintenance

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
    for name in Name.filter(Name.original_citation != None, Name.type >> None, Name.year > '1930', Name.group == db.constants.GROUP_GENUS).order_by(Name.original_citation):
        name.taxon.display(full=True, max_depth=1)
        message = 'Name %s is missing type, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        verbatim_type = getinput.get_line(
            message, handlers={'o': lambda _: name.open_description()}, should_stop=lambda line: line == 's'
        )
        if verbatim_type is not None:
            name.detect_and_set_type(verbatim_type, verbose=True)


@command
def find_rank_mismatch():
    for taxon in Taxon.select():
        expected_group = db.helpers.group_of_rank(taxon.rank)
        if expected_group != taxon.base_name.group:
            rank = db.constants.string_of_rank(taxon.rank)
            group = db.constants.string_of_group(taxon.base_name.group)
            print("Group mismatch for %s: rank %s but group %s" % (taxon, rank, group))


@command
def detect_types(max_count=None, verbose=False):
    """Converts verbatim_types into references to the actual names."""
    count = 0
    successful_count = 0
    group = (db.constants.GROUP_FAMILY, db.constants.GROUP_GENUS)
    for name in Name.filter(Name.verbatim_type != None, Name.type >> None, Name.group << group).limit(max_count):
        count += 1
        if name.detect_and_set_type(verbatim_type=name.verbatim_type, verbose=verbose):
            successful_count += 1
    print("Success: %d/%d" % (successful_count, count))


@command
def detect_types_from_root_names(max_count=None):
    """Detects types for family-group names on the basis of the root_name."""
    def detect_from_root_name(name, root_name):
        candidates = Name.filter(Name.group == db.constants.GROUP_GENUS, (Name.stem == root_name) | (Name.stem == root_name + 'i'))
        candidates = list(filter(lambda c: c.taxon.is_child_of(name.taxon), candidates))
        if len(candidates) == 1:
            print("Detected type for name %s: %s" % (name, candidates[0]))
            name.type = candidates[0]
            name.save()
            return True
        else:
            return False

    count = 0
    successful_count = 0
    for name in Name.filter(Name.group == db.constants.GROUP_FAMILY, Name.type >> None).order_by(Name.id.desc()).limit(max_count):
        if name.is_unavailable():
            continue
        count += 1
        if detect_from_root_name(name, name.root_name):
            successful_count += 1
        else:
            for stripped in db.helpers.name_with_suffixes_removed(name.root_name):
                if detect_from_root_name(name, stripped):
                    successful_count += 1
                    break
            else:
                print("Could not detect type for name %s (root_name = %s)" % (name, name.root_name))
    print("Success: %d/%d" % (successful_count, count))


@command
def endswith(end):
    return list(Name.filter(Name.group == db.constants.GROUP_GENUS, Name.root_name % ('%%%s' % end)))


@command
def detect_stems():
    for name in Name.filter(Name.group == db.constants.GROUP_GENUS, Name.stem >> None):
        inferred = db.detection.detect_stem_and_gender(name.root_name)
        if inferred is None:
            continue
        if not inferred.confident:
            print('%s: stem %s, gender %s' % (name.description(), inferred.stem, inferred.gender))
            if not getinput.yes_no('Is this correct? '):
                continue
        print("Inferred stem and gender for %s: %s, %s" % (name, inferred.stem, inferred.gender))
        name.stem = inferred.stem
        name.gender = inferred.gender
        name.save()


@command
def root_name_mismatch():
    for name in Name.filter(Name.group == db.constants.GROUP_FAMILY, ~(Name.type >> None)):
        if name.is_unavailable():
            continue
        stem_name = name.type.stem
        if stem_name is None:
            continue
        if name.root_name == stem_name:
            continue
        for stripped in db.helpers.name_with_suffixes_removed(name.root_name):
            if stripped == stem_name or stripped + 'i' == stem_name:
                print('Autocorrecting root name: %s -> %s' % (name.root_name, stem_name))
                name.root_name = stem_name
                name.save()
                break
        if name.root_name != stem_name:
            print('Stem mismatch for %s: %s vs. %s' % (name, name.root_name, stem_name))


@command
def stem_statistics():
    stem = Name.filter(Name.group == db.constants.GROUP_GENUS, ~(Name.stem >> None)).count()
    gender = Name.filter(Name.group == db.constants.GROUP_GENUS, ~(Name.gender >> None)).count()
    total = Name.filter(Name.group == db.constants.GROUP_GENUS).count()
    print("Genus-group names:")
    print("stem: %s/%s (%.02f%%)" % (stem, total, stem / total * 100))
    print("gender: %s/%s (%.02f%%)" % (gender, total, gender / total * 100))
    print("Family-group names:")
    total = Name.filter(Name.group == db.constants.GROUP_FAMILY).count()
    typ = Name.filter(Name.group == db.constants.GROUP_FAMILY, ~(Name.type >> None)).count()
    print("type: %s/%s (%.02f%%)" % (typ, total, typ / total * 100))


@command
def name_mismatches(max_count=None):
    count = 0
    for taxon in Taxon.select():
        computed = taxon.compute_valid_name()
        if taxon.valid_name != computed:
            print("Mismatch for %s: %s (actual) vs. %s (computed)" % (taxon, taxon.valid_name, computed))
            count += 1
            if max_count is not None and count == max_count:
                return


@command
def authorless_names(root_taxon):
    for nam in root_taxon.names:
        if nam.authority is None:
            print(nam)
    for child in root_taxon.children:
        authorless_names(child)


# Statistics

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
def bad_base_names():
    return list(Taxon.raw('SELECT * FROM taxon WHERE base_name_id IS NULL OR base_name_id NOT IN (SELECT id FROM name)'))


@command
def bad_taxa():
    return list(Name.raw('SELECT * FROM name WHERE taxon_id IS NULL or taxon_id NOT IN (SELECT id FROM taxon)'))


@command
def fossilize(taxon, to_status=db.constants.AGE_FOSSIL, from_status=db.constants.AGE_EXTANT):
    if taxon.age != from_status:
        return
    taxon.age = to_status
    taxon.save()
    for child in taxon.children:
        fossilize(child, to_status=to_status, from_status=from_status)


def run_shell():
    config = IPython.config.loader.Config()
    config.InteractiveShellEmbed.confirm_exit = False
    IPython.start_ipython(config=config, user_ns=ns)


if __name__ == '__main__':
    run_shell()
