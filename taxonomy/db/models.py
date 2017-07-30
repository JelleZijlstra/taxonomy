import collections
import json
import operator
import re
import sys
import traceback

from peewee import (
    MySQLDatabase, Model, IntegerField, CharField, ForeignKeyField, TextField, BooleanField
)
import peewee

from .. import events
from .. import getinput

from . import constants
from . import definition
from . import ehphp
from . import helpers
from . import settings

database = MySQLDatabase(settings.DATABASE, user=settings.USER, passwd=settings.PASSWD, charset='utf8')
database.get_conn().ping(True)


class BaseModel(Model):
    creation_event = None
    save_event = None

    class Meta(object):
        database = database

    @classmethod
    def create(cls, *args, **kwargs):
        result = super().create(*args, **kwargs)
        if cls.creation_event is not None:
            cls.creation_event.trigger(result)
        return result

    def save(self, *args, **kwargs):
        result = super().save(*args, **kwargs)
        if self.save_event is not None:
            self.save_event.trigger(self)
        return result

    def dump_data(self):
        return "%s(%r)" % (self.__class__.__name__, self.__dict__)

    def full_data(self):
        for field in sorted(self.fields()):
            try:
                value = getattr(self, field)
                if value is not None:
                    print("{}: {}".format(field, value))
            except Exception:
                traceback.print_exc()
                print('{}: could not get value'.format(field))

    def s(self, **kwargs):
        """Set attributes on the object.

        Use this in the shell instead of directly assigning properties because that does
        not automatically save the object. This is especially problematic if one does
        something like `Oryzomys.base_name.authority = 'Smith'`, because `Oryzomys.base_name`
        creates a temporary object that is immediately thrown away.

        """
        for name, value in kwargs.items():
            assert hasattr(self, name), 'Invalid attribute %s' % name
            setattr(self, name, value)
        self.save()

    def __hash__(self):
        return self.id

    def __del__(self):
        if self.is_dirty():
            try:
                self.save()
            except peewee.IntegrityError:
                pass

    @classmethod
    def fields(cls):
        for field in dir(cls):
            if isinstance(getattr(cls, field), peewee.Field):
                yield field

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join('%s=%s' % (field, getattr(self, field)) for field in self.fields()))

    def _merge_fields(self, into, exclude=set()):
        for field in self.fields():
            if field in exclude:
                continue
            my_data = getattr(self, field)
            into_data = getattr(into, field)
            if my_data is None:
                pass
            elif into_data is None:
                print('setting %s: %s' % (field, my_data))
                setattr(into, field, my_data)
            elif my_data != into_data:
                print('warning: dropping %s: %s' % (field, my_data))
        into.save()


class _EnumFieldDescriptor(peewee.FieldDescriptor):
    def __init__(self, field, enum):
        super().__init__(field)
        self.enum = enum

    def __get__(self, instance, instance_type=None):
        value = super().__get__(instance, instance_type=instance_type)
        if isinstance(value, int):
            value = self.enum(value)
        return value

    def __set__(self, instance, value):
        if isinstance(value, self.enum):
            value = value.value
        super().__set__(instance, value)


class EnumField(IntegerField):
    def __init__(self, enum, **kwargs):
        super().__init__(**kwargs)
        self.enum = enum

    def add_to_class(self, model_class, name):
        super().add_to_class(model_class, name)
        setattr(model_class, name, _EnumFieldDescriptor(self, self.enum))


class Taxon(BaseModel):
    creation_event = events.on_new_taxon
    save_event = events.on_taxon_save

    rank = EnumField(constants.Rank)
    valid_name = CharField(default='')
    age = EnumField(constants.Age)
    parent = ForeignKeyField('self', related_name='children', null=True, db_column='parent_id')
    comments = TextField(null=True)
    data = TextField(null=True)
    is_page_root = BooleanField(default=False)
    _base_name_id = IntegerField(null=True, db_column='base_name_id')

    class Meta(object):
        db_table = 'taxon'

    name = property(lambda self: self.base_name)

    @property
    def base_name(self):
        try:
            return Name.get(Name.id == self._base_name_id)
        except Name.DoesNotExist:
            return None

    @base_name.setter
    def base_name(self, value):
        self._base_name_id = value.id
        Taxon.update(_base_name_id=value.id).where(Taxon.id == self.id).execute()
        self.save()

    def group(self):
        return helpers.group_of_rank(self.rank)

    def sorted_names(self, exclude_valid=False):
        names = self.names
        if exclude_valid:
            names = filter(lambda name: name.status != constants.STATUS_VALID, names)
        return sorted(names, key=operator.attrgetter('status', 'root_name'))

    def sorted_children(self):
        children = self.children
        return sorted(children, key=operator.attrgetter('rank', 'valid_name'))

    def sorted_occurrences(self):
        return sorted(self.occurrences, key=lambda o: o.location.name)

    def root_name(self):
        return self.valid_name.split(' ')[-1]

    def all_data(self):
        self.full_data()
        self.base_name.full_data()
        print(self.sorted_names())
        print(self.sorted_children())
        print(self.sorted_occurrences())

    def full_name(self):
        if self.rank == constants.SUBGENUS:
            return self.parent.valid_name + ' (' + self.valid_name + ')'
        if self.rank == constants.SPECIES_GROUP:
            return self.parent.full_name() + ' (' + self.base_name.root_name + ')'
        elif self.rank == constants.SPECIES:
            if self.parent.rank > constants.GENUS:
                return self.valid_name
            parent_name = self.parent.full_name()
            if self.parent.needs_is():
                parent_name += " (?)"
            return parent_name + " " + self.base_name.root_name
        elif self.rank == constants.SUBSPECIES:
            return self.parent.full_name() + " " + self.base_name.root_name
        else:
            return self.valid_name

    def needs_is(self):
        if not hasattr(self, '_needs_is'):
            if self.rank == constants.SUBGENUS:
                self._needs_is = Taxon.select().where(Taxon.parent == self, Taxon.rank == constants.SPECIES_GROUP).count() > 0
            elif self.rank == constants.GENUS:
                self._needs_is = Taxon.select().where(Taxon.parent == self, (Taxon.rank == constants.SUBGENUS) | (Taxon.rank == constants.SPECIES_GROUP)).count() > 0
            else:
                self._needs_is = False
        return self._needs_is

    def parent_of_rank(self, rank, original_taxon=None):
        if original_taxon is None:
            original_taxon = self
        if self.rank > rank and self.rank != constants.UNRANKED:
            raise ValueError("%s (id = %s) has no ancestor of rank %s" % (original_taxon, original_taxon.id, constants.string_of_rank(rank)))
        elif self.rank == rank:
            return self
        else:
            return self.parent.parent_of_rank(rank, original_taxon=original_taxon)

    def has_parent_of_rank(self, rank):
        try:
            self.parent_of_rank(rank)
        except ValueError:
            return False
        else:
            return True

    def is_child_of(self, taxon):
        if self == taxon:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.is_child_of(taxon)

    def children_of_rank(self, rank, age=None):
        if self.rank < rank:
            return []
        elif self.rank == rank:
            if age is None or self.age == age:
                return [self]
            else:
                return []
        else:
            out = []
            for child in self.children:
                out += child.children_of_rank(rank, age=age)
            return out

    def find_names(self, root_name, group=None, fuzzy=True):
        """Find instances of the given root_name within the given container taxon."""
        if fuzzy:
            query = Name.root_name % root_name
        else:
            query = Name.root_name == root_name  # LIKE
        candidates = Name.filter(query)

        result = []
        # maybe I could do some internal caching here but for now this is fast enough
        for candidate in candidates:
            if group is not None and candidate.group != group:
                continue
            taxon = candidate.taxon
            while taxon.parent is not None:
                if taxon.id == self.id:
                    result.append(candidate)
                    break
                taxon = taxon.parent
        return result

    def display(self, full=False, max_depth=None, file=sys.stdout, depth=0, exclude=set(), exclude_fn=None, name_exclude_fn=None, show_occurrences=True):
        if exclude_fn is not None and exclude_fn(self):
            return
        file.write(' ' * (4 * depth))
        file.write('%s %s (%s)\n' % (constants.string_of_rank(self.rank), self.full_name(), constants.string_of_age(self.age)))
        if full:
            data = {
                'comments': self.comments,
                'data': self.data,
                'is_page_root': self.is_page_root,
            }
            for key, value in data.items():
                if value:
                    file.write(' ' * ((depth + 1) * 4))
                    file.write('%s: %s\n' % (key, value))
        for name in self.sorted_names():
            if name_exclude_fn is None or not name_exclude_fn(name):
                file.write(name.display(depth=depth + 1, full=full))
        if show_occurrences:
            for occurrence in self.sorted_occurrences():
                file.write(' ' * ((depth + 1) * 4))
                file.write('%s\n' % (occurrence if full else occurrence.location))
        if self in exclude:
            return
        if max_depth is None or max_depth > 0:
            new_max_depth = None if max_depth is None else max_depth - 1
            for child in self.sorted_children():
                child.display(file=file, depth=depth + 1, max_depth=new_max_depth, full=full, exclude=exclude, exclude_fn=exclude_fn, name_exclude_fn=name_exclude_fn, show_occurrences=show_occurrences)

    def display_parents(self, max_depth=None, file=sys.stdout):
        if max_depth == 0:
            return
        if max_depth is not None:
            max_depth -= 1
        if self.parent is not None:
            self.parent.display_parents(max_depth=max_depth, file=file)

        file.write('%s %s (%s)\n' % (constants.string_of_rank(self.rank), self.full_name(), constants.string_of_age(self.age)))
        file.write(self.base_name.display(depth=1))

    def ranked_parents(self):
        """Returns the order-level and family-level parents of the taxon.

        The family-level parents is the one parent of family rank. The order-level parent
        is of rank order if there is one, and otherwise the first unranked taxon above the
        highest-ranked family-group taxon.

        """
        family_rank = None
        order_rank = None
        current_parent = self
        while current_parent is not None:
            parent_rank = current_parent.rank
            if parent_rank == constants.FAMILY:
                family_rank = current_parent
            if helpers.group_of_rank(parent_rank) == constants.GROUP_FAMILY:
                order_rank = None
            if parent_rank == constants.ORDER:
                order_rank = current_parent
                break
            if parent_rank == constants.UNRANKED and order_rank is None:
                order_rank = current_parent
            if parent_rank > constants.ORDER and parent_rank != constants.UNRANKED:
                break

            current_parent = current_parent.parent
        return order_rank, family_rank

    def add(self, rank, name, authority=None, year=None, age=None, type=False, comments=None, **kwargs):
        if age is None:
            age = self.age
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self, comments=comments)
        kwargs['group'] = helpers.group_of_rank(rank)
        kwargs['root_name'] = helpers.root_name_of_name(name, rank)
        if 'status' not in kwargs:
            kwargs['status'] = constants.STATUS_VALID
        name = Name.create(taxon=taxon, **kwargs)
        if authority is not None:
            name.authority = authority
        if year is not None:
            name.year = year
        name.save()
        taxon.base_name = name
        if type:
            self.base_name.type = name
            self.save()
        taxon.save()
        return taxon

    def add_syn(self, root_name, authority=None, year=None, original_name=None, original_citation=None, page_described=None, status=constants.STATUS_SYNONYM, **kwargs):
        kwargs['root_name'] = root_name
        kwargs['authority'] = authority
        kwargs['year'] = year
        # included in the method signature so they autocomplete in shell
        kwargs['original_name'] = original_name
        kwargs['original_citation'] = original_citation
        kwargs['page_described'] = page_described
        kwargs['status'] = status
        kwargs['taxon'] = self
        if 'group' not in kwargs:
            kwargs['group'] = self.base_name.group
        return Name.create(**kwargs)

    def add_type_identical(self, name, page_described=None, locality=None, **kwargs):
        """Convenience method to add a type species described in the same paper as the genus."""
        assert self.rank == constants.GENUS
        assert self.base_name.type is None
        full_name = '%s %s' % (self.valid_name, name)
        result = self.add(
            constants.SPECIES, full_name, type=True, authority=self.base_name.authority, year=self.base_name.year,
            original_citation=self.base_name.original_citation, original_name=full_name, page_described=page_described,
            status=self.base_name.status)
        self.base_name.type = result.base_name
        self.save()
        if locality is not None:
            result.add_occurrence(locality)
        result.base_name.s(**kwargs)
        return result

    def add_occurrence(self, location, paper=None, comment=None, status=constants.OccurrenceStatus.valid):
        if paper is None:
            paper = self.base_name.original_citation
        try:
            return Occurrence.create(taxon=self, location=location, source=paper, comment=comment, status=status)
        except peewee.IntegrityError:
            print("DUPLICATE OCCURRENCE")
            return self.at(location)

    def syn_from_paper(self, name, paper, page_described=None, status=constants.STATUS_SYNONYM, group=None, age=None, **kwargs):
        authority, year = ehphp.call_ehphp('taxonomicAuthority', [paper])
        result = self.add_syn(
            root_name=name, authority=authority, year=year, original_citation=paper,
            page_described=page_described, original_name=name, status=status, age=age,
        )
        if group is not None:
            kwargs['group'] = group
        result.s(**kwargs)
        return result

    def from_paper(self, rank, name, paper, page_described=None, status=constants.STATUS_VALID, comments=None, age=None, **override_kwargs):
        authority, year = ehphp.call_ehphp('taxonomicAuthority', [paper])
        result = self.add(
            rank=rank, name=name, original_citation=paper, page_described=page_described,
            original_name=name, authority=authority, year=year, parent=self, status=status,
            comments=comments, age=age
        )
        result.base_name.s(**override_kwargs)
        return result

    def add_nominate(self):
        if self.rank == constants.SPECIES:
            rank = constants.SUBSPECIES
        elif self.rank == constants.GENUS:
            rank = constants.SUBGENUS
        elif self.rank == constants.TRIBE:
            rank = constants.SUBTRIBE
        elif self.rank == constants.SUBFAMILY:
            rank = constants.TRIBE
        elif self.rank == constants.FAMILY:
            rank = constants.SUBFAMILY
        elif self.rank == constants.SUPERFAMILY:
            rank = constants.FAMILY
        else:
            assert False, 'Cannot add nominate subtaxon of %s of rank %s' % (self, helpers.string_of_rank(self.rank))

        taxon = Taxon.create(age=self.age, rank=rank, parent=self)
        taxon.base_name = self.base_name
        taxon.base_name.taxon = taxon
        taxon.recompute_name()
        return taxon

    def syn(self, name=None, **kwargs):
        """Find a synonym matching the given arguments."""
        if name is not None:
            kwargs['root_name'] = name
        for candidate in self.sorted_names():
            for key, value in kwargs.items():
                if getattr(candidate, key) != value:
                    break
            else:
                return candidate
        else:
            return None

    def open_description(self):
        return self.base_name.open_description()

    def compute_valid_name(self):
        name = self.base_name
        if name is None:
            raise Name.DoesNotExist("Taxon with id %d has an invalid base_name" % self.id)
        if self.rank == constants.DIVISION:
            return '%s Division' % name.root_name
        elif name.group in (constants.GROUP_GENUS, constants.GROUP_HIGH):
            return name.root_name
        elif name.group == constants.GROUP_FAMILY:
            return name.root_name + helpers.suffix_of_rank(self.rank)
        else:
            assert name.group == constants.GROUP_SPECIES
            try:
                genus = self.parent_of_rank(constants.GENUS)
            except ValueError:
                # if there is no genus, just use the original name
                # this may be one case where we can't rely on the computed valid name
                assert self.rank == constants.SPECIES, 'Taxon %s should have a genus parent' % self
                # default to the original name for now. This isn't ideal because sometimes the original name
                # contains misspellings, but we don't really have a place to store that information better.
                return name.original_name
            else:
                if self.rank == constants.SPECIES_GROUP:
                    return '%s (%s)' % (genus.base_name.root_name, name.root_name)
                elif self.rank == constants.SPECIES:
                    return '%s %s' % (genus.base_name.root_name, name.root_name)
                else:
                    assert self.rank == constants.SUBSPECIES, "Unexpected rank %s" % constants.string_of_rank(self.rank)
                    species = self.parent_of_rank(constants.SPECIES)
                    return '%s %s %s' % (genus.base_name.root_name, species.base_name.root_name, name.root_name)

    def recompute_name(self):
        new_name = self.compute_valid_name()
        if new_name != self.valid_name and new_name is not None:
            print('Changing valid name: %s -> %s' % (self.valid_name, new_name))
            self.valid_name = new_name
            self.save()

    def merge(self, into):
        for child in self.children:
            child.parent = into
            child.save()
        for nam in self.names:
            if nam != self.base_name:
                nam.taxon = into
                nam.save()

        self._merge_fields(into, exclude={'id', '_base_name_id'})
        self.base_name.merge(into.base_name, allow_valid=True)
        self.remove()

    def synonymize(self, to_taxon):
        if self.comments is not None:
            print("Warning: removing comments: %s" % self.comments)
        if self.data is not None:
            print("Warning: removing data: %s" % self.data)
        assert self != to_taxon, 'Cannot synonymize %s with itself' % self
        for child in self.children:
            child.parent = to_taxon
            child.save()
        nam = self.base_name
        nam.status = constants.STATUS_SYNONYM
        nam.save()
        for name in self.names:
            name.taxon = to_taxon
            name.save()
        for occ in self.occurrences:
            occ.taxon = to_taxon
            comment = occ.comment
            try:
                occ.add_comment('Previously under _%s_.' % self.name)
                occ.save()
            except peewee.IntegrityError:
                print('dropping duplicate occurrence %s' % occ)
                existing = to_taxon.at(occ.location)
                additional_comment = 'Also under _%s_ with source {%s}.' % (self.name, occ.source)
                if comment is not None:
                    additional_comment += ' ' + comment
                existing.add_comment(additional_comment)
        to_taxon.base_name.status = constants.STATUS_VALID
        self.delete_instance()
        return Name.get(Name.id == nam.id)

    def make_species_group(self):
        if self.parent.rank == constants.SPECIES_GROUP:
            parent = self.parent.parent
        else:
            parent = self.parent
        new_taxon = Taxon.create(rank=constants.SPECIES_GROUP, age=self.age, parent=parent)
        new_taxon.base_name = self.base_name
        new_taxon.recompute_name()
        self.parent = new_taxon
        self.save()
        return new_taxon

    def run_on_self_and_children(self, callback):
        callback(self)
        for child in self.children:
            child.run_on_self_and_children(callback)

    def remove(self):
        if self.children.count() != 0:
            print('Cannot remove %s since it has unremoved children' % self)
            return
        print('Removing taxon %s' % self)
        for name in self.sorted_names():
            name.remove()
        self.delete_instance()

    def all_names(self):
        names = set(self.names)
        for child in self.children:
            names |= child.all_names()
        return names

    def stats(self):
        attributes = ['original_name', 'original_citation', 'page_described', 'authority', 'year']
        names = self.all_names()
        counts = collections.defaultdict(int)
        for name in names:
            for attribute in attributes:
                if getattr(name, attribute) is not None:
                    counts[attribute] += 1

        total = len(names)
        output = {'total': total}
        print("Total names:", total)
        for attribute in attributes:
            percentage = counts[attribute] * 100.0 / total
            print("%s: %s (%.2f%%)" % (attribute, counts[attribute], percentage))
            output[attribute] = percentage
        return output

    class _OccurrenceGetter(object):
        """For easily accessing occurrences of a taxon.

        This is exposed at taxon.at. You can access taxa as either taxon.at.Locality_Name or taxon.at(L.Locality_Name).

        """
        def __init__(self, instance=None):
            self.instance = instance

        def __get__(self, instance, instance_type):
            return self.__class__(instance)

        def __getattr__(self, loc_name):
            return self(Location.get(Location.name == loc_name.replace('_', ' ')))

        def __call__(self, loc):
            return self.instance.occurrences.filter(Occurrence.location == loc).get()

        def __dir__(self):
            return [o.location.name.replace(' ', '_') for o in self.instance.occurrences]

    at = _OccurrenceGetter()

    def __str__(self):
        return self.valid_name

    def __repr__(self):
        return str(self)

    def __getattr__(self, attr):
        """Returns a name belonging to this taxon with the given root_name or original_name."""
        candidates = [name for name in self.sorted_names() if name.root_name == attr or name.original_name == attr]
        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) == 0:
            raise AttributeError(attr)
        else:
            raise Name.DoesNotExist("Candidates: {}".format(candidates))

    def __dir__(self):
        result = set(super(Model, self).__dir__())
        names = self.sorted_names()
        result |= set(name.original_name for name in names)
        result |= set(name.root_name for name in names)
        result = [name for name in result if name is not None and ' ' not in name]
        return result

definition.taxon_cls = Taxon


class Name(BaseModel):
    creation_event = events.on_new_name
    save_event = events.on_name_save

    root_name = CharField()
    group = EnumField(constants.Group)
    status = EnumField(constants.Status)
    taxon = ForeignKeyField(Taxon, related_name='names', db_column='taxon_id')
    authority = CharField(null=True)
    data = TextField(null=True)
    nomenclature_comments = TextField(null=True)
    original_citation = CharField(null=True)
    original_name = CharField(null=True)
    other_comments = TextField(null=True)
    page_described = CharField(null=True)
    stem = CharField(null=True)
    gender = EnumField(constants.Gender)
    taxonomy_comments = TextField(null=True)
    type = ForeignKeyField('self', null=True, db_column='type_id')
    verbatim_type = CharField(null=True)
    verbatim_citation = CharField(null=True)
    year = CharField(null=True)
    _definition = CharField(null=True, db_column='definition')

    @property
    def definition(self):
        data = self._definition
        if data is None:
            return None
        else:
            return definition.Definition.unserialize(data)

    @definition.setter
    def definition(self, definition):
        self._definition = definition.serialize()

    class Meta(object):
        db_table = 'name'

    def add_additional_data(self, new_data):
        '''Add data to the "additional" field within the "data" field'''
        data = json.loads(self.data)
        if 'additional' not in data:
            data['additional'] = []
        data['additional'].append(new_data)
        self.data = json.dumps(data)
        self.save()

    def add_data(self, field, value):
        if self.data is None or self.data == '':
            data = {}
        else:
            data = json.loads(self.data)
        data[field] = value
        self.data = json.dumps(data)

    def description(self):
        if self.original_name:
            out = self.original_name
        else:
            out = self.root_name
        if self.authority:
            out += " %s" % self.authority
        if self.year:
            out += ", %s" % self.year
        out += " (= %s)" % self.taxon.valid_name
        return out

    def is_unavailable(self):
        # TODO: generalize this
        return self.nomenclature_comments is not None and \
            'Unavailable because not based on a generic name.' in self.nomenclature_comments

    def display(self, full=False, depth=0):
        if self.original_name is None:
            out = self.root_name
        else:
            out = self.original_name
        if self.authority is not None:
            out += ' %s' % self.authority
        if self.year is not None:
            out += ', %s' % self.year
        if self.page_described is not None:
            out += ':%s' % self.page_described
        if self.original_citation is not None:
            out += ' {%s}' % self.original_citation
        if self.type is not None:
            out += ' (type: %s)' % self.type
        out += ' (%s)' % (constants.string_of_status(self.status))
        if full and (self.original_name is not None or self.stem is not None or self.gender is not None or self.definition is not None):
            parts = []
            if self.original_name is not None:
                parts.append('root: %s' % self.root_name)
            if self.stem is not None:
                parts.append('stem: %s' % self.stem)
            if self.gender is not None:
                parts.append(constants.Gender(self.gender).name)
            if self.definition is not None:
                parts.append(str(self.definition))
            out += ' (%s)' % '; '.join(parts)
        if self.is_well_known():
            intro_line = getinput.green(out)
        else:
            intro_line = getinput.red(out)
        result = ' ' * ((depth + 1) * 4) + intro_line + '\n'
        if full:
            data = {
                'nomenclature_comments': self.nomenclature_comments,
                'other_comments': self.other_comments,
                'taxonomy_comments': self.taxonomy_comments,
                'verbatim_type': self.verbatim_type,
                'verbatim_citation': self.verbatim_citation,
            }
            result = ''.join([result] + [
                ' ' * ((depth + 2) * 4) + '%s: %s\n' % (key, value)
                for key, value in data.items()
                if value
            ])
        return result

    def is_well_known(self):
        """Returns whether all necessary attributes of the name have been filled in."""
        if self.authority is None or self.year is None or self.page_described is None or self.original_citation is None or self.original_name is None:
            return False
        elif self.group in (constants.GROUP_FAMILY, constants.GROUP_GENUS) and self.type is None:
            return False
        elif self.group == constants.GROUP_GENUS and (self.stem is None or self.gender is None):
            return False
        else:
            return True

    def validate(self, status=constants.STATUS_VALID, parent=None, new_rank=None):
        assert self.status not in (constants.STATUS_VALID, constants.STATUS_NOMEN_DUBIUM)
        old_taxon = self.taxon
        parent_group = helpers.group_of_rank(old_taxon.rank)
        if self.group == constants.GROUP_SPECIES and parent_group != constants.GROUP_SPECIES:
            if new_rank is None:
                new_rank = constants.SPECIES
            if parent is None:
                parent = old_taxon
        elif self.group == constants.GROUP_GENUS and parent_group != constants.GROUP_GENUS:
            if new_rank is None:
                new_rank = constants.GENUS
            if parent is None:
                parent = old_taxon
        elif self.group == constants.GROUP_FAMILY and parent_group != constants.GROUP_FAMILY:
            if new_rank is None:
                new_rank = constants.FAMILY
            if parent is None:
                parent = old_taxon
        else:
            if new_rank is None:
                new_rank = old_taxon.rank
            if parent is None:
                parent = old_taxon.parent
        new_taxon = Taxon.create(rank=new_rank, parent=parent, age=old_taxon.age, valid_name='')
        new_taxon.base_name = self
        new_taxon.valid_name = new_taxon.compute_valid_name()
        new_taxon.save()
        self.taxon = new_taxon
        self.status = status
        self.save()
        return new_taxon

    def merge(self, into, allow_valid=False):
        if not allow_valid:
            assert self.status in (constants.STATUS_SYNONYM, constants.STATUS_DUBIOUS), \
                'Can only merge synonymous names (not %s)' % self
        self._merge_fields(into, exclude={'id'})
        self.remove()

    def open_description(self):
        if self.original_citation is None:
            print("%s: original citation unknown" % self.description())
        else:
            ehphp.call_ehphp('openf', [self.original_citation])

    def remove(self):
        print("Deleting name: " + self.description())
        self.delete_instance()
        return True

    def original_valid(self):
        assert self.original_name is None
        assert self.status == constants.STATUS_VALID
        self.original_name = self.taxon.valid_name

    def compute_gender(self):
        assert self.group == constants.GROUP_SPECIES, 'Cannot compute gender outside the species group'
        genus = self.taxon.parent_of_rank(constants.GENUS)
        gender = genus.base_name.gender
        if gender is None:
            print('Parent genus %s does not have gender set' % genus)
            return
        computed = helpers.convert_gender(self.root_name, gender)
        if computed != self.root_name:
            print('Modifying root_name: %s -> %s' % (self.root_name, computed))
            self.root_name = computed
            self.save()

    def __str__(self):
        return self.description()

    def __repr__(self):
        return self.description()

    def set_paper(self, paper, page_described=None, original_name=None, force=False, **kwargs):
        authority, year = ehphp.call_ehphp('taxonomicAuthority', [paper])
        if original_name is None and self.status == constants.STATUS_VALID:
            original_name = self.taxon.valid_name
        attributes = [
            ('authority', authority), ('year', year), ('original_citation', paper),
            ('page_described', page_described), ('original_name', original_name),
        ]
        for label, value in attributes:
            if value is None:
                continue
            current_value = getattr(self, label)
            if current_value is not None:
                if current_value != value and current_value != str(value):
                    print('Warning: %s does not match (given as %s, paper has %s)' % (label, current_value, value))
                    if force:
                        setattr(self, label, value)
            else:
                setattr(self, label, value)
        self.s(**kwargs)

    def detect_and_set_type(self, verbatim_type=None, verbose=False):
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        if verbose:
            print('=== Detecting type for %s from %s' % (self, verbatim_type))
        candidates = self.detect_type(verbatim_type=verbatim_type, verbose=verbose)
        if candidates is None or len(candidates) == 0:
            print("Verbatim type %s for name %s could not be recognized" % (verbatim_type, self))
            return False
        elif len(candidates) == 1:
            if verbose:
                print('Detected type: %s' % candidates[0])
            self.type = candidates[0]
            self.save()
            return True
        else:
            print("Verbatim type %s for name %s yielded multiple possible names: %s" % (verbatim_type, self, candidates))
            return False

    def detect_type(self, verbatim_type=None, verbose=False):
        def cleanup(name):
            return re.sub(r'\s+', ' ', name.strip().rstrip('.').replace('<i>', '').replace('</i>', ''))

        steps = [
            lambda verbatim: verbatim,
            lambda verbatim: re.sub(r'\([^)]+\)', '', verbatim),
            lambda verbatim: re.sub(r'=.*$', '', verbatim),
            lambda verbatim: re.sub(r'\(.*$', '', verbatim),
            lambda verbatim: re.sub(r'\[.*$', '', verbatim),
            lambda verbatim: re.sub(r',.*$', '', verbatim),
            lambda verbatim: self._split_authority(verbatim)[0],
            lambda verbatim: verbatim.split()[1] if ' ' in verbatim else verbatim,
            lambda verbatim: helpers.convert_gender(verbatim, constants.Gender.masculine),
            lambda verbatim: helpers.convert_gender(verbatim, constants.Gender.feminine),
            lambda verbatim: helpers.convert_gender(verbatim, constants.Gender.neuter),
        ]
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        candidates = None
        for step in steps:
            new_verbatim = cleanup(step(verbatim_type))
            if verbatim_type != new_verbatim or candidates is None:
                if verbose:
                    print('Trying verbatim type: %s' % new_verbatim)
                verbatim_type = new_verbatim
                candidates = self.detect_type_from_verbatim_type(verbatim_type)
                if len(candidates) > 0:
                    return candidates
        return []

    def _split_authority(self, verbatim_type):
        # if there is an uppercase letter following an all-lowercase word (the species name),
        # the authority is included
        find_authority = re.match(r'^(.* [a-z]+) ([A-Z+].+)$', verbatim_type)
        if find_authority:
            return find_authority.group(1), find_authority.group(2)
        else:
            return verbatim_type, None

    def detect_type_from_verbatim_type(self, verbatim_type):
        def _filter_by_authority(candidates, authority):
            if authority is None:
                return candidates
            split = re.split(r', (?=\d)', authority, maxsplit=1)
            if len(split) == 1:
                author, year = authority, None
            else:
                author, year = split
            result = []
            for candidate in candidates:
                if candidate.authority != authority:
                    continue
                if year is not None and candidate.year != year:
                    continue
                result.append(candidate)
            return result

        parent = self.taxon
        if self.group == constants.GROUP_FAMILY:
            verbatim = verbatim_type.split(maxsplit=1)
            if len(verbatim) == 1:
                type_name, authority = verbatim, None
            else:
                type_name, authority = verbatim
            return _filter_by_authority(parent.find_names(verbatim[0], group=constants.GROUP_GENUS), authority)
        else:
            type_name, authority = self._split_authority(verbatim_type)
            if ' ' not in type_name:
                root_name = type_name
                candidates = Name.filter(Name.root_name == root_name, Name.group == constants.GROUP_SPECIES)
                find_abbrev = False
            else:
                find_abbrev = re.match(r'^[A-Z]\. ([a-z]+)$', type_name)
                if find_abbrev:
                    root_name = find_abbrev.group(1)
                    candidates = Name.filter(Name.root_name == root_name, Name.group == constants.GROUP_SPECIES)
                else:
                    candidates = Name.filter(Name.original_name == type_name, Name.group == constants.GROUP_SPECIES)
            # filter by authority first because it's cheaper
            candidates = _filter_by_authority(candidates, authority)
            candidates = [candidate for candidate in candidates if candidate.taxon.is_child_of(parent)]
            # if we failed to find using the original_name, try the valid_name
            if not candidates and not find_abbrev:
                candidates = Name.filter(Name.status == constants.STATUS_VALID).join(Taxon).where(Taxon.valid_name == type_name)
                candidates = _filter_by_authority(candidates, authority)
                candidates = [candidate for candidate in candidates if candidate.taxon.is_child_of(parent)]
            return candidates

    @classmethod
    def find_name(cls, name, rank=None, authority=None, year=None):
        '''Find a Name object corresponding to the given information'''
        if rank is None:
            group = None
            initial_lst = cls.select().where(cls.root_name == name)
        else:
            group = helpers.group_of_rank(rank)
            if group == constants.GROUP_FAMILY:
                root_name = helpers.strip_rank(name, rank, quiet=True)
            else:
                root_name = name
            initial_lst = cls.select().where(cls.root_name == root_name, cls.group == group)
        for nm in initial_lst:
            if authority is not None and nm.authority and nm.authority != authority:
                continue
            if year is not None and nm.year and nm.year != year:
                continue
            if group == constants.GROUP_FAMILY:
                if nm.original_name and nm.original_name != name and initial_lst.count() > 1:
                    continue
            return nm
        raise cls.DoesNotExist


class Period(BaseModel):
    save_event = events.on_period_save

    name = CharField()
    parent = ForeignKeyField('self', related_name='children', db_column='parent_id', null=True)
    prev = ForeignKeyField('self', related_name='next_foreign', db_column='prev_id', null=True)
    next = ForeignKeyField('self', related_name='prev_foreign', db_column='next_id', null=True)
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    min_period = ForeignKeyField('self', related_name='children_min', db_column='min_period_id', null=True)
    max_period = ForeignKeyField('self', related_name='children_max', db_column='max_period_id', null=True)
    system = EnumField(constants.PeriodSystem)
    comment = CharField()

    @staticmethod
    def _filter_none(seq):
        return (elt for elt in seq if elt is not None)

    def get_min_age(self):
        if self.min_age is not None:
            return self.min_age
        return min(self._filter_none(child.get_min_age() for child in self.children), default=None)

    def get_max_age(self):
        if self.max_age is not None:
            return self.max_age
        return max(self._filter_none(child.get_max_age() for child in self.children), default=None)

    @classmethod
    def make(cls, name, system, parent=None, next=None, min_age=None, max_age=None, **kwargs):
        if max_age is None and next is not None:
            max_age = next.min_age
        period = cls.create(name=name, system=system.value, parent=parent, next=next, min_age=min_age, max_age=max_age, **kwargs)
        if next is not None:
            next.prev = period
            next.save()
        return period

    @classmethod
    def make_stratigraphy(cls, name, kind, period=None, parent=None, **kwargs):
        if period is not None:
            kwargs['max_period'] = kwargs['min_period'] = period
        period = cls.create(name=name, system=kind.value, parent=parent, **kwargs)
        if 'next' in kwargs:
            next = kwargs['next']
            next.prev = period
            next.save()
        return period

    def display(self, full=False, depth=0, file=sys.stdout):
        file.write('%s%s\n' % (' ' * (depth + 4), repr(self)))
        for location in Location.filter(Location.max_period == self, Location.min_period == self):
            location.display(full=full, depth=depth + 2, file=file)
        for location in self.locations_stratigraphy:
            location.display(full=full, depth=depth + 2, file=file)
        for period in self.children:
            period.display(full=full, depth=depth + 1, file=file)
        for period in Period.filter(Period.max_period == self, Period.min_period == self):
            period.display(full=full, depth=depth + 1, file=file)

    def make_locality(self, region):
        return Location.make(self.name, region, self)

    def __repr__(self):
        properties = {}
        for field in self.fields():
            if field == 'name':
                continue
            value = getattr(self, field)
            if value is None:
                continue
            if isinstance(value, Period):
                value = value.name
            properties[field] = value
        return '%s (%s)' % (self.name, ', '.join('%s=%s' % item for item in properties.items()))


class Region(BaseModel):
    name = CharField()
    comment = CharField(null=True)
    parent = ForeignKeyField('self', related_name='children', db_column='parent_id', null=True)
    kind = EnumField(constants.RegionKind)

    @classmethod
    def make(cls, name, kind, parent=None):
        region = cls.create(name=name, kind=kind, parent=parent)
        Location.make(name=name, period=Period.filter(Period.name == 'Recent').get(), region=region)
        return region

    def __repr__(self):
        out = self.name
        if self.parent:
            out += ', %s' % self.parent.name
        out += ' (%s)' % self.kind
        return out

    def display(self, full=False, depth=0, file=sys.stdout):
        file.write('%s%s\n' % (' ' * (depth + 4), repr(self)))
        if self.comment:
            file.write('%sComment: %s\n' % (' ' * (depth + 12), self.comment))
        for location in self.locations:
            location.display(full=full, depth=depth + 4, file=file)
        for child in self.children:
            child.display(full=full, depth=depth + 4, file=file)


class Location(BaseModel):
    save_event = events.on_locality_save

    name = CharField()
    min_period = ForeignKeyField(Period, related_name='locations_min', db_column='min_period_id', null=True)
    max_period = ForeignKeyField(Period, related_name='locations_max', db_column='max_period_id', null=True)
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    stratigraphic_unit = ForeignKeyField(Period, related_name='locations_stratigraphy', db_column='stratigraphic_unit_id', null=True)
    region = ForeignKeyField(Region, related_name='locations', db_column='region_id')
    comment = CharField()

    @classmethod
    def make(cls, name, region, period, comment=None, stratigraphic_unit=None):
        return cls.create(
            name=name, min_period=period, max_period=period, region=region, comment=comment,
            stratigraphic_unit=stratigraphic_unit
        )

    def __repr__(self):
        age_str = ''
        if self.stratigraphic_unit is not None:
            age_str += self.stratigraphic_unit.name
        if self.max_period is not None:
            if self.stratigraphic_unit is not None:
                age_str += '; '
            age_str += self.max_period.name
            if self.min_period != self.max_period:
                age_str += '–%s' % self.min_period.name
        if self.min_age is not None and self.max_age is not None:
            age_str += '; %s–%s' % (self.max_age, self.min_age)
        return '%s (%s), %s' % (self.name, age_str, self.region.name)

    def display(self, full=False, depth=0, file=sys.stdout):
        file.write('%s%s\n' % (' ' * (depth + 4), repr(self)))
        if self.comment:
            file.write('%sComment: %s\n' % (' ' * (depth + 12), self.comment))
        if full:
            self.display_organized(depth=depth, file=file)
        else:
            for occurrence in sorted(self.taxa, key=lambda occ: occ.taxon.valid_name):
                file.write('%s%s\n' % (' ' * (depth + 8), occurrence))

    def display_organized(self, depth=0, file=sys.stdout):
        taxa = sorted(
            ((occ, occ.taxon.ranked_parents()) for occ in self.taxa),
            key=lambda pair: (
                '' if pair[1][0] is None else pair[1][0].valid_name,
                '' if pair[1][1] is None else pair[1][1].valid_name,
                pair[0].taxon.valid_name
            ))
        current_order = None
        current_family = None
        for occ, (order, family) in taxa:
            if order != current_order:
                current_order = order
                if order is not None:
                    file.write('%s%s\n' % (' ' * (depth + 8), order))
            if family != current_family:
                current_family = family
                if family is not None:
                    file.write('%s%s\n' % (' ' * (depth + 12), family))
            file.write('%s%s\n' % (' ' * (depth + 16), occ))

    def make_local_unit(self, name=None, parent=None):
        if name is None:
            name = self.name
        period = Period.make(name, constants.PeriodSystem.local_unit, parent=parent, min_age=self.min_age, max_age=self.max_age, min_period=self.min_period, max_period=self.max_period)
        self.min_period = self.max_period = period
        self.save()
        return period


class Occurrence(BaseModel):
    taxon = ForeignKeyField(Taxon, related_name='occurrences', db_column='taxon_id')
    location = ForeignKeyField(Location, related_name='taxa', db_column='location_id')
    comment = CharField()
    status = EnumField(constants.OccurrenceStatus, default=constants.OccurrenceStatus.valid)
    source = CharField()

    def add_comment(self, new_comment):
        if self.comment is None:
            self.comment = new_comment
        else:
            self.comment += ' ' + new_comment
        self.save()

    def __repr__(self):
        out = '%s in %s (%s%s)' % (self.taxon, self.location, self.source, '; ' + self.comment if self.comment else '')
        if self.status != constants.OccurrenceStatus.valid:
            out = '[%s] %s' % (self.status.name.upper(), out)
        return out
