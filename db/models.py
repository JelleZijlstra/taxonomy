# http://stackoverflow.com/questions/372885/how-do-i-connect-to-a-mysql-database-in-python
# generated by pwiz.py
from peewee import *
import json
import operator

import constants
import helpers
import settings

database = MySQLDatabase(settings.DATABASE, user=settings.USER, passwd=settings.PASSWD, charset='utf8')

class BaseModel(Model):
    class Meta:
        database = database

class Taxon(BaseModel):
    rank = IntegerField()
    valid_name = CharField()
    age = IntegerField()
    parent = ForeignKeyField('self', related_name='children', null=True, db_column='parent_id')
    comments = TextField(null=True)
    data = TextField(null=True)
    is_page_root = BooleanField(default=False)
    base_name_id = IntegerField(null=True)

    class Meta:
        db_table = 'taxon'

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

    def root_name(self):
        return self.valid_name.split(' ')[-1]

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

    def parent_of_rank(self, rank):
        if self.rank > rank:
            raise ValueError("%s has no ancestor of rank %s" % (self, rank))
        elif self.rank == rank:
            return self
        else:
            return self.parent.parent_of_rank(rank)

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

    def __str__(self):
        return self.valid_name

    def __repr__(self):
        return "Taxon(%r)" % self.__dict__

class Name(BaseModel):
    root_name = CharField()
    group = IntegerField()
    status = IntegerField()
    taxon = ForeignKeyField(Taxon, related_name='names', db_column='taxon_id')
    authority = CharField(null=True)
    data = TextField(null=True)
    nomenclature_comments = TextField(null=True)
    original_citation = CharField(null=True)
    original_name = CharField(null=True)
    other_comments = TextField(null=True)
    page_described = CharField(null=True)
    taxonomy_comments = TextField(null=True)
    type = ForeignKeyField('self', null=True, db_column='type_id')
    verbatim_type = CharField(null=True)
    verbatim_citation = CharField(null=True)
    year = CharField(null=True)

    class Meta:
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
            out += " " + self.authority
        if self.year:
            out += ", " + self.year
        out += " (= " + self.taxon.valid_name + ")"
        return out

    @classmethod
    def find_name(cls, name, rank, authority=None, year=None):
        '''Find a Name object corresponding to the given information'''
        group = helpers.group_of_rank(rank)
        if group == constants.GROUP_FAMILY:
            root_name = helpers.strip_rank(name, rank, quiet=True)
        else:
            root_name = name
        initial_lst = cls.select().where(cls.root_name == root_name, cls.group == group)
        for nm in initial_lst:
            if authority and nm.authority and nm.authority != authority:
                continue
            if year and nm.year and nm.year != year:
                continue
            if group == constants.GROUP_FAMILY:
                if nm.original_name and nm.original_name != name and initial_lst.count() > 1:
                    continue
            return nm
        raise cls.DoesNotExist

# Simulate peewee property
def _getter(self):
    try:
        return Name.get(Name.id == self.base_name_id)
    except Name.DoesNotExist:
        return None
def _setter(self, value):
    self.base_name_id = value.id
    Taxon.update(base_name_id=value.id).where(Taxon.id == self.id).execute()
    self.save()
Taxon.base_name = property(_getter, _setter)
