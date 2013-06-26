# http://stackoverflow.com/questions/372885/how-do-i-connect-to-a-mysql-database-in-python
# generated by pwiz.py
from peewee import *
import settings

database = MySQLDatabase(settings.DATABASE, user=settings.USER, passwd=settings.PASSWD)

class BaseModel(Model):
    class Meta:
        database = database

class Taxon(BaseModel):
    comments = TextField(null=True)
    data = TextField(null=True)
    rank = IntegerField()
    valid_name = CharField()
    parent = ForeignKeyField('self', related_name='children', null=True, db_column='parent_id')

    class Meta:
        db_table = 'taxon'

class Name(BaseModel):
    authority = CharField(null=True)
    base_name = CharField()
    data = TextField(null=True)
    group = IntegerField()
    nomenclature_comments = TextField(null=True)
    original_citation = CharField(null=True)
    original_name = CharField(null=True)
    other_comments = TextField(null=True)
    page_described = CharField(null=True)
    status = IntegerField()
    taxon = ForeignKeyField(Taxon, related_name='names', db_column='taxon_id')
    taxonomy_comments = TextField(null=True)
    type = ForeignKeyField('self', null=True, db_column='type_id')
    year = CharField(null=True)

    class Meta:
        db_table = 'name'
