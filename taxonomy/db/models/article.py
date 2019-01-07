from peewee import CharField, IntegerField, TextField


from .base import BaseModel


class Article(BaseModel):
    addmonth = CharField()
    addday = CharField()
    addyear = CharField()
    path = CharField()
    name = CharField()
    authors = CharField()
    year = CharField()
    title = CharField()
    journal = CharField()
    series = CharField()
    volume = CharField()
    issue = CharField()
    start_page = CharField()
    end_page = CharField()
    url = CharField()
    doi = CharField()
    typ = IntegerField(db_column="type")
    publisher = CharField()
    location = CharField()
    pages = CharField()
    ids = TextField()
    bools = TextField()
    parent = CharField()
    misc_data = TextField()

    class Meta:
        db_table = "article"
