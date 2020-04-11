from .. import components as c
from .page import ModelPage

from taxonomy.db import models


class NamePage(ModelPage):
    model_cls = models.Name


class TaxonPage(ModelPage):
    model_cls = models.Taxon


class ArticlePage(ModelPage):
    model_cls = models.Article


class CollectionPage(ModelPage):
    model_cls = models.Collection
