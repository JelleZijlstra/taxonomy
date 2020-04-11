from aiohttp import web
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from functools import partial
from taxonomy.db import models
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Optional, Tuple, Type

from .. import components as c


@dataclass
class Page(c.Component):
    request: web.Request

    async def atree(self) -> c.Nodes:
        z: c.Nodes = []
        with c.Tag("html").into(z):
            with c.Tag("head").into(z):
                z += c.Tag(
                    "link",
                    attrs={
                        "rel": "stylesheet",
                        "href": "https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css",
                        "integrity": "sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu",
                        "crossorigin": "anonymous",
                    },
                )
                z += c.Tag(
                    "link",
                    attrs={"rel": "stylesheet", "href": "/static/hesperomys.css"},
                )

            with c.Tag("body").into(z):
                with c.Div(class_="header").into(z):
                    z += await self.arender_header()
                with c.Div(class_="body").into(z):
                    z += await self.arender_body()
        return z

    async def arender_header(self) -> c.Nodes:
        z: c.Nodes = []
        with c.Div(class_="home-link").into(z):
            with c.A(href="/").into(z):
                z += c.Text("Hesperomys")

        with c.Div(class_="page-title").into(z):
            z += await self.arender_page_title()
        return z

    async def arender_page_title(self) -> c.Nodes:
        raise NotImplementedError

    async def arender_main(self) -> c.Nodes:
        raise NotImplementedError


class ModelPage(Page):
    model_cls: ClassVar[Type[models.BaseModel]]

    async def arender_page_title(self) -> c.Nodes:
        obj = self.get_obj()
        if obj is not None:
            return c.Text(repr(obj))
        else:
            return c.Text(self.request.match_info["id"])

    def get_obj(self) -> Optional[models.BaseModel]:
        oid = self.request.match_info["id"]
        if oid.isnumeric():
            return self.model_cls.get(id=oid)
        else:
            return None

    def get_fields(self) -> Iterator[Tuple[str, Callable[[Any], Optional[c.Nodes]]]]:
        for field in self.model_cls.fields():
            yield field, partial(self.render_field, field)

    def render_field(self, field: str, obj: Any) -> Optional[c.Nodes]:
        value = getattr(obj, field)
        if value is None or value == "":
            return None
        if isinstance(value, str):
            return c.Text(value)
        elif isinstance(value, Enum):
            return c.Text(value.name)
        elif isinstance(value, models.BaseModel):
            return ModelLink(value)
        else:
            return c.Text(repr(value))

    async def arender_body(self) -> c.Nodes:
        z: c.Nodes = []
        obj = self.get_obj()
        if obj is None:
            query = self.request.match_info["id"]
            objs = list(
                self.model_cls.select_valid().filter(
                    getattr(self.model_cls, self.model_cls.label_field) == query
                )
            )
            if not objs:
                # TODO should be a 404
                z += c.Text(f"No object found named {query!r}")
            elif len(objs) == 1:
                raise web.HTTPFound(
                    location=f"/{self.model_cls.call_sign.lower()}/{objs[0].id}"
                )
            else:
                with c.P().into(z):
                    z += c.Text("Choose one:")
                z += self.render_obj_list(objs)
            return z
        with c.Table(class_="table").into(z):
            for field, renderer in self.get_fields():
                rendered = renderer(obj)
                if rendered is None:
                    continue
                with c.TR().into(z):
                    with c.TD(class_="table-label").into(z):
                        z += c.Text(field)
                    with c.TD(class_="table-value").into(z):
                        z += rendered

        for relname in self.model_cls._meta.reverse_rel.keys():
            z += await self.arender_relname(relname, obj)
        return z

    async def arender_relname(self, relname: str, obj: models.BaseModel) -> c.Nodes:
        related_objs = list(getattr(obj, relname))
        if not related_objs:
            return []
        z: c.Nodes = []
        with c.H2().into(z):
            z += c.Text(relname)
        related_type = type(related_objs[0])
        if related_type.grouping_field:
            by_field: Dict[Any, List[models.BaseModel]] = defaultdict(list)
            for related_obj in related_objs:
                by_field[getattr(related_obj, related_type.grouping_field)].append(
                    related_obj
                )
            for val, objs in sorted(
                by_field.items(),
                key=lambda pair: pair[0].sort_key()
                if hasattr(pair[0], "sort_key")
                else pair[0]
                if pair[0] is not None
                else "",
            ):
                with c.H4().into(z):
                    z += c.Text(str(val))
                z += self.render_obj_list(objs)
        else:
            z += self.render_obj_list(related_objs)
        return z

    def render_obj_list(self, objs: Iterator[models.BaseModel]) -> c.Nodes:
        z: c.Nodes = []
        with c.UL().into(z):
            for obj in sorted(objs, key=lambda o: o.sort_key()):
                with c.LI().into(z):
                    z += ModelLink(obj)
        return z


@dataclass
class ModelLink(c.Component):
    obj: models.BaseModel

    async def atree(self) -> c.Nodes:
        href = f"/{self.obj.call_sign.lower()}/{self.obj.id}"
        z: c.Nodes = []
        with c.A(href=href).into(z):
            z += c.Text(repr(self.obj))
        return z
