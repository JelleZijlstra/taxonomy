from __future__ import annotations

import base64
import enum
import re
from collections.abc import Callable
from itertools import islice
from typing import TYPE_CHECKING, Any, TypeVar

import clorm
import graphene
import typing_inspect
from graphene import (
    ID,
    Boolean,
    Enum,
    Field,
    Int,
    Interface,
    List,
    Mutation,
    NonNull,
    ObjectType,
    ResolveInfo,
    Schema,
    String,
)
from graphene.relay import Connection, ConnectionField, Node
from graphene.utils.str_converters import to_snake_case

from taxonomy.adt import ADT
from taxonomy.config import get_options
from taxonomy.db.constants import CommentKind
from taxonomy.db.derived_data import DerivedField
from taxonomy.db.models import (
    Article,
    Location,
    Name,
    NameComment,
    Period,
    Person,
    Taxon,
)
from taxonomy.db.models.base import ADTField, BaseModel, TextField, TextOrNullField

from . import search
from .render import CALL_SIGN_TO_MODEL, DOCS_ROOT, render_markdown, render_plain_text

T = TypeVar("T")

if TYPE_CHECKING:

    def cache(obj: T, /) -> T:
        return obj

else:
    from functools import cache

TYPE_TO_GRAPHENE = {str: String, bool: Boolean, int: Int}
TYPES: list[ObjectType] = []


class Model(Interface):
    oid = Int(required=True)
    call_sign = String(required=True)
    page_title = String(required=True)
    redirect_url = String(required=False)
    model_cls = Field(lambda: ModelCls, required=True)


@cache
def make_enum(python_enum: type[enum.Enum]) -> type[Enum]:
    return Enum.from_enum(python_enum)


def build_graphene_field_from_adt_arg(typ: type[Any], is_required: bool) -> Field:
    if typ is str:
        graphene_type = String
    elif typ is int:
        graphene_type = Int
    elif typ is bool:
        graphene_type = Boolean
    elif issubclass(typ, BaseModel):
        graphene_type = lambda: build_object_type_from_model(typ)
    elif issubclass(typ, enum.Enum):
        graphene_type = make_enum(typ)
    else:
        assert False, f"failed to translate {typ}"
    if is_required:
        graphene_type = NonNull(graphene_type)
    return Field(graphene_type)


@cache
def build_adt_member(adt_cls: type[ADT], adt: type[ADT]) -> type[ObjectType]:
    namespace = {}
    for name, typ in adt._attributes.items():
        graphene_field = build_graphene_field_from_adt_arg(
            typ, is_required=name in adt.__required_attrs__
        )
        if graphene_field is not None:
            namespace[name] = graphene_field

    class Meta:
        interfaces = (build_adt_interface(adt_cls),)

    namespace["Meta"] = Meta

    if adt._has_args:
        name = adt.__name__
    else:
        name = type(adt).__name__

    return type(name, (ObjectType,), namespace)


@cache
def build_adt_interface(adt_cls: type[ADT]) -> type[Interface]:
    # These interfaces are empty, but graphene complains if we actually leave it empty
    return type(
        adt_cls.__name__, (Interface,), {"__ignored": Field(ID, required=False)}
    )


@cache
def build_adt(adt_cls: type[ADT]) -> type[Interface]:
    interface = build_adt_interface(adt_cls)
    for member in adt_cls._tag_to_member.values():
        TYPES.append(build_adt_member(adt_cls, member))
    return interface


def translate_adt_arg(arg: Any, attr_name: str) -> Any:
    if isinstance(arg, BaseModel):
        return build_object_type_from_model(type(arg))(id=arg.id, oid=arg.id)
    elif attr_name == "comment" and isinstance(arg, str):
        return render_markdown(arg)
    elif isinstance(arg, str):
        return render_plain_text(arg)
    else:
        return arg


def build_graphene_field(
    model_cls: type[BaseModel], name: str, clorm_field: clorm.Field
) -> Field:
    if issubclass(clorm_field.type_object, enum.Enum):
        return Field(
            make_enum(clorm_field.type_object),
            required=not clorm_field.allow_none,
            resolver=lambda parent, info: getattr(
                get_model(model_cls, parent, info), name
            ),
        )
    elif issubclass(clorm_field.type_object, BaseModel):
        call_sign = getattr(model_cls, name).type_object.call_sign

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> ObjectType | None:
            model = get_model(model_cls, parent, info)
            oid = getattr(model_cls, name).get_raw(model)
            if oid is None:
                return None
            key = (call_sign, oid)
            cache = info.context["request"]
            if key in cache:
                foreign_model = cache[key]
            else:
                foreign_model = getattr(model, name)
                if foreign_model is None:
                    return None
                cache[key] = foreign_model
            return build_object_type_from_model(clorm_field.type_object)(
                id=foreign_model.id, oid=foreign_model.id
            )

        return Field(
            lambda: build_object_type_from_model(clorm_field.type_object),
            required=not clorm_field.allow_none,
            resolver=fk_resolver,
        )
    elif isinstance(clorm_field, ADTField):
        adt_cls = clorm_field.adt_type

        def adt_resolver(parent: ObjectType, info: ResolveInfo) -> list[ObjectType]:
            model = get_model(model_cls, parent, info)
            adts = getattr(model, name)
            if not adts:
                return []
            out = []
            for adt in adts:
                if not adt._has_args:
                    graphene_cls = build_adt_member(adt_cls, adt)
                    out.append(graphene_cls())
                else:
                    graphene_cls = build_adt_member(adt_cls, type(adt))
                    out.append(
                        graphene_cls(
                            **{
                                key: translate_adt_arg(value, key)
                                for key, value in adt.__dict__.items()
                            }
                        )
                    )
            return out

        return List(NonNull(build_adt(adt_cls)), required=True, resolver=adt_resolver)
    elif (
        isinstance(clorm_field, (TextField, TextOrNullField))
        or name in model_cls.markdown_fields
    ):

        def md_resolver(parent: ObjectType, info: ResolveInfo) -> str | None:
            value = getattr(get_model(model_cls, parent, info), name)
            if value is None:
                return None
            return render_markdown(value)

        return Field(String, required=not clorm_field.allow_none, resolver=md_resolver)
    elif clorm_field.type_object is str:

        def str_resolver(parent: ObjectType, info: ResolveInfo) -> str | None:
            value = getattr(get_model(model_cls, parent, info), name)
            if value is None:
                return None
            return render_plain_text(value)

        return Field(String, required=not clorm_field.allow_none, resolver=str_resolver)
    elif clorm_field.type_object is int:
        return Field(
            Int,
            required=not clorm_field.allow_none,
            resolver=lambda parent, info: getattr(
                get_model(model_cls, parent, info), name
            ),
        )
    elif clorm_field.type_object is bool:
        return Field(
            Boolean,
            required=not clorm_field.allow_none,
            resolver=lambda parent, info: getattr(
                get_model(model_cls, parent, info), name
            ),
        )
    else:
        assert (
            False
        ), f"failed to translate {clorm_field} with type {clorm_field.type_object}"


def get_model(model_cls: type[BaseModel], parent: Any, info: ResolveInfo) -> BaseModel:
    cache = info.context["request"]
    key = (model_cls.call_sign, parent.oid)
    if key not in cache:
        obj = model_cls(parent.oid)
        if obj.is_invalid() and not obj.get_redirect_target():
            raise ValueError(f"No {model_cls} with id {obj.id}")
        cache[key] = obj
    return cache[key]


@cache
def build_connection(object_type: type[ObjectType]) -> type[Connection]:
    class Meta:
        node = object_type

    return type(f"{object_type.__name__}Connection", (Connection,), {"Meta": Meta})


def build_reverse_rel_count_field(
    model_cls: type[BaseModel], name: str, clorm_field: clorm.Field
) -> Field:
    def resolver(parent: ObjectType, info: ResolveInfo) -> list[ObjectType]:
        model = get_model(model_cls, parent, info)
        query = getattr(model, name)
        return query.count()

    return Int(required=True, resolver=resolver)


def _get_locations(
    parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
) -> Any:
    model = get_model(Period, parent, info)
    query = (
        Location.select_valid()
        .filter((Location.min_period == model) | (Location.max_period == model))
        .order_by(Location.label_field)
    )
    if after:
        offset = int(base64.b64decode(after).split(b":")[1]) + 1
        query = query.limit(first + offset + 1)
    else:
        query = query.limit(first + 1)
    return query


def locations_resolver(
    parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
) -> list[ObjectType]:
    object_type = build_object_type_from_model(Location)
    query = _get_locations(parent, info, first, after)
    cache = info.context["request"]
    ret = []
    for obj in query:
        ret.append(object_type(id=obj.id, oid=obj.id))
        cache[(Location.call_sign, obj.id)] = obj
    return ret


def _get_aliases(
    parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
) -> Any:
    model = get_model(Person, parent, info)
    query = model.get_aliases()
    if after:
        offset = int(base64.b64decode(after).split(b":")[1]) + 1
        query = query.limit(first + offset + 1)
    else:
        query = query.limit(first + 1)
    return query


def person_aliases_resolver(
    parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
) -> list[ObjectType]:
    object_type = build_object_type_from_model(Person)
    query = _get_aliases(parent, info, first, after)
    cache = info.context["request"]
    ret = []
    for obj in query:
        ret.append(object_type(id=obj.id, oid=obj.id))
        cache[(Person.call_sign, obj.id)] = obj
    return ret


def num_aliases_resolver(parent: ObjectType, info: ResolveInfo) -> list[ObjectType]:
    model = get_model(Person, parent, info)
    return model.get_aliases().count()


def _get_names_missing_field(
    parent: ObjectType,
    info: ResolveInfo,
    field: str,
    first: int = 10,
    after: str | None = None,
) -> Any:
    model = get_model(Taxon, parent, info)
    query = model.names_missing_field_lazy(field)
    if after:
        offset = int(base64.b64decode(after).split(b":")[1]) + 1
        limit = first + offset + 1
    else:
        limit = first + 1
    return islice(query, limit)


def names_missing_field_resolver(
    parent: ObjectType,
    info: ResolveInfo,
    field: str,
    first: int = 10,
    after: str | None = None,
) -> list[ObjectType]:
    object_type = build_object_type_from_model(Name)
    query = _get_names_missing_field(parent, info, field, first, after)
    cache = info.context["request"]
    ret = []
    for obj in query:
        ret.append(object_type(id=obj.id, oid=obj.id))
        cache[(Name.call_sign, obj.id)] = obj
    return ret


def num_locations_resolver(
    parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
) -> int:
    model = get_model(Period, parent, info)
    query = Location.select_valid().filter(
        (Location.min_period == model) | (Location.max_period == model)
    )
    return query.count()


def numeric_year_resolver_name(parent: ObjectType, info: ResolveInfo) -> int | None:
    model = get_model(Name, parent, info)
    assert isinstance(model, Name)
    return model.valid_numeric_year()


def numeric_year_resolver_article(parent: ObjectType, info: ResolveInfo) -> int | None:
    model = get_model(Article, parent, info)
    assert isinstance(model, Article)
    return model.valid_numeric_year()


def build_reverse_rel_field(
    model_cls: type[BaseModel], name: str, clorm_field: clorm.Field
) -> Field:
    foreign_model = clorm_field.model_cls
    call_sign = foreign_model.call_sign

    if hasattr(foreign_model, "label_field"):
        label_field = getattr(foreign_model, foreign_model.label_field)

        def apply_ordering(query: Any) -> Any:
            return query.order_by(label_field)

    else:

        def apply_ordering(query: Any) -> Any:
            return query

    def resolver(
        parent: ObjectType, info: ResolveInfo, first: int = 10, after: str | None = None
    ) -> list[ObjectType]:
        model = get_model(model_cls, parent, info)
        object_type = build_object_type_from_model(foreign_model)
        query = apply_ordering(getattr(model, name))
        query = query.limit(first + _decode_after(after) + 1)
        query = foreign_model.add_validity_check(query)
        if foreign_model is NameComment:
            query = query.filter(
                NameComment.kind.is_not_in(
                    (CommentKind.structured_quote, CommentKind.automatic_change)
                )
            )
        cache = info.context["request"]
        ret = []
        for obj in query:
            ret.append(object_type(id=obj.id, oid=obj.id))
            cache[(call_sign, obj.id)] = obj
        return ret

    return ConnectionField(
        lambda: build_connection(build_object_type_from_model(foreign_model)),
        resolver=resolver,
    )


def _decode_after(after: str | None) -> int:
    if after:
        return int(base64.b64decode(after).split(b":")[1]) + 1
    else:
        return 0


def make_connection(model_cls: type[BaseModel]) -> Callable[[], type[Connection]]:
    return lambda: build_connection(build_object_type_from_model(model_cls))


CUSTOM_FIELDS = {
    Period: {
        "locations": ConnectionField(
            make_connection(Location), resolver=locations_resolver
        ),
        "num_locations": Int(required=True, resolver=num_locations_resolver),
    },
    Name: {"numeric_year": Int(required=False, resolver=numeric_year_resolver_name)},
    Article: {
        "numeric_year": Int(required=False, resolver=numeric_year_resolver_article)
    },
    Person: {
        "aliases": ConnectionField(
            make_connection(Person), resolver=person_aliases_resolver
        ),
        "num_aliases": Int(required=True, resolver=num_aliases_resolver),
    },
    Taxon: {
        "names_missing_field": ConnectionField(
            make_connection(Name),
            resolver=names_missing_field_resolver,
            field=graphene.Argument(String, required=True),
        )
    },
}


def build_derived_field(
    model_cls: type[BaseModel], derived_field: DerivedField[Any]
) -> Field:
    field_name = derived_field.name
    typ = derived_field.get_type()
    if isinstance(typ, type) and issubclass(typ, BaseModel):

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> ObjectType | None:
            model = get_model(model_cls, parent, info)
            foreign_model_oid = model.get_raw_derived_field(field_name)
            if foreign_model_oid is None:
                return None
            return build_object_type_from_model(typ)(
                id=foreign_model_oid, oid=foreign_model_oid
            )

        return Field(
            lambda: build_object_type_from_model(typ),
            required=False,
            resolver=fk_resolver,
        )
    elif isinstance(typ, type) and issubclass(typ, enum.Enum):
        return Field(
            make_enum(typ),
            required=False,
            resolver=lambda parent, info: get_model(
                model_cls, parent, info
            ).get_derived_field(field_name),
        )
    elif isinstance(typ, type) and typ in TYPE_TO_GRAPHENE:
        return Field(
            TYPE_TO_GRAPHENE[typ],
            required=False,
            resolver=lambda parent, info: get_model(
                model_cls, parent, info
            ).get_derived_field(field_name),
        )
    elif typing_inspect.is_generic_type(typ) and typing_inspect.get_origin(typ) is list:
        (arg_type,) = typing_inspect.get_args(typ)
        if issubclass(arg_type, BaseModel):

            def elt_type() -> type[Connection]:
                return build_connection(build_object_type_from_model(arg_type))

            def list_resolver(
                parent: ObjectType,
                info: ResolveInfo,
                first: int = 10,
                after: str | None = None,
            ) -> Any:
                model = get_model(model_cls, parent, info)
                foreign_model_oids = model.get_raw_derived_field(field_name)
                if foreign_model_oids is None:
                    return []
                object_type = build_object_type_from_model(arg_type)
                return [object_type(id=oid, oid=oid) for oid in foreign_model_oids]

        elif typ in TYPE_TO_GRAPHENE:
            elt_type = build_connection(TYPE_TO_GRAPHENE[arg_type])

            def list_resolver(
                parent: ObjectType,
                info: ResolveInfo,
                first: int = 10,
                after: str | None = None,
            ) -> Any:
                model = get_model(model_cls, parent, info)
                return model.get_derived_field(field_name)

        else:
            assert False, f"unimplemented for {arg_type}"
        return ConnectionField(elt_type, resolver=list_resolver)
    else:
        assert False, f"unimplemented for {typ}"


def build_derived_count_field(
    model_cls: type[BaseModel], derived_field: DerivedField[Any]
) -> Field | None:
    field_name = derived_field.name
    typ = derived_field.get_type()
    if typing_inspect.is_generic_type(typ) and typing_inspect.get_origin(typ) is list:

        def resolver(parent: ObjectType, info: ResolveInfo) -> int:
            model = get_model(model_cls, parent, info)
            value = model.get_raw_derived_field(field_name)
            if value is None:
                return 0
            return len(value)

        return Field(Int, required=True, resolver=resolver)

    return None


@cache
def build_object_type_from_model(model_cls: type[BaseModel]) -> type[ObjectType]:
    namespace = {}
    for name, clorm_field in model_cls.clorm_fields.items():
        if name == "id":
            continue
        namespace[name] = build_graphene_field(model_cls, name, clorm_field)

    for clorm_field in model_cls.clorm_backrefs:
        namespace[clorm_field.related_name] = build_reverse_rel_field(
            model_cls, clorm_field.related_name, clorm_field
        )
        namespace[f"num_{clorm_field.related_name}"] = build_reverse_rel_count_field(
            model_cls, clorm_field.related_name, clorm_field
        )

    for derived_field in model_cls.derived_fields:
        namespace[derived_field.name] = build_derived_field(model_cls, derived_field)
        count_field = build_derived_count_field(model_cls, derived_field)
        if count_field is not None:
            namespace[f"num_{derived_field.name}"] = count_field

    namespace.update(CUSTOM_FIELDS.get(model_cls, {}))

    class Meta:
        interfaces = (Node, Model)

    def get_node(cls: type[ObjectType], info: ResolveInfo, id: int) -> ObjectType:
        return cls(oid=id, id=id)

    namespace["Meta"] = Meta
    namespace["oid"] = Field(Int, required=True)
    namespace["model_cls"] = Field(
        lambda: ModelCls,
        required=True,
        resolver=lambda *args: ModelCls(call_sign=model_cls.call_sign),
    )
    namespace["call_sign"] = Field(
        String, required=True, resolver=lambda *args: model_cls.call_sign
    )
    namespace["get_node"] = classmethod(get_node)

    def page_title_resolver(parent: ObjectType, info: ResolveInfo) -> str:
        model = get_model(model_cls, parent, info)
        return model.get_page_title()

    namespace["page_title"] = Field(String, required=True, resolver=page_title_resolver)

    def redirect_url_resolver(parent: ObjectType, info: ResolveInfo) -> str | None:
        model = get_model(model_cls, parent, info)
        target = model.get_redirect_target()
        if target is None:
            return None
        return target.get_url()

    namespace["redirect_url"] = Field(
        String, required=False, resolver=redirect_url_resolver
    )

    return type(model_cls.__name__, (ObjectType,), namespace)


def build_model_field(model_cls: type[BaseModel]) -> tuple[Field, Field | None]:
    object_type = build_object_type_from_model(model_cls)

    def resolver(parent: ObjectType, info: ResolveInfo, oid: int) -> ObjectType:
        return object_type(oid=oid, id=oid)

    by_label_field: Field | None = None
    if hasattr(model_cls, "label_field"):
        label_field = getattr(model_cls, model_cls.label_field)

        def by_label_resolver(
            parent: ObjectType, info: ResolveInfo, label: str
        ) -> list[ObjectType]:
            objects = model_cls.select_valid().filter(label_field == label)
            return [object_type(id=obj.id, oid=obj.id) for obj in objects]

        by_label_field = List(object_type, label=String(), resolver=by_label_resolver)

    return Field(object_type, oid=Int(), resolver=resolver), by_label_field


def get_model_resolvers() -> dict[str, Field]:
    resolvers = {}
    for model_cls in BaseModel.__subclasses__():
        field, by_label_field = build_model_field(model_cls)
        snake_name = to_snake_case(model_cls.__name__)
        resolvers[snake_name] = field
        if by_label_field is not None:
            resolvers[f"{snake_name}_by_label"] = by_label_field

    return resolvers


def resolve_by_call_sign(
    parent: ObjectType, info: ResolveInfo, call_sign: str, oid: str
) -> list[ObjectType]:
    model_cls = get_by_call_sign(call_sign)
    object_type = build_object_type_from_model(model_cls)
    if oid.isnumeric():
        return [object_type(oid=int(oid), id=int(oid))]
    else:
        if not model_cls.label_field_has_underscores:
            oid = oid.replace("_", " ")
        objs = model_cls.select_valid().filter(
            getattr(model_cls, model_cls.label_field) == oid
        )
        return [object_type(id=obj.id, oid=obj.id) for obj in objs]


def resolve_documentation(
    parent: ObjectType, info: ResolveInfo, path: str
) -> str | None:
    if not re.match(r"^[a-zA-Z\-\d]+(/[a-zA-Z\-\d]+)?$", path):
        return None
    full_path = DOCS_ROOT / (path + ".md")
    if full_path.exists():
        return render_markdown(full_path.read_text())
    return None


def resolve_autocompletions(
    parent: ModelCls, info: ResolveInfo, field: str | None = None
) -> list[str]:
    model_cls = get_by_call_sign(parent.call_sign)
    if field is None:
        field = model_cls.label_field
    return model_cls.getter(field).get_all()


def get_by_call_sign(call_sign: str) -> type[BaseModel]:
    return CALL_SIGN_TO_MODEL[call_sign.upper()]


class ModelConnection(Connection):
    class Meta:
        node = Model


def resolve_newest(
    parent: ModelCls, info: ResolveInfo, first: int = 10, after: str | None = None
) -> list[Model]:
    model_cls = get_by_call_sign(parent.call_sign)
    object_type = build_object_type_from_model(model_cls)
    query = model_cls.select_valid().order_by(model_cls.id.desc())  # type: ignore[attr-defined]
    query = query.limit(first + _decode_after(after) + 1)
    ret = []
    cache = info.context["request"]
    for obj in query:
        ret.append(object_type(id=obj.id, oid=obj.id))
        cache[(model_cls.call_sign, obj.id)] = obj
    return ret


class ModelCls(ObjectType):
    call_sign = String(required=True)
    name = String(
        required=True,
        resolver=lambda self, *args: get_by_call_sign(self.call_sign).__name__,
    )
    autocompletions = Field(
        NonNull(List(NonNull(String))),
        field=String(required=False),
        resolver=resolve_autocompletions,
    )
    newest = ConnectionField(
        NonNull(ModelConnection),
        first=Int(required=False),
        after=String(required=False),
        resolver=resolve_newest,
    )


class SearchResult(ObjectType):
    model = Field(Model)
    context = String(required=False)
    highlight = String(required=False)

    @classmethod
    def from_hit(cls, hit: dict[str, Any]) -> SearchResult:
        document_id = hit["id"]
        pieces = document_id.split("/")
        if len(pieces) == 3:
            call_sign, oid, page = pieces
            context = f"Page {page}"
        else:
            call_sign, oid = pieces
            context = None
        model_cls = get_by_call_sign(call_sign)
        object_type = build_object_type_from_model(model_cls)
        model = object_type(oid=int(oid), id=int(oid))
        highlights = [value for value in hit["highlights"].values() if "**" in value]
        return SearchResult(
            model=model,
            context=context,
            highlight=" .. ".join(highlights) if highlights else None,
        )


class SearchResultConnection(Connection):
    class Meta:
        node = SearchResult


def resolve_search(
    parent: ObjectType,
    info: ResolveInfo,
    query: str,
    first: int = 10,
    after: str | None = None,
) -> list[SearchResult | None]:
    if after is not None:
        offset = int(base64.b64decode(after).split(b":")[1]) + 1
    else:
        offset = 0
    # + 1 so Relay can know whether there are additional results
    result = search.run_query(query, size=first + offset + 1, start=offset)
    return [None] * offset + [SearchResult.from_hit(hit) for hit in result["hit"]]


class QueryRoot(ObjectType):
    node = Node.Field()
    by_call_sign = List(
        Model,
        call_sign=String(required=True),
        oid=String(required=True),
        resolver=resolve_by_call_sign,
    )
    documentation = String(
        required=False, path=String(required=True), resolver=resolve_documentation
    )
    model_cls = Field(
        ModelCls,
        call_sign=String(required=True),
        required=True,
        resolver=lambda self, info, call_sign: ModelCls(call_sign=call_sign),
    )
    search = ConnectionField(
        SearchResultConnection, query=String(required=True), resolver=resolve_search
    )
    locals().update(get_model_resolvers())


@cache
def has_library() -> bool:
    options = get_options()
    return options.library_path.exists()


class OpenArticle(Mutation):
    class Arguments:
        article_id = Int(required=True)

    ok = Boolean()

    def mutate(self, info: ResolveInfo, article_id: int) -> OpenArticle:
        if not has_library():
            return OpenArticle(ok=False)
        try:
            art = Article.get(id=article_id)
        except Article.DoesNotExist:
            return OpenArticle(ok=False)
        art.openf()
        return OpenArticle(ok=True)


class MutationRoot(ObjectType):
    open_article = OpenArticle.Field()


schema = Schema(query=QueryRoot, mutation=MutationRoot, types=TYPES)


def get_schema_string(schema: Schema) -> str:
    # Graphene has a bug in the str() of its schema, where it puts multiple interfaces
    # as "A, B" instead of "A & B". Hacky workaround (that will work only as long as
    # we have at most two interfaces).
    return re.sub(r" implements ([A-Z][a-z]+), ", r" implements \1 & ", str(schema))
