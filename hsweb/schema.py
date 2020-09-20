from taxonomy.db.models.base import BaseModel, EnumField, ADTField
from taxonomy.db.derived_data import DerivedField
from taxonomy.adt import ADT
from typing import Type, Any, Dict, List as TList, Optional, Tuple
import base64
import enum
from functools import lru_cache
from graphene import (
    Boolean,
    Enum,
    ObjectType,
    Field,
    ID,
    Interface,
    List,
    NonNull,
    String,
    Int,
    ResolveInfo,
    Schema,
)
from graphene.relay import Node, Connection, ConnectionField
from graphene.utils.str_converters import to_snake_case
from pathlib import Path
import peewee
import re

SCALAR_FIELD_TO_GRAPHENE = {
    peewee.CharField: String,
    peewee.TextField: String,
    peewee.BooleanField: Boolean,
    peewee.IntegerField: Int,
}
TYPE_TO_GRAPHENE = {str: String, bool: Boolean, int: Int}
TYPES: TList[ObjectType] = []
CALL_SIGN_TO_MODEL = {model.call_sign: model for model in BaseModel.__subclasses__()}

DOCS_ROOT = Path(__file__).parent.parent / "docs"


class Model(Interface):
    oid = Int(required=True)
    call_sign = String(required=True)


@lru_cache()
def make_enum(python_enum: Type[enum.Enum]) -> Type[Enum]:
    return Enum.from_enum(python_enum)


def build_graphene_field_from_adt_arg(typ: Type[Any]) -> Field:
    if typ is str:
        return Field(String)
    elif typ is int:
        return Field(Int)
    elif typ is bool:
        return Field(Boolean)
    elif issubclass(typ, BaseModel):
        return Field(NonNull(lambda: build_object_type_from_model(typ)))
    elif issubclass(typ, enum.Enum):
        return Field(make_enum(typ))
    else:
        assert False, f"failed to translate {typ}"


@lru_cache()
def build_adt_member(adt_cls: Type[ADT], adt: ADT) -> Type[ObjectType]:
    namespace = {}
    for name, typ in adt._attributes.items():
        graphene_field = build_graphene_field_from_adt_arg(typ)
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


@lru_cache()
def build_adt_interface(adt_cls: Type[ADT]) -> Type[Interface]:
    # These interfaces are empty, but graphene complains if we actually leave it empty
    return type(adt_cls.__name__, (Interface,), {"__ignored": Field(ID, required=False)})


@lru_cache()
def build_adt(adt_cls: Type[ADT]) -> Type[Interface]:
    interface = build_adt_interface(adt_cls)
    for member in adt_cls._tag_to_member.values():
        TYPES.append(build_adt_member(adt_cls, member))
    return interface


def translate_adt_arg(arg: Any) -> Any:
    if isinstance(arg, BaseModel):
        return build_object_type_from_model(type(arg))(id=arg.id, oid=arg.id)
    else:
        return arg


def build_graphene_field(
    model_cls: Type[BaseModel], name: str, peewee_field: peewee.Field
) -> Field:
    if isinstance(peewee_field, EnumField):
        return Field(
            make_enum(peewee_field.enum_cls),
            required=not peewee_field.null,
            resolver=lambda parent, info: getattr(
                get_model(model_cls, parent, info), name
            ),
        )
    elif isinstance(peewee_field, peewee.ForeignKeyField):
        call_sign = getattr(model_cls, name).rel_model.call_sign

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> Optional[ObjectType]:
            model = get_model(model_cls, parent, info)
            oid = model.__data__[name]
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
                info.context["request"][key] = foreign_model
            return build_object_type_from_model(peewee_field.rel_model)(
                id=foreign_model.id, oid=foreign_model.id
            )

        return Field(
            lambda: build_object_type_from_model(peewee_field.rel_model),
            required=not peewee_field.null,
            resolver=fk_resolver,
        )
    elif isinstance(peewee_field, ADTField):
        adt_cls = peewee_field.adt_cls()

        def adt_resolver(parent: ObjectType, info: ResolveInfo) -> TList[ObjectType]:
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
                                key: translate_adt_arg(value)
                                for key, value in adt.__dict__.items()
                            }
                        )
                    )
            return out

        return List(build_adt(adt_cls), required=True, resolver=adt_resolver)
    elif type(peewee_field) in SCALAR_FIELD_TO_GRAPHENE:
        return Field(
            SCALAR_FIELD_TO_GRAPHENE[type(peewee_field)],
            required=not peewee_field.null,
            resolver=lambda parent, info: getattr(
                get_model(model_cls, parent, info), name
            ),
        )
    else:
        assert False, f"failed to translate {peewee_field}"


def get_model(model_cls: Type[BaseModel], parent: Any, info: ResolveInfo) -> BaseModel:
    cache = info.context["request"]
    key = (model_cls.call_sign, parent.oid)
    if key not in cache:
        cache[key] = model_cls.select_valid().filter(model_cls.id == parent.oid).get()
    return cache[key]


@lru_cache()
def build_connection(object_type: Type[ObjectType]) -> Type[Connection]:
    class Meta:
        node = object_type

    return type(f"{object_type.__name__}Connection", (Connection,), {"Meta": Meta})


def build_reverse_rel_count_field(
    model_cls: Type[BaseModel], name: str, peewee_field: peewee.ForeignKeyField
) -> Field:
    def resolver(parent: ObjectType, info: ResolveInfo) -> TList[ObjectType]:
        model = get_model(model_cls, parent, info)
        query = getattr(model, name)
        return query.count()

    return Int(required=True, resolver=resolver)


def build_reverse_rel_field(
    model_cls: Type[BaseModel], name: str, peewee_field: peewee.ForeignKeyField
) -> Field:
    foreign_model = peewee_field.model
    call_sign = foreign_model.call_sign

    if hasattr(foreign_model, "label_field"):
        label_field = getattr(foreign_model, foreign_model.label_field)

        def apply_ordering(query: Any) -> Any:
            return query.order_by(label_field)

    else:

        def apply_ordering(query: Any) -> Any:
            return query

    def resolver(
        parent: ObjectType,
        info: ResolveInfo,
        first: int = 10,
        after: Optional[str] = None,
    ) -> TList[ObjectType]:
        model = get_model(model_cls, parent, info)
        object_type = build_object_type_from_model(foreign_model)
        query = apply_ordering(getattr(model, name))
        if after:
            offset = int(base64.b64decode(after).split(b":")[1]) + 1
            query = query.limit(first + offset + 1)
        else:
            query = query.limit(first + 1)
        query = foreign_model.add_validity_check(query)
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


def build_derived_field(
    model_cls: Type[BaseModel], derived_field: DerivedField
) -> Field:
    field_name = derived_field.name
    if issubclass(derived_field.typ, BaseModel):

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> Optional[ObjectType]:
            model = get_model(model_cls, parent, info)
            foreign_model_oid = model.get_raw_derived_field(field_name)
            if foreign_model_oid is None:
                return None
            return build_object_type_from_model(derived_field.typ)(
                id=foreign_model_oid, oid=foreign_model_oid
            )

        return Field(
            lambda: build_object_type_from_model(derived_field.typ),
            required=False,
            resolver=fk_resolver,
        )
    elif derived_field.typ in TYPE_TO_GRAPHENE:
        return Field(
            TYPE_TO_GRAPHENE[derived_field.typ],
            required=False,
            resolver=lambda parent, info: get_model(
                model_cls, parent, info
            ).get_derived_field(field_name),
        )
    else:
        assert False, f"unimplemented for {derived_field.typ}"


@lru_cache()
def build_object_type_from_model(model_cls: Type[BaseModel]) -> Type[ObjectType]:
    namespace = {}
    for name, peewee_field in model_cls._meta.fields.items():
        if name == "id":
            continue
        namespace[name] = build_graphene_field(model_cls, name, peewee_field)

    for peewee_field in model_cls._meta.backrefs:
        namespace[peewee_field.backref] = build_reverse_rel_field(
            model_cls, peewee_field.backref, peewee_field
        )
        namespace[f"num_{peewee_field.backref}"] = build_reverse_rel_count_field(
            model_cls, peewee_field.backref, peewee_field
        )

    for derived_field in model_cls.derived_fields:
        namespace[derived_field.name] = build_derived_field(model_cls, derived_field)

    class Meta:
        interfaces = (Node, Model)

    @classmethod
    def get_node(cls: Type[ObjectType], info: ResolveInfo, id: int) -> ObjectType:
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
    namespace["get_node"] = get_node

    return type(model_cls.__name__, (ObjectType,), namespace)


def build_model_field(model_cls: Type[BaseModel]) -> Tuple[Field, Optional[Field]]:
    object_type = build_object_type_from_model(model_cls)

    def resolver(parent: ObjectType, info: ResolveInfo, oid: int) -> ObjectType:
        return object_type(oid=oid, id=oid)

    by_label_field: Optional[Field] = None
    if hasattr(model_cls, "label_field"):
        label_field = getattr(model_cls, model_cls.label_field)

        def by_label_resolver(
            parent: ObjectType, info: ResolveInfo, label: str
        ) -> TList[ObjectType]:
            objects = model_cls.select_valid().filter(label_field == label)
            return [object_type(id=obj.id, oid=obj.id) for obj in objects]

        by_label_field = List(object_type, label=String(), resolver=by_label_resolver)

    return Field(object_type, oid=Int(), resolver=resolver), by_label_field


def get_model_resolvers() -> Dict[str, Field]:
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
) -> TList[ObjectType]:
    model_cls = CALL_SIGN_TO_MODEL[call_sign.upper()]
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
) -> Optional[str]:
    if not re.match(r"^[a-zA-Z\-\d]+$", path):
        return None
    full_path = DOCS_ROOT / (path + ".md")
    if full_path.exists():
        return full_path.read_text()
    return None


def resolve_autocompletions(
    parent: "ModelCls", info: ResolveInfo, field: Optional[str] = None
) -> TList[str]:
    model_cls = BaseModel.call_sign_to_model[parent.call_sign]
    if field is None:
        field = model_cls.label_field
    return model_cls.getter(field).get_all()


class ModelCls(ObjectType):
    call_sign = String(required=True)
    name = String(
        required=True,
        resolver=lambda self, *args: BaseModel.call_sign_to_model[
            self.call_sign
        ].__name__,
    )
    autocompletions = Field(
        NonNull(List(NonNull(String))),
        field=String(required=False),
        resolver=resolve_autocompletions,
    )


class Query(ObjectType):
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
    locals().update(get_model_resolvers())


schema = Schema(query=Query, types=TYPES)


def get_schema_string() -> str:
    # Graphene has a bug in the str() of its schema, where it puts multiple interfaces
    # as "A, B" instead of "A & B". Hacky workaround (that will work only as long as
    # we have at most two interfaces).
    return re.sub(r" implements ([A-Z][a-z]+), ", r" implements \1 & ", str(schema))
