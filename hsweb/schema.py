from taxonomy.db.models.base import BaseModel, EnumField, ADTField
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
    String,
    Int,
    ResolveInfo,
    Schema,
)
from graphene.relay import Node, Connection, ConnectionField
from graphene.utils.str_converters import to_snake_case
import peewee

SCALAR_FIELD_TO_GRAPHENE = {
    peewee.CharField: String,
    peewee.TextField: String,
    peewee.BooleanField: Boolean,
    peewee.IntegerField: Int,
}
TYPES: TList[ObjectType] = []


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
        return Field(lambda: build_object_type_from_model(typ))
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
    return type(adt_cls.__name__, (Interface,), {"__ignored": Field(ID, required=True)})


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
            resolver=lambda parent, info: getattr(get_model(model_cls, parent), name),
        )
    elif isinstance(peewee_field, peewee.ForeignKeyField):

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> ObjectType:
            model = get_model(model_cls, parent)
            foreign_model = getattr(model, name)
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

        def fk_resolver(parent: ObjectType, info: ResolveInfo) -> TList[ObjectType]:
            model = get_model(model_cls, parent)
            adts = getattr(model, name)
            if not adts:
                return []
            out = []
            for adt in adts:
                graphene_cls = build_adt_member(adt_cls, type(adt))
                if not adt._has_args:
                    out.append(graphene_cls())
                else:
                    out.append(
                        graphene_cls(
                            **{
                                key: translate_adt_arg(value)
                                for key, value in adt.__dict__.items()
                            }
                        )
                    )
            return out

        return List(build_adt(adt_cls), required=True, resolver=fk_resolver)
    elif type(peewee_field) in SCALAR_FIELD_TO_GRAPHENE:
        return Field(
            SCALAR_FIELD_TO_GRAPHENE[type(peewee_field)],
            required=not peewee_field.null,
            resolver=lambda parent, info: getattr(get_model(model_cls, parent), name),
        )
    else:
        assert False, f"failed to translate {peewee_field}"


def get_model(model_cls: Type[BaseModel], parent: Any) -> BaseModel:
    return model_cls.select_valid().filter(model_cls.id == parent.oid).get()


@lru_cache()
def build_connection(object_type: Type[ObjectType]) -> Type[Connection]:
    class Meta:
        node = object_type

    return type(f"{object_type.__name__}Connection", (Connection,), {"Meta": Meta})


def build_reverse_rel_field(
    model_cls: Type[BaseModel], name: str, peewee_field: peewee.ForeignKeyField
) -> Field:
    def resolver(
        parent: ObjectType,
        info: ResolveInfo,
        first: int = 10,
        after: Optional[str] = None,
    ) -> TList[ObjectType]:
        model = get_model(model_cls, parent)
        object_type = build_object_type_from_model(peewee_field.rel_model)
        query = getattr(model, name)
        if after:
            offset = int(base64.b64decode(after).split(b":")[1]) + 1
            query = query.limit(first + offset).offset(offset)
        else:
            query = query.limit(first)
        return [object_type(id=obj.id, oid=obj.id) for obj in query]

    return ConnectionField(
        lambda: build_connection(build_object_type_from_model(peewee_field.rel_model)),
        resolver=resolver,
    )


@lru_cache()
def build_object_type_from_model(model_cls: Type[BaseModel]) -> Type[ObjectType]:
    namespace = {}
    for name, peewee_field in model_cls._meta.fields.items():
        if name == "id":
            continue
        namespace[name] = build_graphene_field(model_cls, name, peewee_field)

    for name, peewee_field in model_cls._meta.reverse_rel.items():
        namespace[name] = build_reverse_rel_field(model_cls, name, peewee_field)

    class Meta:
        interfaces = (Node,)

    @classmethod
    def get_node(cls: Type[ObjectType], info: ResolveInfo, id: int) -> ObjectType:
        return cls(oid=id, id=id)

    namespace["Meta"] = Meta
    namespace["oid"] = Field(Int, required=True)
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


class Query(ObjectType):
    node = Node.Field()
    locals().update(get_model_resolvers())


schema = Schema(query=Query, types=TYPES)
