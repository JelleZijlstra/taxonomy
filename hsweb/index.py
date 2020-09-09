from aiohttp import web
from functools import lru_cache
from pathlib import Path
import taxonomy
from typing import Callable, Iterator, Type
from aiohttp_graphql import GraphQLView
import logging

from . import components
from . import view
from . import schema

HSWEB_ROOT = Path(view.__file__).parent.parent
HESPEROMYS_ROOT = Path("/Users/jelle/py/hesperomys")

logger = logging.getLogger("peewee")
logger.setLevel(logging.DEBUG)


@lru_cache()
def get_static_file_contents(path: str) -> bytes:
    return (HESPEROMYS_ROOT / "build" / path).read_bytes()


def make_handler(
    root_component: Type[components.Node],
) -> Callable[[web.Request], web.Response]:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=await root_component(request).arender(),
            content_type="text/html",
            charset="utf-8",
        )

    return handler


def make_static_handler(
    path: str, content_type: str
) -> Callable[[web.Request], web.Response]:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=get_static_file_contents("index.html"), content_type=content_type
        )

    return handler


react_handler = make_static_handler("index.html", "text/html")


def get_model_routes() -> Iterator[str]:
    seen_models = set()
    for page in view.page.ModelPage.__subclasses__():
        yield web.get(f"/{page.model_cls.call_sign.lower()}/{{id}}", make_handler(page))
        seen_models.add(page.model_cls)
    for model_cls in taxonomy.db.models.BaseModel.__subclasses__():
        if model_cls not in seen_models:
            page_cls = type(
                f"{model_cls.__name__}Page",
                (view.page.ModelPage,),
                {"model_cls": model_cls},
            )
            yield web.get(
                f"/{model_cls.call_sign.lower()}/{{id}}", make_handler(page_cls)
            )


async def on_prepare(request: web.Request, response: web.Response) -> None:
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Headers"] = "*"


def make_app() -> web.Application:
    app = web.Application()
    GraphQLView.attach(app, schema=schema.schema, graphiql=True)
    app.router.add_static("/static", HESPEROMYS_ROOT / "build" / "static")
    app.add_routes(
        [web.get("/favicon.ico", make_static_handler("favicon.ico", "image/x-icon"))]
    )

    # Delegate everything else to React
    app.add_routes(
        [
            web.get("/{part1}/{part2}/{part3}", react_handler),
            web.get("/{part1}/{part2}", react_handler),
            web.get("/{part1}", react_handler),
            web.get("/", react_handler),
        ]
    )
    app.on_response_prepare.append(on_prepare)

    graphql_schema = HESPEROMYS_ROOT / "hesperomys.graphql"
    graphql_schema.write_text(schema.get_schema_string())
    return app
