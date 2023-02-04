from collections.abc import Awaitable, Callable
from functools import lru_cache
from pathlib import Path

from aiohttp import web
from aiohttp_graphql import GraphQLView

from . import schema

HESPEROMYS_ROOT = Path("/Users/jelle/py/hesperomys")


@lru_cache
def get_static_file_contents(hesperomys_dir: Path, path: str) -> bytes:
    return (hesperomys_dir / "build" / path).read_bytes()


def make_static_handler(
    path: str, content_type: str, hesperomys_dir: Path
) -> Callable[[web.Request], Awaitable[web.Response]]:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=get_static_file_contents(hesperomys_dir, "index.html"),
            content_type=content_type,
        )

    return handler


async def on_prepare(request: web.Request, response: web.Response) -> None:
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Headers"] = "*"


def make_app(build_root: str | None = None) -> web.Application:
    if build_root is None:
        hesperomys_dir = HESPEROMYS_ROOT
    else:
        hesperomys_dir = Path(build_root)
    app = web.Application()
    GraphQLView.attach(app, schema=schema.schema, graphiql=True)
    app.router.add_static("/static", hesperomys_dir / "build" / "static")
    app.add_routes(
        [
            web.get(
                "/favicon.ico",
                make_static_handler("favicon.ico", "image/x-icon", hesperomys_dir),
            )
        ]
    )

    # Delegate everything else to React
    react_handler = make_static_handler("index.html", "text/html", hesperomys_dir)
    app.add_routes(
        [
            web.get("/{part1}/{part2}/{part3}", react_handler),
            web.get("/{part1}/{part2}", react_handler),
            web.get("/{part1}", react_handler),
            web.get("/", react_handler),
        ]
    )
    # invariance is too strict here
    app.on_response_prepare.append(on_prepare)  # type: ignore

    graphql_schema = hesperomys_dir / "hesperomys.graphql"
    graphql_schema.write_text(schema.get_schema_string(schema.schema))
    return app
