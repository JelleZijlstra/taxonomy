import gzip
import os
import re
from collections.abc import Awaitable, Callable
from functools import lru_cache
from pathlib import Path

from aiohttp import web
from aiohttp_graphql import GraphQLView

from . import schema

HESPEROMYS_ROOT = Path("/Users/jelle/py/hesperomys")
GAME_DATA_DIR = Path(__file__).parent / "game_data"
ROOT_BUILD_ASSETS = ("favicon.ico", "logo192.png", "logo512.png", "manifest.json")
COMPRESSIBLE_SUFFIXES = {".css", ".html", ".js", ".json", ".svg", ".txt", ".xml"}
HASHED_ASSET_PATTERN = re.compile(r"\.[0-9a-f]{8,}\.")
IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable"
GAME_DATA_CACHE_CONTROL = "public, max-age=86400"
ROBOTS_TXT = """\
User-agent: Baiduspider
Disallow: /

User-agent: PetalBot
Disallow: /

User-agent: Applebot
Disallow: /

User-agent: *
Disallow: /graphql
Crawl-delay: 10
"""


@lru_cache
def get_static_file_contents(parent_dir: Path, path: str) -> bytes:
    return (parent_dir / path).read_bytes()


def make_static_handler(
    path: str, content_type: str, hesperomys_dir: Path
) -> Callable[[web.Request], Awaitable[web.Response]]:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=get_static_file_contents(hesperomys_dir / "build", "index.html"),
            content_type=content_type,
        )

    return handler


def make_build_asset_handler(
    path: str, hesperomys_dir: Path
) -> Callable[[web.Request], Awaitable[web.FileResponse]]:
    async def handler(request: web.Request) -> web.FileResponse:
        return web.FileResponse(hesperomys_dir / "build" / path)

    return handler


async def robots_handler(request: web.Request) -> web.Response:
    return web.Response(text=ROBOTS_TXT, content_type="text/plain")


async def on_prepare(request: web.Request, response: web.Response) -> None:
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Headers"] = "*"


def prepare_compressed_static_files(root: Path) -> None:
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in COMPRESSIBLE_SUFFIXES:
            continue
        compressed_path = path.with_suffix(f"{path.suffix}.gz")
        if (
            compressed_path.exists()
            and compressed_path.stat().st_mtime_ns >= path.stat().st_mtime_ns
        ):
            continue

        temporary_path = compressed_path.with_name(
            f".{compressed_path.name}.{os.getpid()}.tmp"
        )
        try:
            with (
                path.open("rb") as source,
                temporary_path.open("wb") as destination,
                gzip.GzipFile(
                    filename="", mode="wb", fileobj=destination, mtime=0
                ) as compressed,
            ):
                while chunk := source.read(1024 * 1024):
                    compressed.write(chunk)
            temporary_path.replace(compressed_path)
        finally:
            temporary_path.unlink(missing_ok=True)


@web.middleware
async def response_policy_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    if request.path.endswith(".map"):
        raise web.HTTPNotFound

    response = await handler(request)
    if (
        request.path == "/graphql"
        and isinstance(response, web.Response)
        and response.body is not None
    ):
        response.enable_compression()
    elif request.path.startswith("/static/"):
        if HASHED_ASSET_PATTERN.search(request.path):
            response.headers["Cache-Control"] = IMMUTABLE_CACHE_CONTROL
        else:
            response.headers["Cache-Control"] = GAME_DATA_CACHE_CONTROL
    elif request.path.startswith("/games/data/"):
        response.headers["Cache-Control"] = GAME_DATA_CACHE_CONTROL
    elif request.path.removeprefix("/") in ROOT_BUILD_ASSETS:
        response.headers["Cache-Control"] = GAME_DATA_CACHE_CONTROL
    return response


def make_app(build_root: str | None = None) -> web.Application:
    if build_root is None:
        hesperomys_dir = HESPEROMYS_ROOT
    else:
        hesperomys_dir = Path(build_root)
    prepare_compressed_static_files(hesperomys_dir / "build" / "static")
    prepare_compressed_static_files(GAME_DATA_DIR)
    app = web.Application(middlewares=[response_policy_middleware])
    # Validate schema consistency for frontend queries before serving
    schema.validate_no_conflicting_model_fields(schema.schema)
    GraphQLView.attach(app, schema=schema.schema, graphiql=True)
    app.router.add_static("/static", hesperomys_dir / "build" / "static")
    # Serve pre-generated game data files
    app.router.add_static("/games/data", GAME_DATA_DIR)
    for path in ROOT_BUILD_ASSETS:
        app.router.add_get(f"/{path}", make_build_asset_handler(path, hesperomys_dir))
    app.router.add_get("/robots.txt", robots_handler)

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
    app.on_response_prepare.append(on_prepare)  # type: ignore[arg-type]

    graphql_schema = hesperomys_dir / "hesperomys.graphql"
    graphql_schema.write_text(schema.get_schema_string(schema.schema))
    return app
