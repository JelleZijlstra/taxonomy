from aiohttp import web
from pathlib import Path
import taxonomy
from typing import Callable, Iterator, Type

from . import components
from . import view


def make_handler(
    root_component: Type[components.Node]
) -> Callable[[web.Request], web.Response]:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            body=await root_component(request).arender(), content_type="text/html",
            charset="utf-8",
        )

    return handler


def get_model_routes() -> Iterator[str]:
    seen_models = set()
    for page in view.page.ModelPage.__subclasses__():
        yield web.get(f"/{page.model_cls.call_sign.lower()}/{{id}}", make_handler(page))
        seen_models.add(page.model_cls)
    for model_cls in taxonomy.db.models.BaseModel.__subclasses__():
        if model_cls not in seen_models:
            page_cls = type(f"{model_cls.__name__}Page", (view.page.ModelPage,), {"model_cls": model_cls})
            yield web.get(f"/{model_cls.call_sign.lower()}/{{id}}", make_handler(page_cls))


def make_app() -> web.Application:
    app = web.Application()
    app.add_routes([web.get("/", make_handler(view.page.HomePage))])
    app.add_routes(get_model_routes())
    app.router.add_static("/static", Path(view.__file__).parent.parent / "static")
    return app
