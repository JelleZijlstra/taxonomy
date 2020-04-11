from aiohttp import web

from . import index


if __name__ == "__main__":
    web.run_app(index.make_app())
