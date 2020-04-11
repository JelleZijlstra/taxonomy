from aiohttp import web
import argparse

from . import index


if __name__ == "__main__":
    parser = argparse.ArgumentParser("hsweb")
    parser.add_argument("-p", "--port", type=int)
    args = parser.parse_args()
    web.run_app(index.make_app(), port=args.port)
