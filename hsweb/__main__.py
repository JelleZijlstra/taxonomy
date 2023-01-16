import argparse
import logging

from aiohttp import web

from . import index

if __name__ == "__main__":
    parser = argparse.ArgumentParser("hsweb")
    parser.add_argument("-p", "--port", type=int)
    parser.add_argument("-b", "--build-root", type=str)
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    if args.verbose:
        logger = logging.getLogger("peewee")
        logger.setLevel(logging.DEBUG)

    web.run_app(index.make_app(args.build_root), port=args.port)
