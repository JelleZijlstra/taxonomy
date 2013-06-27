from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response

import db.api

if __name__ == '__main__':
    config = Configurator()
    config.add_static_view(name='public', path='public/')
    config.add_route('api', '/api/{action}')
    config.add_view(db.api.api, route_name='api')
    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', 8080, app)
    server.serve_forever()
