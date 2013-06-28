from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy

import db.api
import db.settings
import login

if __name__ == '__main__':
	authn_policy = AuthTktAuthenticationPolicy(db.settings.secret, hashalg='sha512')
	authz_policy = ACLAuthorizationPolicy()
	config = Configurator()

	# authentication
	config.set_authentication_policy(authn_policy)
	config.set_authorization_policy(authz_policy)

	# add views
	config.add_static_view(name='public', path='public/')
	config.add_route('api', '/api/{action}')
	config.add_view(db.api.api, route_name='api')

	config.add_route('login', '/login')
	config.add_view(login.login, route_name='login')

	# run server
	app = config.make_wsgi_app()
	server = make_server('0.0.0.0', 8080, app)
	server.serve_forever()
