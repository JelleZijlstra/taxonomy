import pyramid.security
from pyramid.httpexceptions import HTTPFound
from pyramid.response import Response

import db.settings

def login(request):
	if request.method == 'POST':
		passwd = request.params['password']
		if passwd == db.settings.passwd:
			headers = pyramid.security.remember(request, 'root')
			return HTTPFound(headers=headers, location='/public/view.html')
		else:
			return Response("Fail")
	else:
		return Response(open('login.html', 'r').read())
