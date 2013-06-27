from pyramid.response import Response
import json

import helpers
import models

def serve_error(err):
	return Response(json.dumps({"status": "error", "message": err}))

def serve_ok(contents):
	return Response(json.dumps({"status": "ok", "response": contents}))

def api(request):
	action = request.matchdict['action']
	if action == 'view':
		try:
			taxon = request.params['taxon']
		except KeyError:
			return serve_error("Required parameter not given: taxon")
		try:
			taxon_obj = models.Taxon.filter(models.Taxon.valid_name == taxon)[0]
		except KeyError:
			return serve_error("Unrecognized taxon: " + taxon)
		return serve_ok(helpers.tree_of_taxon(taxon_obj, include_root=True))
	else:
		return serve_error("Unrecognized action " + action)
