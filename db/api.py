from pyramid.response import Response
import pyramid.security
import json

from constants import *
import helpers
import models

class ApiError(Exception):
	pass

def serve_error(err):
	return Response(json.dumps({"status": "error", "message": err}))

def serve_ok(contents):
	return Response(json.dumps({"status": "ok", "response": contents}))

def get_para(dict, name):
	try:
		return dict[name]
	except KeyError:
		raise ApiError("Required parameter not provided: " + name)

def perform_edit(edit):
	kind = get_para(edit, 'kind')
	data = get_para(edit, 'data')
	if kind == 'create_pair':
		# create a taxon-name pair
		txn = models.Taxon.create(valid_name=data['valid_name'],
			rank=data['rank'], parent=data['parent'], age=data['age'])
		nm = models.Name.create(status=STATUS_VALID, taxon=txn,
			root_name=data['root_name'], group=data['group'])
		return [{
			'kind': 'create_pair',
			'valid_name': data['valid_name'],
			'taxon': helpers.tree_of_taxon(txn)
		}]
	table = get_para(edit, 'table')
	if table == 'taxon':
		model = models.Taxon
	elif table == 'name':
		model = models.Name
	else:
		raise ApiError("Unrecognized table: " + table)
	if kind == 'update':
		id = get_para(edit, 'id')
		try:
			obj = model.filter(model.id == id)[0]
		except IndexError:
			raise ApiError("Invalid id: " + str(id))
		for key in data:
			# I suppose some validation would be useful here
			setattr(obj, key, data[key])
		obj.save()
		return []
	elif kind == 'create':
		mdl = model.create(**data)
		if table == 'taxon':
			data = {'kind': 'create', 'valid_name': data['valid_name'], 'taxon': helpers.tree_of_taxon(mdl) }
		else:
			data = {'kind': 'create', 'root_name': data['root_name'], 'name': helpers.dict_of_name(mdl) }
		return [data]
	else:
		raise ApiError("Invalid kind: " + kind)

def api(request):
	# Very primitive authorization, but can't be bothered to figure out how
	# the full Pyramid system works.
	if pyramid.security.authenticated_userid(request) == None:
		return serve_error("Not logged in")
	action = request.matchdict['action']
	if action == 'view':
		try:
			taxon = request.params['taxon']
		except KeyError:
			return serve_error("Required parameter not given: taxon")
		try:
			taxon_obj = models.Taxon.filter(models.Taxon.valid_name == taxon)[0]
		except IndexError:
			return serve_error("Unrecognized taxon: " + taxon)
		return serve_ok(helpers.tree_of_taxon(taxon_obj, include_root=True))
	elif action == 'find_taxon':
		try:
			valid_name = request.params['valid_name']
		except KeyError:
			return serve_error("Required parameter not provided: valid_name")
		results = models.Taxon.filter(models.Taxon.valid_name == valid_name)
		objs = [helpers.dict_of_taxon(txn) for txn in results]
		return serve_ok(objs)
	elif action == 'edit':
		try:
			changes = json.loads(request.params['changes'])
		except:
			return serve_error("Required parameter not given or invalid JSON: changes")
		returns = []
		for change in changes:
			try:
				returns += perform_edit(change)
			except ApiError, e:
				return serve_error(str(e))
		return serve_ok(returns)
	else:
		return serve_error("Unrecognized action " + action)
