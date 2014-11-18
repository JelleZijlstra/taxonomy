import db.constants
import db.helpers
from db.models import Name, Taxon
import IPython

ns = {
	'Taxon': Taxon,
	'Name': Name,
	'constants': db.constants,
	'helpers': db.helpers,
}


def command(fn):
	ns[fn.__name__] = fn
	return fn


@command
def find_names_in(root_name, container):
	"""Find instances of the given root_name within the given container taxon."""
	candidates = Name.filter(Name.root_name == root_name)
	try:
		container = Taxon.filter(Taxon.valid_name == container)[0]
	except IndexError:
		raise KeyError('Cannot find container taxon: %s' % container)

	result = []
	# maybe I could do some internal caching here but for now this is fast enough
	for candidate in candidates:
		taxon = candidate.taxon
		while taxon.parent is not None:
			if taxon.id == container.id:
				result.append(candidate)
				break
			taxon = taxon.parent
	return result


def run_shell():
	IPython.start_ipython(user_ns=ns)


if __name__ == '__main__':
	run_shell()
