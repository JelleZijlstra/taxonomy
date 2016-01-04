'''Maintenance scripts'''

from db.ehphp import call_ehphp
from db.models import Name, Taxon
from db.constants import *

import collections
import re
import sys

def cite_exists(cite):
	return call_ehphp('exists', {'0': cite})

def may_be_citation(cite):
	'''Checks whether a citation may be a catalog ID'''
	return '.' not in cite or re.search(r"\.[a-z]+$", cite)

def must_be_citation(cite):
	return re.search(r"\.[a-z]+$", cite)

def check_refs():
	for name in Name.select():
		# if there is an original_citation, check whether it is valid
		if name.original_citation:
			if not cite_exists(name.original_citation):
				print("Name:", name.description())
				print("Warning: invalid original citation:", name.original_citation)
		elif name.verbatim_citation and may_be_citation(name.verbatim_citation):
			if cite_exists(name.verbatim_citation):
				name.original_citation = name.verbatim_citation
				name.verbatim_citation = None
				name.save()
			elif must_be_citation(name.verbatim_citation):
				print("Name:", name.description())
				print("Warning: invalid citation:", name.verbatim_citation)

scripts = {
	'check_refs': check_refs,
}

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print(sys.argv[0] + ": error: no argument given")
	script = scripts[sys.argv[1]]
	script()
