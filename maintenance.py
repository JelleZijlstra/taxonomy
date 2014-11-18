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
				print "Name:", name.description()
				print "Warning: invalid original citation:", name.original_citation
		elif name.verbatim_citation and may_be_citation(name.verbatim_citation):
			if cite_exists(name.verbatim_citation):
				name.original_citation = name.verbatim_citation
				name.verbatim_citation = None
				name.save()
			elif must_be_citation(name.verbatim_citation):
				print "Name:", name.description()
				print "Warning: invalid citation:", name.verbatim_citation

def dup_taxa():
	counts = collections.defaultdict(int)
	for txn in Taxon.select():
		if txn.rank == SUBGENUS and counts[txn.valid_name] > 0:
			continue
		counts[txn.valid_name] += 1
	for name in counts:
		if counts[name] > 1:
			print "Duplicate:", name, counts[name]

def dup_genus():
	counts = collections.defaultdict(int)
	for name in Name.select().where(Name.group == GROUP_GENUS):
		full_name = "%s %s, %s" % (name.root_name, name.authority, name.year)
		counts[full_name] += 1
	for full_name in counts:
		if counts[full_name] > 1:
			print "Duplicate:", full_name, counts[full_name]

def should_have_original():
	for name in Name.select():
		if name.original_citation and not name.original_name:
			print "Should have original name:", name.description()

scripts = {
	'check_refs': check_refs,
	'dup_taxa': dup_taxa,
	'dup_genus': dup_genus,
	'should_have_original': should_have_original,
}

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print argv[0] + ": error: no argument given"
	script = scripts[sys.argv[1]]
	script()
