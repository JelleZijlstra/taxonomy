import export_tools

from db.constants import *
import db.helpers
from db.models import Name, Taxon

import argparse
import collections
import gc
import subprocess

'''
Layout of a row:
0 - rank/status (t/n)
1 - age (t)
2 - group (n)
3 - valid_name (t)
4 - original_name (n)
5 - root_name (n). For names without their own root_name, put "see something" there
6 - authority (n)
7 - year (n)
8 - page_described (n)
9 - type (n)
10 - citation (n)
11 - verbatim_citation (n)
12 - nomenclature_comments (n)
13 - taxonomy_comments (n)
14 - other_comments (n)
15 - data (n)
16 - comments (t)
17 - data(t)
'''

def export_spreadsheet(txn, **kwargs):
	# To get rid of the overly large cache
	gc.collect()
	with export_tools.spreadsheet(txn.valid_name) as sprsh:
		# add first line
		first_line = export_tools.empty_row()
		if txn.parent is not None:
			first_line[0] = txn.parent.valid_name
		first_line[3] = "=COUNTA(D2:D10000)"
		first_line[4] = "=COUNTA(E2:E10000)/D1*100"
		first_line[6] = "=COUNTA(G2:G10000)/D1*100"
		first_line[7] = "=COUNTA(H2:H10000)/D1*100"
		first_line[8] = "=COUNTA(I2:I10000)/D1*100"
		first_line[10] = "=COUNTA(K2:K10000)/D1*100"

		sprsh.add_row(first_line, status=STATUS_DUBIOUS)
		export_taxon(txn, sprsh, is_root=True, **kwargs)

def fill_in_name(row, name):
	row[2] = abbrev_of_group(name.group)
	row[4] = name.original_name
	row[5] = name.root_name
	row[6] = name.authority
	row[7] = name.year
	row[8] = name.page_described
	row[9] = name.verbatim_type
	row[10] = name.original_citation
	row[11] = name.verbatim_citation
	row[12] = name.nomenclature_comments
	row[13] = name.taxonomy_comments
	row[14] = name.other_comments
	row[15] = name.data

def add_nas(row):
	row[4] = 'n/a'
	row[6] = 'n/a'
	row[7] = 'n/a'
	row[8] = 'n/a'
	row[10] = 'n/a'
	row[11] = 'n/a'

def export_taxon(txn, sprsh, is_root=False, recurse=True):
	# Start new spreadsheet
	if txn.is_page_root and not is_root:
		if recurse:
			export_spreadsheet(txn)
		return

	# Insert empty row for esthetics
	if txn.rank >= FAMILY:
		sprsh.add_row(export_tools.empty_row(), rank=SPECIES)

	taxon_row = export_tools.empty_row()
	# Taxon stuff
	try:
		taxon_row[0] = abbrev_of_rank(txn.rank)
	except KeyError:
		# Give numeric rank explicitly
		taxon_row[0] = str(txn.rank)
	taxon_row[1] = abbrev_of_age(txn.age)
	taxon_row[3] = txn.full_name()
	taxon_row[16] = txn.comments
	taxon_row[17] = txn.data

	# Find Name
	base_name = txn.base_name
	if base_name is None:
		taxon_row[5] = "n/a"
		add_nas(taxon_row)
	elif base_name.taxon.id != txn.id:
		taxon_row[5] = "see " + base_name.taxon.valid_name
		add_nas(taxon_row)
	else:
		fill_in_name(taxon_row, base_name)

	sprsh.add_row(taxon_row, rank=txn.rank)

	# Add synonyms
	for name in txn.sorted_names(exclude_valid=True):
		name_row = export_tools.empty_row()
		name_row[0] = abbrev_of_status(name.status)
		name_row[1] = abbrev_of_age(txn.age)
		name_row[3] = txn.full_name()
		fill_in_name(name_row, name)
		sprsh.add_row(name_row, status=name.status)

	# Add children
	for child in txn.sorted_children():
		export_taxon(child, sprsh, recurse=recurse)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Export the database into ODS files')

	parser.add_argument('--taxon', '-t', help="Taxon to export")
	parser.add_argument('--recursive', '-r', action='store_true', help="Perform the whole export in a single process")
	args = parser.parse_args()

	if args.recursive:
		root = Taxon.get(Taxon.rank == ROOT)
		export_spreadsheet(root, recurse=True)
	elif args.taxon:
		print "Exporting", args.taxon
		root = Taxon.get(Taxon.valid_name == args.taxon)
		export_spreadsheet(root, recurse=False)
	else:
		taxa = Taxon.filter(Taxon.is_page_root == True)
		for taxon in taxa:
			cmd = ' '.join(['python', 'export.py', '--taxon', taxon.valid_name])
			subprocess.call(cmd, shell=True)
