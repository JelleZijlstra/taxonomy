#!/usr/bin/env python
# -*- coding: utf-8 -*-
from db.constants import *
import db.helpers as helpers
import db.models as models
from db.models import Name, Taxon
import export_tools

import argparse
import codecs
import csv
import json
import re
import sys
import traceback

def create_root():
	Taxon.create(rank=ROOT, valid_name='root', is_page_root=True)

def parse_row(row):
	'''Parse a row list into an associative array, then do some further magic with rank'''

	# get rid of curly quotes
	row = [cell.replace('“', '"').replace('”', '"').strip() for cell in row]
	row = [None if cell == '' else cell for cell in row]

	# set 16 and 17 if necessary
	if len(row) == 16:
		row.append(None)
	if len(row) == 17:
		row.append(None)

	result = {
		'rank': row[0],
		'age': row[1],
		'group': row[2],
		'valid_name': row[3],
		'original_name': row[4],
		'root_name': row[5],
		'authority': row[6],
		'year': row[7],
		'page_described': row[8],
		'verbatim_type': row[9],
		'original_citation': row[10],
		'verbatim_citation': row[11],
		'nomenclature_comments': row[12],
		'taxonomy_comments': row[13],
		'other_comments': row[14],
		'data': row[15],
		'comments_taxon': row[16],
		'data_taxon': row[17]
	}

	# validate name
	# complicated regex to allow for weird stuff in informal name
	regex = r"^([a-zA-Z \(\)\.\"]|\?|\"[a-zA-Z \(\)\-]+\")+$"
	valid_name = result['valid_name']
	if not re.match(regex, valid_name):
		raise Exception("Invalid name: " + valid_name)

	# get rid of explicit i.s.
	valid_name = valid_name.replace(' (?)', '')

	# get rid of explicit subgenus
	valid_name = re.sub(r' (\([A-Za-z" \-]+\) )+', ' ', valid_name)

	# subgenus name should just be the subgenus
	valid_name = re.sub(r'^.*\(([A-Z][A-Za-z" \-]+)\)$', r'\1', valid_name)

	result['valid_name'] = valid_name

	# translate textual classes into the numeric constants used internally
	try:
		status = status_of_abbrev(result['rank'])
	except KeyError:
		status = STATUS_VALID
	result['status'] = status
	if status == STATUS_VALID:
		try:
			result['rank'] = int(result['rank'])
		except ValueError:
			result['rank'] = abbreviations['rank'][result['rank']]

	if result['rank'] in helpers.SUFFIXES:
		suffix = helpers.SUFFIXES[result['rank']]
		assert valid_name.endswith(suffix), \
			"Taxon %s has an unexpected ending (expected %s)" % (valid_name, suffix)

	if result['group'] is not None:
		result['group'] = abbreviations['group'][result['group']]
	if result['age'] is None:
		result['age'] = AGE_EXTANT
	else:
		result['age'] = abbreviations['age'][result['age']]

	return result

# from http://stackoverflow.com/questions/904041/reading-a-utf8-csv-file-with-python
def unicode_csv_reader(utf8_data, **kwargs):
    csv_reader = csv.reader(utf8_data, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def read_file(filename):
	with codecs.open(filename, mode='r') as file:
		reader = csv.reader(file)
		first_line = reader.next()

		# name of parent of root taxon should be in cell A1
		root_name = first_line[0]
		if root_name:
			root_parent = Taxon.filter(Taxon.valid_name == root_name)[0]

			# maintain stack of taxa that are parents of the current taxon
			stack = [root_parent]
		else:
			stack = []

		# current valid taxon (for synonyms)
		current_valid = None
		# whether current taxon should be marked as root of a page
		is_page_root = True
		error_occurred = False
		for row in reader:
			try:
				# ignore blank rows
				if row[3] == '' and row[0] == '':
					continue
				data = parse_row(row)

				if data['status'] == STATUS_VALID:
					# get stuff off the stack
					rank = data['rank']
					# TODO: make this somehow unranked-clade-aware
					while len(stack) > 0 and rank >= stack[-1].rank:
						stack.pop()
					# create new Taxon
					current_valid = Taxon.create(valid_name=data['valid_name'], age=data['age'],
						rank=data['rank'], is_page_root=is_page_root,
						comments=data['comments_taxon'], data=data['data_taxon'])
					if len(stack) > 0:
						current_valid.parent = stack[-1]
					if is_page_root:
						is_page_root = False
					stack.append(current_valid)
				# create new Name
				data['taxon'] = current_valid
				assert current_valid.valid_name == data['valid_name'], \
					"Valid name %s does not match expected %s" % (data['valid_name'], current_valid.valid_name)

				data['data'] = helpers.fix_data(data['data'])

				# Detect whether a name object is already present (Principle of Coordination)
				nm = None
				if data['root_name'][0:4] == 'see ':
					seen = data['root_name'][4:]
					nm = Taxon.get(Taxon.valid_name == seen).base_name

				# create a new Name if none was found
				if nm is None:
					nm = Name.create(**data)

				# set base_name field
				if data['status'] == STATUS_VALID:
					current_valid.base_name = nm

			except Exception:
				traceback.print_exc()
				print('Error parsing row: %s' % row)
				error_occurred = True
				# ignore error and happily go on with the next
	return not error_occurred

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Import a CSV spreadsheet file into the database')

	parser.add_argument('--inputfile', '-f', help="Input file")
	parser.add_argument('--root', '-r', default=False, action='store_true',
		help="If set to true, the root taxon is created")
	args = parser.parse_args()

	with models.database.transaction():
		if args.root:
			create_root()

		result = read_file(args.inputfile)

	if result:
		sys.exit(0)
	else:
		sys.exit(1)
