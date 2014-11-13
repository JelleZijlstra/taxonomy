#!/usr/bin/env python
# -*- coding: utf-8 -*-
from constants import *
import helpers
import models
from models import Name, Taxon

import argparse
import codecs
import csv
import json
import re
import sys
import traceback

def create_root():
	Taxon.create(rank=ROOT, valid_name='root', is_page_root=True)

def remove_null(dict):
	out = {}
	for k, v in dict.items():
		if v is not None:
			out[k] = v
	return out

KIND_RANKS = {
	'gen': GENUS,
	'sgen': SUBGENUS,
	'spg': SPECIES_GROUP,
	'sp': SPECIES,
	'ssp': SUBSPECIES
}

def detect_super(name, rank):
	'''Find a taxon named name of rank rank'''
	try:
		return Taxon.get(Taxon.valid_name == name, Taxon.rank == rank).base_name
	except Taxon.DoesNotExist:
		return None

def parse_row(row):
	'''Parse a row list into an associative array, then do some further magic with rank'''
	# get rid of curly quotes
	row = [cell.replace('“', '"').replace('”', '"').strip() for cell in row]
	row = [None if cell == '' else cell for cell in row]
	result = {
		'kind': row[8].strip(),
		'age': row[9],
		'valid_name': row[14],
		'original_name': row[15],
		'root_name': row[16],
		'authority': row[17],
		'year': row[19],
		'page_described': row[20],
		'other_comments': row[30],
		'verbatim_type': row[21],
		'verbatim_citation': row[24],
		'data': {
			'column_K': row[10],
			'column_L': row[11],
			'column_M': row[12],
			'column_N': row[13],
			'parentheses': row[18],
			'type_specimen': row[22],
			'distribution': row[23],
			'original_name_Y': row[25],
			'etymology': row[26],
			'karyo_2n': row[27],
			'karyo_FN': row[28],
			'placement': row[29],
			'English_name': row[31],
			'Dutch_name': row[32],
		},
	}
	# random data in some files
	if len(row) == 34:
		# Perissodactyla
		result['data']['column_AH'] = row[33]
	elif len(row) == 35:
		# Eutheria
		result['data']['column_AH'] = row[33]
		result['data']['column_AI'] = row[34]
	elif len(row) == 36:
		# Cetartiodactyla
		result['data']['column_AH'] = row[33]
		result['data']['column_AI'] = row[34]
		result['data']['column_AJ'] = row[35]
	elif len(row) > 33:
		raise Exception("Missing data: " + str(row[33:]))

	# deal with bad data
	if ' /' in result['valid_name']:
		names = result['valid_name'].split(' /')
		result['valid_name'] = names[0]
		result['additional_synonyms'] = names[1:]
	if result['root_name'] == None:
		if result['original_name'] == None:
			result['root_name'] = result['valid_name']
		else:
			result['root_name'] = result['original_name']
	result['root_name'] = result['root_name'].split()[-1].replace('(', '').replace(')', '')

	# validate name
	# complicated regex to allow for weird stuff in informal name
	regex = r"^([a-zA-Z \(\)\.\"]|\"[a-zA-Z \(\)\-]+\")+$"
	valid_name = result['valid_name']
	if not re.match(regex, valid_name):
		raise Exception("Invalid name: " + valid_name)

	# get rid of explicit i.s.
	valid_name = valid_name.replace(' ( i.s.)', '')

	# get rid of explicit subgenus
	valid_name = re.sub(r' (\([A-Za-z" \-]+\) )+', ' ', valid_name)

	# subgenus name should just be the subgenus
	valid_name = re.sub(r'^.*\(([A-Z][A-Za-z" \-]+)\)$', r'\1', valid_name)

	result['valid_name'] = valid_name

	# translate textual classes into the numeric constants used internally
	if result['kind'] == 'HT':
		if 'Division' in result['valid_name']:
			rank = 'Division'
			valid_name = result['valid_name']
		else:
			rank, valid_name = result['valid_name'].split(' ', 1)
		result['rank'] = db.helpers.string_of_rank(rank)
		result['valid_name'] = valid_name
		result['status'] = STATUS_VALID
	elif result['kind'] in KIND_RANKS:
		result['rank'] = KIND_RANKS[result['kind']]
		result['status'] = STATUS_VALID
	elif result['kind'][0:3] == 'syn':
		result['status'] = STATUS_SYNONYM
		# minor fix
		if result['kind'] == 'synht':
			result['kind'] = 'synHT'
	elif result['kind'] == 'si':
		result['status'] = STATUS_DUBIOUS
	elif result['kind'] in ('nHT', 'nsgen'):
		# dealt with at higher level
		pass
	else:
		raise Exception("Unknown kind: " + result['kind'] + str(result))
	# translate age
	if result['age'] == None:
		result['age'] = AGE_EXTANT
	elif result['age'] == 'h':
		result['age'] = AGE_HOLOCENE
	elif result['age'] == 'e':
		result['age'] = AGE_FOSSIL
	else:
		raise Exception("Unknown age group: " + result['age'] + result)
	return result

ignored_nHT = ['Paracimexomys group', 'Extinct genera of uncertain or basal placement']

# from http://stackoverflow.com/questions/904041/reading-a-utf8-csv-file-with-python
def unicode_csv_reader(utf8_data, **kwargs):
    csv_reader = csv.reader(utf8_data, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def read_file(filename):
	with codecs.open(filename, mode='r') as file:
		reader = csv.reader(file)
		first_line = reader.next()

		# maintain stack of taxa that are parents of the current taxon
		stack = []
		# name of parent of root taxon should be in cell A1
		root_name = first_line[0]
		if root_name != '':
			root_parent = Taxon.filter(Taxon.valid_name == root_name)[0]
			stack.append(root_parent)

		# current valid taxon (for synonyms)
		current_valid = None
		# whether current taxon should be marked as root of a page
		is_page_root = True
		error_occurred = False
		for row in reader:
			try:
				# ignore blank rows
				if row[14] == '' and row[8] == '':
					continue
				data = parse_row(row)
				# deal with "nHT", which is a pain. Some of these may need to be manually
				# readded to the DB
				if data['kind'] == 'nHT':
					if data['valid_name'] in ignored_nHT:
						continue
					else:
						raise Exception("Unrecognized nHT: " + str(data))
				# nsgen is i.s., just ignore
				if data['kind'] == 'nsgen':
					continue

				if data['status'] == STATUS_VALID:
					# get stuff off the stack
					rank = data['rank']
					if rank == ROOT:
						current_valid = Taxon.create(valid_name=data['valid_name'], age=data['age'],
							rank=data['rank'], is_page_root=True)
					else:
						# TODO: make this somehow unranked-clade-aware
						while rank >= stack[-1].rank:
							stack.pop()
						# create new Taxon
						current_valid = Taxon.create(valid_name=data['valid_name'], age=data['age'],
							rank=data['rank'], parent=stack[-1], is_page_root=is_page_root)
					if is_page_root:
						is_page_root = False
					stack.append(current_valid)
				# create new Name
				data['taxon'] = current_valid
				if data['status'] == STATUS_DUBIOUS:
					# current system is inadequate for properly marking si species
					# assume there's only genera and species
					if ' ' in data['valid_name']:
						data['group'] = GROUP_SPECIES
					else:
						data['group'] = GROUP_GENUS
					# si species don't have a meaningful "valid name", but preserve what's there now
					data['data']['si_valid_name'] = data['valid_name']
				else:
					# this will be wrong in the few cases where a high-ranked name is listed
					# as a synonym of a family-group name. Don't see a way to correct that
					# programmatically.
					data['group'] = helpers.group_of_rank(current_valid.rank)
					if data['status'] == STATUS_SYNONYM:
						if data['kind'] == 'synHT':
							valid_name = data['valid_name'].split(' ', 1)[1]
						else:
							valid_name = data['valid_name']
						if valid_name != current_valid.valid_name:
							raise Exception("Valid name of synonym does not match: " + data['valid_name'] + " and " + current_valid.valid_name)
				# shorten root name for family-group names
				if data['group'] == GROUP_FAMILY:
					data['root_name'] = helpers.strip_rank(data['root_name'], current_valid.rank)
				del data['kind']
				data['data'] = json.dumps(remove_null(data['data']))

				# Detect whether a name object is already present (Principle of Coordination)
				nm = None
				if data['status'] == STATUS_VALID:
					root_name = data['root_name']
					if current_valid.rank == FAMILY:
						nm = detect_super(root_name + 'oidea', SUPERFAMILY)
					elif current_valid.rank == SUBFAMILY:
						nm = detect_super(root_name + 'idae', FAMILY)
					elif current_valid.rank == TRIBE:
						nm = detect_super(root_name + 'inae', SUBFAMILY)
					elif current_valid.rank == SUBTRIBE:
						nm = detect_super(root_name + 'ini', TRIBE)
					elif current_valid.rank == SUBGENUS:
						nm = detect_super(root_name, GENUS)
					elif current_valid.rank == SPECIES:
						spg_name = helpers.spg_of_species(current_valid.valid_name)
						nm = detect_super(spg_name, SPECIES_GROUP)
					elif current_valid.rank == SUBSPECIES and helpers.is_nominate_subspecies(current_valid.valid_name):
						sp_name = helpers.species_of_subspecies(current_valid.valid_name)
						nm = detect_super(sp_name, SPECIES)
					if nm is not None:
						del data['taxon']
						nm.add_additional_data(data)
						# nm's Taxon should be the lowest-ranking one
						nm.taxon = current_valid

				# create a new Name if none was found
				if nm is None:
					nm = Name.create(**data)

				# set base_name field
				if data['status'] == STATUS_VALID:
					current_valid.base_name = nm
				if 'additional_synonyms' in data:
					group = helpers.group_of_rank(current_valid.rank)
					for synonym in data['additional_synonyms']:
						Name.create(taxon=current_valid, root_name=synonym, group=group, status=STATUS_SYNONYM)

			except Exception, e:
				print traceback.format_exc(e)
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
