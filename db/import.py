#!/usr/bin/env python
from constants import *
import helpers
import models
from models import Name, Taxon

import argparse
import codecs
import csv
import json

def create_root():
	Taxon.create(rank=ROOT, valid_name='root')

HT_RANKS = {
	'Classis': CLASS,
	'Subclassis': SUBCLASS,
	'Infraclassis': INFRACLASS,
	'Superordo': SUPERORDER,
	'Supraordo': SUPERORDER,
	'Ordo': ORDER,
	'Subordo': SUBORDER,
	'Infraordo': INFRAORDER,
	'Parvordo': PARVORDER,
	'Superfamilia': SUPERFAMILY,
	'Suprafamilia': SUPERFAMILY,
	'Superfamily': SUPERFAMILY,
	'Familia': FAMILY,
	'Subfamilia': SUBFAMILY,
	'Tribus': TRIBE,
	'Subtribus': SUBTRIBE,
}

KIND_RANKS = {
	'gen': GENUS,
	'sgen': SUBGENUS,
	'spg': SPECIES_GROUP,
	'sp': SPECIES,
	'ssp': SUBSPECIES
}

def parse_row(row):
	'''Parse a row list into an associative array, then do some further magic with rank'''
	result = {
		'kind': row[8].strip(),
		'age': row[9],
		'valid_name': row[14],
		'original_name': row[15],
		'base_name': row[16],
		'authority': row[17],
		'year': row[19],
		'page_described': row[20],
		'other_comments': row[30],
		'data': {
			'column_K': row[10],
			'column_L': row[11],
			'column_M': row[12],
			'column_N': row[13],
			'parentheses': row[18],
			'type': row[21],
			'type_specimen': row[22],
			'distribution': row[23],
			'reference': row[24],
			'original_name_Y': row[25],
			'etymology': row[26],
			'karyo_2n': row[27],
			'karyo_FN': row[28],
			'placement': row[29],
			'English_name': row[31],
			'Dutch_name': row[32],
		},
	}
	if len(row) > 33:
		raise Exception("Missing data: " + str(row[33:]))
	if result['kind'] == 'HT':
		rank, valid_name = result['valid_name'].split(' ', 1)
		if rank not in HT_RANKS:
			raise Exception("Unknown HT rank: " + rank + result)
		result['rank'] = HT_RANKS[rank]
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
	elif result['kind'] == 'nHT':
		# dealt with at higher level
		pass
	else:
		raise Exception("Unknown kind: " + result['kind'] + str(result))
	# translate age
	if result['age'] == '':
		result['age'] = AGE_EXTANT
	elif result['age'] == 'h':
		result['age'] = AGE_HOLOCENE
	elif result['age'] == 'e':
		result['age'] == AGE_FOSSIL
	else:
		raise Exception("Unknown age group: " + result['age'] + result)
	# check for encoding
	if result['valid_name'] == 'Sudamericidae':
		print result
		print result['authority']
	return result

ignored_nHT = ['"Paracimexomys group"']

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
		root_parent = Taxon.filter(Taxon.valid_name == root_name)[0]

		# maintain stack of taxa that are parents of the current taxon
		stack = [root_parent]
		# current valid taxon (for synonyms)
		current_valid = None
		# whether current taxon should be marked as root of a page
		is_page_root = True
		for row in reader:
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

			if data['status'] == STATUS_VALID:
				# get stuff off the stack
				rank = data['rank']
				# TODO: make this somehow unranked-clade-aware
				while rank >= stack[-1].rank:
					stack.pop()
				# create new Taxon
				print "Creating Taxon:", data['valid_name']
				current_valid = Taxon.create(valid_name=data['valid_name'],
					rank=data['rank'], parent=stack[-1], is_page_root=is_page_root)
				if is_page_root:
					is_page_root = False
				stack.append(current_valid)
			# create new Name
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
				data['taxon'] = current_valid
				if data['status'] == STATUS_SYNONYM:
					if data['kind'] == 'synHT':
						valid_name = data['valid_name'].split(' ', 1)[1]
					else:
						valid_name = data['valid_name']
					if valid_name != current_valid.valid_name:
						raise Exception("Valid name of synonym does not match: " + data['valid_name'] + " and " + current_valid.valid_name)
			del data['kind']
			data['data'] = json.dumps(data['data'])
			Name.create(**data)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Import a CSV spreadsheet file into the database')

	parser.add_argument('--inputfile', '-f', help="Input file")
	parser.add_argument('--root', '-r', default=False, action='store_true',
		help="If set to true, the root taxon is created")
	args = parser.parse_args()

	with models.database.transaction():
		if args.root:
			create_root()

		read_file(args.inputfile)
