'''Helper functions'''

from operator import itemgetter

from constants import *

SPECIES_RANKS = [SUBSPECIES, SPECIES, SPECIES_GROUP]
GENUS_RANKS = [SUBGENUS, GENUS]
FAMILY_RANKS = [SUBTRIBE, TRIBE, SUBFAMILY, FAMILY, SUPERFAMILY]
HIGH_RANKS = [43, DIVISION, PARVORDER, INFRAORDER, SUBORDER, ORDER, SUPERORDER, SUBCOHORT, COHORT, SUPERCOHORT, INFRACLASS, SUBCLASS, CLASS, UNRANKED]

def group_of_rank(rank):
	if rank in SPECIES_RANKS:
		return GROUP_SPECIES
	elif rank in GENUS_RANKS:
		return GROUP_GENUS
	elif rank in FAMILY_RANKS:
		return GROUP_FAMILY
	elif rank in HIGH_RANKS:
		return GROUP_HIGH
	else:
		raise Exception("Unrecognized rank: " + str(rank))

def dict_of_name(name):
	result = {
		'id': name.id,
		'authority': name.authority,
		'base_name': name.base_name,
		'group_numeric': name.group,
		'group': string_of_group(name.group),
		'nomenclature_comments': name.nomenclature_comments,
		'original_citation': name.original_citation,
		'original_name': name.original_name,
		'other_comments': name.other_comments,
		'page_described': name.page_described,
		'status_numeric': name.status,
		'status': string_of_status(name.status),
		'taxonomy_comments': name.taxonomy_comments,
		'year': name.year
	}
	if name.type is not None:
		result['type'] = {'id': name.type.id }
		if name.type.original_name is not None:
			result['type']['name'] = name.type.original_name
		else:
			result['type']['name'] = name.type.base_name
	return result

def tree_of_taxon(taxon, include_root=False):
	result = {
		'id': taxon.id,
		'valid_name': taxon.valid_name,
		'rank_numeric': taxon.rank,
		'rank': string_of_rank(taxon.rank),
		'comments': taxon.comments,
		'names': [],
		'children': [],
		'age_numeric': taxon.age,
		'age': string_of_age(taxon.age)
	}
	if include_root or not taxon.is_page_root:
		for name in taxon.names:
			result['names'].append(dict_of_name(name))
		result['names'].sort(key=itemgetter('status_numeric', 'base_name'))
		for child in taxon.children:
			result['children'].append(tree_of_taxon(child))
		result['children'].sort(key=itemgetter('rank_numeric', 'valid_name'))
	return result
