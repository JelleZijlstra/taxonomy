'''Helper functions'''

from constants import *

SPECIES_RANKS = [SUBSPECIES, SPECIES, SPECIES_GROUP]
GENUS_RANKS = [SUBGENUS, GENUS]
FAMILY_RANKS = [SUBTRIBE, TRIBE, SUBFAMILY, FAMILY, SUPERFAMILY]
HIGH_RANKS = [PARVORDER, INFRAORDER, SUBORDER, ORDER, SUPERORDER, SUBCOHORT, COHORT, SUPERCOHORT, INFRACLASS, SUBCLASS, CLASS, UNRANKED]

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

rank_names = {
	SUBSPECIES: 'subspecies',
	SPECIES: 'species',
	SPECIES_GROUP: 'species group',
	SUBGENUS: 'subgenus',
	GENUS: 'genus',
	SUBTRIBE: 'subtribe',
	TRIBE: 'tribe',
	SUBFAMILY: 'subfamily',
	FAMILY: 'family',
	SUPERFAMILY: 'superfamily',
	PARVORDER: 'parvorder',
	INFRAORDER: 'infraorder',
	SUBORDER: 'suborder',
	ORDER: 'order',
	SUPERORDER: 'superorder',
	SUBCOHORT: 'subcohort',
	COHORT: 'cohort',
	SUPERCOHORT: 'supercohort',
	INFRACLASS: 'infraclass',
	SUBCLASS: 'subclass',
	CLASS: 'class',
	ROOT: '(root)',
	UNRANKED: '(unranked)'
}

def string_of_rank(rank):
	return rank_names[rank]

group_names = {
	GROUP_SPECIES: 'species',
	GROUP_GENUS: 'genus',
	GROUP_FAMILY: 'family',
	GROUP_HIGH: 'higher taxon'
}

def string_of_group(group):
	return group_names[group]

status_names = {
	STATUS_VALID: 'valid',
	STATUS_DUBIOUS: 'dubious',
	STATUS_SYNONYM: 'synonym'
}

def string_of_status(status):
	return status_names[status]

age_names = {
	AGE_EXTANT: '',
	AGE_HOLOCENE: 'h',
	AGE_FOSSIL: 'e',
}

def string_of_age(age):
	return age_names[age]

def dict_of_name(name):
	result = {
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
		for child in taxon.children:
			result['children'].append(tree_of_taxon(child))
	return result
