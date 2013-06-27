'''Helper functions'''

from constants import *

SPECIES_RANKS = [SUBSPECIES, SPECIES, SPECIES_GROUP]
GENUS_RANKS = [SUBGENUS, GENUS]
FAMILY_RANKS = [SUBTRIBE, TRIBE, SUBFAMILY, FAMILY, SUPERFAMILY]
HIGH_RANKS = [PARVORDER, INFRAORDER, SUBORDER, ORDER, SUPERORDER, INFRACLASS, SUBCLASS, CLASS, UNRANKED]

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