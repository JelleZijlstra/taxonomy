'''Constants used in database'''

# Use large increments to allow for interpolating ranks later
SUBSPECIES = 0
SPECIES = 5
SPECIES_GROUP = 10
SUBGENUS = 15
GENUS = 20
SUBTRIBE = 25
TRIBE = 30
SUBFAMILY = 35
FAMILY = 40
SUPERFAMILY = 45
PARVORDER = 50
INFRAORDER = 55
SUBORDER = 60
ORDER = 65
SUPERORDER = 70
INFRACLASS = 90
SUBCLASS = 95
CLASS = 100
ROOT = 200
# unranked groups of any kind
UNRANKED = 205
INFORMAL = 210

# Nomenclatural group that the taxon belongs to
GROUP_SPECIES = 0
GROUP_GENUS = 1
GROUP_FAMILY = 2
GROUP_HIGH = 3

# Status of a name
STATUS_VALID = 0
STATUS_SYNONYM = 1
STATUS_DUBIOUS = 2 # nomen dubium, species inquirenda, etcetera

# Age classes
AGE_EXTANT = 0
AGE_HOLOCENE = 1
AGE_FOSSIL = 2
