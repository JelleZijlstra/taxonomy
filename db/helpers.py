'''Helper functions'''

from operator import itemgetter
import re
import json

from .constants import *

SPECIES_RANKS = [SUBSPECIES, SPECIES, SPECIES_GROUP]
GENUS_RANKS = [SUBGENUS, GENUS]
FAMILY_RANKS = [SUBTRIBE, TRIBE, SUBFAMILY, FAMILY, SUPERFAMILY]
HIGH_RANKS = [
    ROOT, 43, DIVISION, PARVORDER, INFRAORDER, SUBORDER, ORDER, SUPERORDER, SUBCOHORT, COHORT,
    SUPERCOHORT, INFRACLASS, SUBCLASS, CLASS, SUPERCLASS, INFRAPHYLUM, SUBPHYLUM, PHYLUM,
    SUPERPHYLUM, INFRAKINGDOM, SUBKINGDOM, KINGDOM, SUPERKINGDOM, DOMAIN, UNRANKED
]

def group_of_rank(rank):
    if rank in SPECIES_RANKS:
        return GROUP_SPECIES
    elif rank in GENUS_RANKS:
        return GROUP_GENUS
    elif rank in FAMILY_RANKS or rank == 34 or rank == 24:
        return GROUP_FAMILY
    elif rank in HIGH_RANKS or rank > SUPERFAMILY:
        return GROUP_HIGH
    else:
        raise Exception("Unrecognized rank: " + str(rank))

SUFFIXES = {
    INFRATRIBE: 'ita',
    SUBTRIBE: 'ina',
    TRIBE: 'ini',
    SUBFAMILY: 'inae',
    FAMILY: 'idae',
    SUPERFAMILY: 'oidea'
}

def name_with_suffixes_removed(name):
    suffixes = list(SUFFIXES.values()) + ['ida', 'oidae', 'i', 'a']
    for suffix in suffixes:
        if name.endswith(suffix):
            yield re.sub(r'%s$' % suffix, '', name)

def suffix_of_rank(rank):
    return SUFFIXES[rank]

_RANKS = {
    'root': ROOT,
    'Unnamed rank': ROOT,
    'Classis': CLASS,
    'Class': CLASS,
    'Subclassis': SUBCLASS,
    'Subclass': SUBCLASS,
    'Infraclassis': INFRACLASS,
    'Infraclass': INFRACLASS,
    'Superlegion': 89,
    'Legion': 88,
    'Sublegion': 87,
    'Supracohors': SUPERCOHORT,
    'Supercohors': SUPERCOHORT,
    'Supercohort': SUPERCOHORT,
    'Cohors': COHORT,
    'Cohort': COHORT,
    'Subcohors': SUBCOHORT,
    'Magnorder': 72,
    'Grandorder': 71,
    'Superordo': SUPERORDER,
    'Supraordo': SUPERORDER,
    'Superorder': SUPERORDER,
    'Mirorder': 69,
    'Ordo': ORDER,
    'Order': ORDER,
    'Subordo': SUBORDER,
    'Suborder': SUBORDER,
    'Infraordo': INFRAORDER,
    'Infraorder': INFRAORDER,
    'Parvordo': PARVORDER,
    'Parvorder': PARVORDER,
    'Superfamilia': SUPERFAMILY,
    'Suprafamilia': SUPERFAMILY,
    'Superfamily': SUPERFAMILY,
    'Clade': 43, # Hack to allow for Eumuroida and Spalacodonta
    'Familia': FAMILY,
    'Family': FAMILY,
    'Subfamilia': SUBFAMILY,
    'Subfamily': SUBFAMILY,
    'Infrafamily': 34,
    'Tribus': TRIBE,
    'Tribe': TRIBE,
    'Subtribus': SUBTRIBE,
    'Subtribe': SUBTRIBE,
    'Infratribe': INFRATRIBE,
    'Division': DIVISION,
    'Genus': GENUS,
    'Subgenus': SUBGENUS,
}

def rank_of_string(s):
    try:
        return _RANKS[s]
    except KeyError:
        raise Exception("Unknown rank: " + s)

def root_name_of_name(s, rank):
    if rank == SPECIES or rank == SUBSPECIES:
        return s.split()[-1]
    elif group_of_rank(rank) == GROUP_FAMILY:
        return strip_rank(s, rank)
    else:
        return s


def strip_rank(name, rank, quiet=False):
    def strip_of_suffix(name, suffix):
        if re.search(suffix + "$", name):
            return re.sub(suffix + "$", "", name)
        else:
            return None

    try:
        suffix = suffix_of_rank(rank)
        res = strip_of_suffix(name, suffix)
    except KeyError:
        res = None
    if res is None:
        if not quiet:
            print("Warning: Cannot find suffix -" + suffix + " on name " + name)
        # Loop over other possibilities
        for rank in SUFFIXES:
            res = strip_of_suffix(name, SUFFIXES[rank])
            if res is not None:
                return res
        return name
    else:
        return res

def spg_of_species(species):
    '''Returns a species group name from a species name'''
    return re.sub(r" ([a-z]+)$", r" (\1)", species)

def species_of_subspecies(ssp):
    return re.sub(r" ([a-z]+)$", r"", ssp)

def is_nominate_subspecies(ssp):
    parts = re.sub(r' \(([A-Za-z"\-\. ]+)\)', '', ssp).split(' ')
    if len(parts) != 3:
        print(parts)
        raise Exception("Invalid subspecies name: " + ssp)
    return parts[1] == parts[2]

def dict_of_name(name):
    result = {
        'id': name.id,
        'authority': name.authority,
        'root_name': name.root_name,
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
            result['type']['name'] = name.type.root_name
    return result

def dict_of_taxon(taxon):
    return {
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

def tree_of_taxon(taxon, include_root=False):
    result = dict_of_taxon(taxon)
    if include_root or not taxon.is_page_root:
        for name in taxon.names:
            result['names'].append(dict_of_name(name))
        result['names'].sort(key=itemgetter('status_numeric', 'root_name'))
        for child in taxon.children:
            result['children'].append(tree_of_taxon(child))
        result['children'].sort(key=itemgetter('rank_numeric', 'valid_name'))
    return result

def remove_null(dict):
    out = {}
    for k, v in dict.items():
        if v is not None:
            out[k] = v
    return out

def fix_data(data):
    if data:
        data = json.dumps(remove_null(json.loads(data)))
        if data == '{}':
            return None
        else:
            return data
    else:
        return None


def convert_gender(name, gender):
    name = _canonicalize_gender(name)
    if gender == Gender.masculine:
        return name
    elif gender == Gender.feminine:
        # TODO this will fail occasionally
        if name.endswith('us'):
            return re.sub(r'us$', 'a', name)
        elif name.endswith('er'):
            return name + 'a'
        else:
            return name
    elif gender == Gender.neuter:
        # should really only be ensis but let's be broader
        if name.endswith('is'):
            return re.sub(r'is$', 'e', name)
        elif name.endswith('us'):
            return re.sub(r'us$', 'um', name)
        else:
            return name


def _canonicalize_gender(name):
    if name.endswith('e'):
        return re.sub(r'e$', 'is', name)
    elif name.endswith('era'):
        return name[:-1]
    elif name.endswith('a'):
        # TODO this will have a boatload of false positives
        return re.sub(r'a$', 'us', name)
    elif name.endswith('um'):
        # TODO this will have a boatload of false positives
        return re.sub(r'um$', 'us', name)
    else:
        return name
