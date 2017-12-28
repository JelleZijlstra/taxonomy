'''Helper functions'''

from operator import itemgetter
import re
import json
from typing import Any, Dict, Mapping, Iterable, Optional, TypeVar, TYPE_CHECKING

from . import constants
from .constants import Group, Rank

if TYPE_CHECKING:
    from .models import Name, Taxon

SPECIES_RANKS = [Rank.subspecies, Rank.species, Rank.species_group]
GENUS_RANKS = [Rank.subgenus, Rank.genus]
FAMILY_RANKS = [Rank.infratribe, Rank.subtribe, Rank.tribe, Rank.subfamily, Rank.family, Rank.superfamily,
                Rank.hyperfamily]
HIGH_RANKS = [
    Rank.root, 43, Rank.division, Rank.parvorder, Rank.infraorder, Rank.suborder,
    Rank.order, Rank.superorder, Rank.subcohort, Rank.cohort, Rank.supercohort,
    Rank.infraclass, Rank.subclass, Rank.class_, Rank.superclass, Rank.infraphylum,
    Rank.subphylum, Rank.phylum, Rank.superphylum, Rank.infrakingdom, Rank.subkingdom,
    Rank.kingdom, Rank.superkingdom, Rank.domain, Rank.unranked,
]
SUFFIXES = {
    Rank.infratribe: 'ita',
    Rank.subtribe: 'ina',
    Rank.tribe: 'ini',
    Rank.subfamily: 'inae',
    Rank.family: 'idae',
    Rank.superfamily: 'oidea',
    Rank.hyperfamily: 'oides',
}
_RANKS = {
    'root': Rank.root,
    'Unnamed rank': Rank.root,
    'Classis': Rank.class_,
    'Class': Rank.class_,
    'Subclassis': Rank.subclass,
    'Subclass': Rank.subclass,
    'Infraclassis': Rank.infraclass,
    'Infraclass': Rank.infraclass,
    'Superlegion': 89,
    'Legion': 88,
    'Sublegion': 87,
    'Supracohors': Rank.supercohort,
    'Supercohors': Rank.supercohort,
    'Supercohort': Rank.supercohort,
    'Cohors': Rank.cohort,
    'Cohort': Rank.cohort,
    'Subcohors': Rank.subcohort,
    'Magnorder': 72,
    'Grandorder': 71,
    'Superordo': Rank.superorder,
    'Supraordo': Rank.superorder,
    'Superorder': Rank.superorder,
    'Mirorder': 69,
    'Ordo': Rank.order,
    'Order': Rank.order,
    'Subordo': Rank.suborder,
    'Suborder': Rank.suborder,
    'Infraordo': Rank.infraorder,
    'Infraorder': Rank.infraorder,
    'Parvordo': Rank.parvorder,
    'Parvorder': Rank.parvorder,
    'Superfamilia': Rank.superfamily,
    'Suprafamilia': Rank.superfamily,
    'Superfamily': Rank.superfamily,
    'Clade': 43,  # Hack to allow for Eumuroida and Spalacodonta
    'Familia': Rank.family,
    'Family': Rank.family,
    'Subfamilia': Rank.subfamily,
    'Subfamily': Rank.subfamily,
    'Infrafamily': 34,
    'Tribus': Rank.tribe,
    'Tribe': Rank.tribe,
    'Subtribus': Rank.subtribe,
    'Subtribe': Rank.subtribe,
    'Infratribe': Rank.infratribe,
    'Division': Rank.division,
    'Genus': Rank.genus,
    'Subgenus': Rank.subgenus,
}


def group_of_rank(rank: Rank) -> Group:
    if rank in SPECIES_RANKS:
        return Group.species
    elif rank in GENUS_RANKS:
        return Group.genus
    elif rank in FAMILY_RANKS or rank == 34 or rank == 24:
        return Group.family
    elif rank in HIGH_RANKS or rank > Rank.hyperfamily:
        return Group.high
    else:
        raise ValueError("Unrecognized rank: " + str(rank))


def name_with_suffixes_removed(name: str) -> Iterable[str]:
    suffixes = list(SUFFIXES.values()) + ['ida', 'oidae', 'ides', 'i', 'a', 'ae', 'ia']
    for suffix in suffixes:
        if name.endswith(suffix):
            yield re.sub(r'%s$' % suffix, '', name)


def suffix_of_rank(rank: Rank) -> str:
    return SUFFIXES[rank]


def rank_of_string(s: str) -> Rank:
    try:
        return _RANKS[s]  # type: ignore
    except KeyError:
        raise ValueError("Unknown rank: " + s)


def root_name_of_name(s: str, rank: Rank) -> str:
    if rank == Rank.species or rank == Rank.subspecies:
        return s.split()[-1]
    elif group_of_rank(rank) == Group.family:
        return strip_rank(s, rank)
    else:
        return s


def strip_rank(name: str, rank: Rank, quiet: bool=False) -> str:
    def strip_of_suffix(name: str, suffix: str) -> Optional[str]:
        if re.search(suffix + "$", name):
            return re.sub(suffix + "$", "", name)
        else:
            return None

    suffix = suffix_of_rank(rank)
    try:
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


def spg_of_species(species: str) -> str:
    '''Returns a species group name from a species name'''
    return re.sub(r" ([a-z]+)$", r" (\1)", species)


def species_of_subspecies(ssp: str) -> str:
    return re.sub(r" ([a-z]+)$", r"", ssp)


def is_nominate_subspecies(ssp: str) -> bool:
    parts = re.sub(r' \(([A-Za-z"\-\. ]+)\)', '', ssp).split(' ')
    if len(parts) != 3:
        print(parts)
        raise Exception("Invalid subspecies name: " + ssp)
    return parts[1] == parts[2]


def dict_of_name(name: 'Name') -> Dict[str, Any]:
    result = {
        'id': name.id,
        'authority': name.authority,
        'root_name': name.root_name,
        'group_numeric': name.group.value,
        'group': name.group.name,
        'nomenclature_comments': name.nomenclature_comments,
        'original_citation': name.original_citation,
        'original_name': name.original_name,
        'other_comments': name.other_comments,
        'page_described': name.page_described,
        'status_numeric': name.status.value,
        'status': name.status.name,
        'taxonomy_comments': name.taxonomy_comments,
        'year': name.year
    }
    if name.type is not None:
        result['type'] = {'id': name.type.id}
        if name.type.original_name is not None:
            result['type']['name'] = name.type.original_name
        else:
            result['type']['name'] = name.type.root_name
    return result


def dict_of_taxon(taxon: 'Taxon') -> Dict[str, Any]:
    return {
        'id': taxon.id,
        'valid_name': taxon.valid_name,
        'rank_numeric': taxon.rank.value,
        'rank': taxon.rank.name,
        'comments': taxon.comments,
        'names': [],
        'children': [],
        'age_numeric': taxon.age.value,
        'age': taxon.age.name,
    }


def tree_of_taxon(taxon: 'Taxon', include_root: bool=False) -> Dict[str, Any]:
    result = dict_of_taxon(taxon)
    if include_root or not taxon.is_page_root:
        for name in taxon.names:
            result['names'].append(dict_of_name(name))
        result['names'].sort(key=itemgetter('status_numeric', 'root_name'))
        for child in taxon.children:
            result['children'].append(tree_of_taxon(child))
        result['children'].sort(key=itemgetter('rank_numeric', 'valid_name'))
    return result


_T1 = TypeVar('_T1')
_T2 = TypeVar('_T2')


def remove_null(d: Mapping[_T1, Optional[_T2]]) -> Dict[_T1, _T2]:
    out = {}
    for k, v in d.items():
        if v is not None:
            out[k] = v
    return out


def fix_data(data: str) -> Optional[str]:
    if data:
        data = json.dumps(remove_null(json.loads(data)))
        if data == '{}':
            return None
        else:
            return data
    else:
        return None


def convert_gender(name: str, gender: constants.Gender) -> str:
    name = _canonicalize_gender(name)
    if gender == constants.Gender.masculine:
        return name
    elif gender == constants.Gender.feminine:
        # TODO this will fail occasionally
        if name.endswith('us'):
            return re.sub(r'us$', 'a', name)
        elif name.endswith('er'):
            return name + 'a'
        else:
            return name
    elif gender == constants.Gender.neuter:
        # should really only be ensis but let's be broader
        if name.endswith('is'):
            return re.sub(r'is$', 'e', name)
        elif name.endswith('us'):
            return re.sub(r'us$', 'um', name)
        else:
            return name
    else:
        raise ValueError('unknown gender {}'.format(gender))


def _canonicalize_gender(name: str) -> str:
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
