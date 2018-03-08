import csv
import re
import sys
from typing import List

from . import lib
from .lib import DataT

SOURCE = lib.Source('msw3-all.csv', 'Mammalia-review (MSW3)')


def translate_to_db(names: DataT) -> DataT:
    for name in names:
        if name['TaxonLevel'] == 'SUBSPECIES':
            # no useful data
            continue
        if not name['Genus']:
            # rank above genus; not much useful data and hard to match
            continue
        if name['TaxonLevel'] == 'SPECIES':
            name['valid_name'] = f'{name["Genus"]} {name["Species"]}'
        else:
            for field in ('Subgenus', 'Genus', 'Tribe', 'Subfamily', 'Family', 'Superfamily', 'Infraorder', 'Suborder', 'Order'):
                if name[field]:
                    name['valid_name'] = name[field].title()
                    break
        name['raw_text'] = dict(name)
        authority = name['Author'].replace('(', '').replace(')', '').replace(' and ', ' & ')
        if authority == 'É. Geoffroy':
            authority = 'É. Geoffroy Saint-Hilaire'
        elif authority == 'Blainville':
            authority = 'de Blainville'
        authority = re.sub(r'([A-Z]\.) (?=[A-Z]\.)', r'\1', authority)
        name['authority'] = authority
        if name['ActualDate']:
            name['year'] = name['ActualDate']
        else:
            name['year'] = name['Date']
        if name['CitationName']:
            verbatim = name['CitationName']
            if name['CitationVolume']:
                verbatim += f', vol. {name["CitationVolume"]}'
            if name['CitationIssue']:
                verbatim += f', {name["CitationIssue"]}'
            if name['CitationPages']:
                verbatim += f', {name["CitationPages"]}'
                name['page_described'] = name['CitationPages']
            if name['CitationType']:
                verbatim += f', {name["CitationType"]}'
            name['verbatim_citation'] = verbatim
        if name['TypeLocality']:
            name['loc'] = name['TypeLocality']
        if name['TypeSpecies']:
            name['verbatim_type'] = f'{name["TypeSpecies"]} [from {{Mammalia-review (MSW3)}}]'
        yield name


def main(argv: List[str]) -> DataT:
    lines = lib.get_text(SOURCE)
    names = csv.DictReader(lines)
    names = translate_to_db(names)
    names = lib.translate_to_db(names, 'USNM', SOURCE)
    names = lib.translate_type_locality(names, quiet=True)
    names = lib.associate_names(names, lib.NameConfig({
        'De Winton': 'de Winton',
        'von Lehmann': 'Lehmann',
        'Cockrum, Vaughn & Vaughn': 'Cockrum, Vaughan & Vaughan',
        'Dalebout et al.': 'Dalebout, Mead, Baker, Baker & Van Helden',
        'Von Haast': 'von Haast',
        'Zaglossus bruijni': 'Tachyglossus bruijnii',
    }), name_field='valid_name', quiet=True, max_distance=2, try_manual=True, use_taxon_match=True, start_at='Saguinus niger')
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    if len(argv) > 1:
        lib.print_counts(names, argv[1])
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for _ in main(sys.argv):
        print(_)
