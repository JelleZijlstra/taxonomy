import itertools
import re
from typing import Any, Counter, Dict, Iterable, List, Optional, Tuple

from taxonomy import getinput
from taxonomy.db import constants, helpers, models

from . import lib
from .lib import DataT

SOURCE = lib.Source('sam.txt', 'South America.pdf')
RefsDictT = Dict[Tuple[str, str], str]


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    current_lines = []
    current_pages = []
    in_synonymy = False
    found_references = False
    last_author = ''
    for page, lines in pages:
        if current_pages:
            current_pages.append(page)
        for line in lines:
            if found_references:
                if not line:
                    continue
                elif re.match(r'^ +\. \d{4}', line):
                    line = re.sub(r'^ +\.', last_author, line)

                if line.startswith(' '):
                    assert current_lines
                    current_lines.append(line)
                else:
                    if current_lines:
                        yield {'raw_text': current_lines, 'pages': current_pages, 't': 2}
                        current_lines = []
                    current_lines = [line]
                    current_pages = [page]
                    last_author = re.sub(r' \[?\d{4}[a-z]?\]?\..*$', '', line)
            else:
                if not line:
                    pass
                elif line.strip() == 'Literature Cited':
                    found_references = True
                elif line.strip().startswith(('synonym:', 'synonyms:')):
                    in_synonymy = True
                elif not in_synonymy:
                    pass
                elif re.match(r' +([a-z] ){5,}| +This subspecies| +This is|KEY TO| +The | +Endemic to| +Additional| +Distribution:| +Although| +In South| +A gray| +Known primarily|Map \d+', line):
                    in_synonymy = False
                elif re.search(r'^[A-Z][a-z]+ [a-z]+ \(?[A-ZÉ][\. a-zA-Z\-]+, \d{4}\)?$', line):
                    in_synonymy = False
                elif line.startswith(' '):
                    assert current_lines
                    current_lines.append(line)
                else:
                    if current_lines:
                        yield {'raw_text': current_lines, 'pages': current_pages, 't': 1}
                        current_lines = []
                    current_lines = [line]
                    current_pages = [page]


def build_refs_dict(refs: DataT) -> RefsDictT:
    refs_dict: RefsDictT = {}
    for ref in refs:
        text = ref['raw_text']
        match = re.match(r'^(.*?)[,\.]\'? \[?(\d+(–\d+)?[a-z]?)\]?\. ', text)
        assert match, f'failed to match {text}'
        year = match.group(2)
        raw_authors = match.group(1)
        if ', and ' not in raw_authors:
            authors = re.sub(r', .*$', '', raw_authors)
            if authors in ('Allen', 'Davis', 'Wagner', 'Philippi', 'Thomas', 'Geoffroy St.-Hilaire', 'Fischer', 'Gervais', 'Peters',
                           'LeConte', 'Miranda-Ribeiro', 'Lima', 'Smith', 'Anderson', 'Vieira', 'Peterson', 'Johnson',
                           'Anthony', 'Shaw', 'Peale', 'Owen', 'Cuvier', 'Carter', 'Brown'):
                authors = re.sub(r'^(.+), (.*)$', r'\2. \1', raw_authors).replace(' da ', ' ').replace(' de. ', ' ')
        else:
            authors = re.sub(r'( [A-Z]\.)+,', '', raw_authors)
            authors = re.sub(r',( [A-Z]\.)+', ',', authors)
            authors = re.sub(r', and( [A-Z]\.)+', ' and', authors)
            authors = re.sub(r', (.*) and ', r', \1, and ', authors)
        authors = authors.replace(', Jr', '')
        assert (authors, year) not in refs_dict, f'duplicate key ({authors!r}, {year!r}) (new: {text}, existing: {refs_dict[(authors, year)]}'
        refs_dict[(authors, year)] = text
    # for key, value in refs_dict.items():
    #     print(key)
    #     print(value)
    return refs_dict


def split_text(names: DataT) -> DataT:
    for name in names:
        # (?P<original_name>\[?[A-Z].*( [a-z-\[\],]{3,})):? (?P<authority>(de )?[A-Z].*?)
        match = re.match(r'^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?): ?(?P<page_described>[^;]+); (?P<rest>.*)$', name['raw_text'])
        if not match:
            match = re.match(r'^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?)[:,;] ?(?P<page_described>\d+)([;,:] (?P<rest>.*)|\.)$', name['raw_text'])
            if not match:
                match = re.match(r'^(?P<name_authority>[^\d]+?),? (?P<year>\d{4}[a-z]?)([:,] ?(?P<page_described>\d+))?; (?P<rest>.*)$', name['raw_text'])
                if not match:
                    continue
        name.update(match.groupdict())
        name_authority = name['name_authority'].replace('’', "'")
        if ': ' in name_authority:
            name['original_name'], name['authority'] = name_authority.split(': ')
            name['has_colon'] = True
        else:
            name.update(split_name_authority(name_authority, try_harder=True))
        if '"' in name['original_name'] or 'sp.' in name['original_name'] or 'species' in name['original_name'] or 'var. γ' in name['original_name'] or 'Var. a.' in name['original_name'] or 'spec.' in name['original_name']:
            name['is_informal'] = True
        yield name


def split_name_authority(name_authority: str, *, try_harder: bool = False, quiet: bool = False) -> Dict[str, str]:
    name_authority = re.sub(r'([A-Za-z][a-z]*)\[([a-z?]+( \([A-Z][a-z]+\))?)\]\.', r'\1\2', name_authority)
    name_authority = re.sub(r'([A-Z][a-z]*)\[([a-z]+)\]', r'\1\2', name_authority)
    name_authority = re.sub(r'^\[([A-Z][a-z]+)\]', r'\1', name_authority)
    name_authority = re.sub(r'\[\([A-Z][a-z]+\)\] ', r'', name_authority)
    name_authority = re.sub(r'^\[[A-Z][a-z]+ \(\]([A-Z][a-z]+)\[\)\]', r'\1', name_authority)
    regexes = [
        r'^(?P<original_name>[A-ZÑ][a-zëöiï]+) (?P<authority>(d\')?[A-ZÁ][a-zA-Z\-öáñ\.èç]+)$',
        r'^(?P<original_name>[A-ZÑ][a-zëöiï]+( \([A-Z][a-z]+\))?( [a-z]{3,}){1,2}) (?P<authority>(d\'|de la )?[A-ZÁ][a-zA-Z\-öáéèíñç\.,\' ]+)$',
        r'^(?P<original_name>.*?) (?P<authority>[A-ZÉ]\. .*)$',
        r'^(?P<original_name>[A-ZÑ][a-zëöíï]+) (?P<authority>(d\'|de la )?[A-ZÁ][a-zA-Z\-öáéíñ\., ]+ and [A-ZÁ][a-zA-Z\-öáéèíñç]+)$',
    ]
    if try_harder:
        regexes += [
            r'^(?P<original_name>.* [a-zë\-]+) (?P<authority>[A-ZÁÉ].*)$',
            r'^(?P<original_name>.*) (?P<authority>[^ ]+)$',
        ]
    for rgx in regexes:
        match = re.match(rgx, name_authority)
        if match:
            return match.groupdict()
            break
    else:
        if not quiet:
            print(name_authority)
        return {}


def split_fields(names: DataT, refs_dict: RefsDictT) -> DataT:
    for name in names:
        if 'has_colon' in name or 'is_informal' in name:
            continue  # we're not interested in name combinations
        text = name['rest']
        if text:
            text = text.rstrip('.')
            if text == 'part' or text.startswith('part; not '):
                continue
            if text == 'nomen nudum':
                name['nomenclature_status'] = constants.NomenclatureStatus.nomen_nudum
            match = re.search(r'(preoccupied by|incorrect subsequent spelling( of)?(, but not)?|unjustified emendation of|replacement name for) ([^;\d=]+?)(, \d{4}|;|$| \(preoccupied\)|, on the assumption)', text)
            if match:
                name['variant_kind'] = {
                    'preoccupied by': constants.NomenclatureStatus.preoccupied,
                    'incorrect subsequent spelling': constants.NomenclatureStatus.incorrect_subsequent_spelling,
                    'incorrect subsequent spelling of': constants.NomenclatureStatus.incorrect_subsequent_spelling,
                    'incorrect subsequent spelling of, but not': constants.NomenclatureStatus.incorrect_subsequent_spelling,
                    'unjustified emendation of': constants.NomenclatureStatus.unjustified_emendation,
                    'replacement name for': constants.NomenclatureStatus.nomen_novum,
                }[match.group(1)]
                name_authority = split_name_authority(match.group(4), quiet=True)
                if name_authority:
                    name['variant_name'] = name_authority['original_name']
                    name['variant_authority'] = name_authority['authority']
                else:
                    name['variant_name_author'] = match.group(2)

            if text.startswith('type locality'):
                name['loc'] = text[len('type locality '):]
            elif text.startswith('type localities'):
                name['loc'] = text[len('type localities '):]
            elif 'localit' in text:
                name['loc'] = text
            elif text.startswith('type species'):
                match = re.match(r'type species (.*?), (\d{4}[a-z]?), ?by (.*)(;|$)', text)
                if match:
                    name_authority = split_name_authority(match.group(1), quiet=True)
                    if name_authority:
                        name['type_name'] = name_authority['original_name']
                        name['type_authority'] = name_authority['authority']
                    else:
                        name['type_name_author'] = match.group(1)
                    name['type_year'] = match.group(2)
                    type_kind = match.group(3)
                    if type_kind == 'monotypy':
                        name['genus_type_kind'] = constants.TypeSpeciesDesignation.monotypy
                    elif type_kind == 'original designation':
                        name['genus_type_kind'] = constants.TypeSpeciesDesignation.original_designation
                    elif type_kind == 'tautonymy':
                        name['genus_type_kind'] = constants.TypeSpeciesDesignation.absolute_tautonymy
                else:
                    name['raw_type_species'] = text

        if not any(field in name for field in ('variant_kind', 'loc', 'raw_type_species', 'type_year', 'nomenclature_status')):
            pass  # print(text)

        key = name['authority'], name['year']
        if key in refs_dict:
            name['verbatim_citation'] = refs_dict[key]
        else:
            # yield (key, name['raw_text'], name['name_authority'])
            pass
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.align_columns(pages)
    names_refs = extract_names(pages)
    names_refs = lib.clean_text(names_refs)
    names = list(itertools.takewhile(lambda n: n['t'] == 1, names_refs))
    refs = names_refs
    refs_dict = build_refs_dict(refs)
    names = split_text(names)
    # for key, text, aut in sorted(split_fields(names, refs_dict)):
    #     print(key, text, aut)
    names = split_fields(names, refs_dict)
    names = lib.translate_to_db(names, 'UU', SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    config = lib.NameConfig(original_name_fixes={
        'Vampyrops caraccioli': 'Vampyrops Caracciolae',
        'Vespertilio ricketii': 'Vespertilio (Leuconoë) Ricketti',
        'Didelphys iheringi': 'Didelphys (Peramys) Iheringi',
        'Didelphys henseli': 'Didelphys (Peramys) Henseli',
        'T. laephotis': 'Tonatia laephotis',
        'Artibeus rosenbergi': 'Artibeus (Dermanura?) Rosenbergi',
        'L. ega': 'Nycticejus Ega',
        'V[sic, = D(idelphis).]. Flavescens': 'Didelphis Flavescens',
        'Metachirus melanurus': 'Metachirus opossum melanurus',
        'D. opossum': 'Didelphis Opossum',
        '[Glossophaga (]Ch[oeronycteris)]. peruana': 'Glossophaga (Choeronycteris) peruana',
    }, authority_fixes={
        'Zimmerman': 'Zimmermann',
        'Menegaux': 'Ménègaux',
        'WiedNeuwied': 'Wied-Neuwied',
        'I. Geoffroy St.Hilaire': 'I. Geoffroy Saint-Hilaire',
        'É. Geoffroy St.Hilaire': 'É. Geoffroy Saint-Hilaire',
        'É. Geoffroy St.-Hilaire': 'É. Geoffroy Saint-Hilaire',
        'Quay and Gaimard': 'Quoy & Gaimard',
        'Albuja & Gardner': 'Albuja V. & Gardner',
        'Muñoz, Cuartas & González': 'Muñoz, Cuartas-Calle & González',
        'Muchhala, Mena & Albuja': 'Muchhala, Mena V. & Albuja V.',
    }, ignored_names={
        ('Gamba palmata', 'Liais'),
        ('Diphylla ecaudata', 'Dobson'),
        ('Noctula serotina', 'Bonaparte'),
        ('Vespertilio murinus', 'Schreber'),
        ('Peramys brachyurus: Lesson, 1842:187 (= Didelphys dimidiata', 'J. A. Wagner'),
        ('Amblyotus', 'Amyot and Servill'),
        ('Mus araneus', 'Marcgraf'),
        ('Sorex Brasiliensis Daudin in', 'Lacépède'),
        ('Tolypoı̈des Grandidier and', 'Neveu-Lemaire'),
        ('Vespertilio', 'Schreber'),
        ('Phyllostoma', 'É. Geoffroy Saint-Hilaire'),
        ('Rhinolophus', 'Schinz'),
        ('Chœronycteris', 'O. Thomas'),
        ('Dolichophyllum', 'Lydekker'),
        ('Bradypus tridactylus', 'Brasiliensis Blainville'),
        ('Myrmecophaga tamandua', 'Var. Mexicana Saussure'),
    })
    names = lib.associate_types(names, config)
    names = lib.associate_variants(names, config)
    names = lib.associate_names(names, config)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # for name in names:
    #     print(name)
    lib.print_field_counts(names)
    print(f'{len(refs_dict)} refs')


if __name__ == '__main__':
    main()
