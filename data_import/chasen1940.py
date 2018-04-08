import re
from typing import List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source('chasen1940.txt', 'Greater Sundas (Chasen 1940).pdf')


def extract_names(pages: PagesT) -> DataT:
    current_name: List[str] = []
    starting_page = 0
    in_distribution = False
    for page, lines in pages:
        lines = lib.dedent_lines(lines)
        for line in lines:
            line = line.rstrip()
            if re.match(r'^ +1( |$)', line):
                break
            if not line:
                continue
            leading_spaces = lib.initial_count(line, ' ')
            if leading_spaces < 2 or re.match(r'^ +Distr\.—', line):
                if current_name:
                    yield {'raw_text': current_name, 'pages': [starting_page]}
                    current_name = []
                in_distribution = bool(re.match(r'^ +Distr..?—', line))
            elif in_distribution:
                continue
            elif not current_name and ',' in line:
                current_name = [line]
                starting_page = page
            elif current_name:
                current_name.append(line)


def split_names(names: DataT) -> DataT:
    for name in names:
        seen_colon = False
        seen_period = False
        current_name: List[str] = []
        last_indentation = 0
        for line in name['raw_text']:
            indentation = lib.initial_count(line, ' ')
            if seen_period:
                if not current_name or indentation <= last_indentation - 2:
                    if current_name:
                        yield {'raw_text': current_name, 'pages': name['pages']}
                    current_name = [line]
                else:
                    current_name.append(line)
            else:
                current_name.append(line)
                if ':' in line:
                    seen_colon = True
                if seen_colon and re.search(r'\.\d*$', line):
                    seen_period = True
                    yield {'raw_text': current_name, 'pages': name['pages']}
                    current_name = []

            last_indentation = lib.initial_count(line, ' ')
        if current_name:
            yield {'raw_text': current_name, 'pages': name['pages']}


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name['raw_text']
        match = re.match(r'^(?P<orig_name_author>[^\d,]+)(, | in )(?P<verbatim_citation>.*?): (?=[A-Z"\(nbrwmotfupsld\?])(?P<loc>.*)$', text)
        if match:
            for k, v in match.groupdict().items():
                if v:
                    name[k] = v
            name['orig_name_author'] = re.sub(r'^\? ', '', name['orig_name_author'])
        else:
            print(f'failed to match {text}')
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = split_names(names)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'USNM', SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, name_config=lib.NameConfig(authority_fixes={
        'Thos.': 'Thomas',
        'Bonh.': 'Bonhote',
        'G. Cuv.': 'G. Cuvier',
        'F. Cuv.': 'F. Cuvier',
        'Linn.': 'Linnaeus',
        'Rob.': 'Robinson',
        'Kl.': 'Kloss',
        'Jent.': 'Jentink',
        'Less.': 'Lesson',
        'Horsf.': 'Horsfield',
        'And.': 'Andersen',
        'Mill.': 'Miller',
        'Wrought.': 'Wroughton',
        'Ell.': 'Elliot',
        'Günth.': 'Günther',
        'Blainv.': 'Blainville',
        'Desm.': 'Desmarest',
        'Geoff.': 'Geoffroy',
        'Temm.': 'Temminck',
        'Cabrer.': 'Cabrera',
        'Elliott': 'Elliot',
        'Gunther': 'Günther',
        'S. Müll.': 'S. Müller',
        'Müll.': 'Müller',
        'Schleg.': 'Schlegel',
        'Fitz.': 'Fitzinger',
        'Gyld.': 'Gyldenstolpe',
        'Kohl.': 'Kohlbrugge',
        'E. Geoff': 'É. Geoffroy Saint-Hilaire',
        'Is. Geoff': 'Is. Geoffroy Saint-Hilaire',
    }), try_manual=True, start_at='Simia Nemestrina')
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for p in main():
        print(p)
