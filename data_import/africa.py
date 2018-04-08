import re
from typing import Counter, List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source('africa-layout.txt', 'Africa.pdf')


def extract_names(pages: PagesT) -> DataT:
    current_name: List[str] = []
    starting_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            leading_spaces = lib.initial_count(line, ' ')
            if current_name:
                if leading_spaces < 3 or re.match(r'^ {6,}(Order|Family|Subfamily|Subgenus) +[A-Z]{2,}', line):
                    # flush the active name
                    yield {'raw_text': current_name, 'pages': [starting_page]}
                    current_name = []
                else:
                    current_name.append(line)
            if leading_spaces == 0 and re.match(r'^[A-Z][a-z\[\]]{2,} .*,', line):
                # perhaps a new name
                starting_page = page
                current_name = [line]


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name['raw_text']
        if '$' in text:
            head, tail = text.split('$', maxsplit=1)
            if tail.strip():
                name['rest'] = tail.strip()
            match = re.match(r'^(?P<orig_name_author>[^,]+(, +var\.[^,]+|(?<=[A-Z]), [^,]+)?), ?'
                             r'(?P<verbatim_citation>.+?\d{4}(, in part|, characters given| \([a-zA-Z\.\' ]+\))?)'
                             r'(; (?P<nomenclature>[^;]+))?\.?$', head.strip())
            if match:
                for k, v in match.groupdict().items():
                    if v:
                        name[k] = v
            else:
                print(f'failed to match {text}')
        else:
            match = re.match(r'^(?P<orig_name_author>[^,]+(, +var\.[^,]+|(?<=[A-Z]), [^,]+)?), ?'
                             r'(?P<verbatim_citation>.+?\d{4}(, in part|, characters given| \([a-zA-Z\.\' ]+\))?)'
                             r'(; (?P<nomenclature>[^\.\(\)]+|\([^\)]+\)))?\.(?P<rest>.*)$', text)
            if match:
                for k, v in match.groupdict().items():
                    if v:
                        name[k] = v.strip()
            else:
                print(f'failed to match {text}')
        yield name


def translate_rest(names: DataT) -> DataT:
    for name in names:
        if 'rest' in name:
            if ' ' in name['original_name']:
                name['loc'] = name['rest']
            else:
                name['verbatim_type'] = name['rest']
        else:
            name['name_quiet'] = True
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'USNM', SOURCE, verbose=True)
    names = translate_rest(names)
    names = lib.translate_to_db(names, 'USNM', SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=True, edit_if_no_holotype=False)
    # for text in sorted(name['rest'] for name in names if 'rest' in name):
    #     print(text)
    # places = Counter()
    # for name in names:
    #     if 'loc' in name and 'type_locality' not in name:
    #         place = name['loc'].split(', ')[-1].strip('.').strip()
    #         places[place] += 1
    # for name in names:
    #     if 'type_locality' in name:
    #         places[name['type_locality']] += 1
    # for place, count in places.most_common():
    #     print(count, place)
    authors: Counter[str] = Counter()
    for name in names:
        if 'name_obj' not in name and 'authority' in name:
            authors[name['authority']] += 1
    for author, count in authors.most_common():
        print(count, author)
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for p in main():
        print(p)
