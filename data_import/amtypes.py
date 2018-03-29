import re
from typing import Any, Dict, List, Optional

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source('amtypes.txt', 'AM-types.pdf')


def extract_names(pages: PagesT) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: Dict[str, Any] = {}
    current_label: Optional[str] = None
    current_lines: List[str] = []
    in_figure = False

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert label not in current_name, f'duplicate label {label} for {current_name}'
        current_label = label
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        if current_name is not None:
            current_name['pages'].append(page)
        for line in lines:
            line = line.rstrip()
            if re.match(r'^ *Figure \d+\. ', line):
                in_figure = True
                continue
            if not line:
                in_figure = False
                last_line_was_blank = True
                continue
            if in_figure:
                continue
            if re.match(r'\s+(Order|Family) +[A-Z][a-z]+$', line):
                continue
            if line.startswith(' ') and last_line_was_blank and current_label != 'name':
                if current_name:
                    assert 'Comments' in current_name, f'missing comments for {current_name} on page {page}'
                    yield current_name
                current_name = {
                    'pages': [page],
                }
                start_label('name', line)
            else:
                match = re.match(r'^(\??[A-Z][a-z]+( [a-z]+)?)\. ', line)
                if match:
                    label = match.group(1)
                    if 'type' in label or 'name' in label or label in ('Comments', 'Type locality', 'Condition', 'Material'):
                        start_label(match.group(1), line)
                    else:
                        current_lines.append(line)
                else:
                    current_lines.append(line)
            last_line_was_blank = False
    yield current_name


def split_fields(names: DataT, verbose: bool = False) -> DataT:
    for name in names:
        name['raw_text'] = dict(name)
        text = name['name']
        match = re.match(r'^(?P<orig_name_author>\D*) (?P<year>\d{4})[a-z]? (?P<verbatim_citation>.*)$', text)
        if match:
            for k, v in match.groupdict().items():
                name[k] = v.rstrip(',')
        else:
            print(f'failed to match {text}')
        if 'Holotype' in name:
            name['species_type_kind'] = constants.SpeciesGroupType.holotype
            name['specimen_detail'] = name['Holotype']
        elif 'Lectotype' in name:
            name['species_type_kind'] = constants.SpeciesGroupType.lectotype
            name['specimen_detail'] = name['Lectotype']
        elif 'Syntypes' in name:
            name['species_type_kind'] = constants.SpeciesGroupType.syntypes
            name['specimen_detail'] = name['Syntypes']
        elif 'Neotype' in name:
            name['species_type_kind'] = constants.SpeciesGroupType.neotype
            name['specimen_detail'] = name['Neotype']
        if 'Type locality' in name:
            name['loc'] = name['Type locality']
        if 'specimen_detail' in name and 'Syntypes' not in name:
            text = name['specimen_detail']
            match = re.match(r'^(?P<type_specimen>[A-Z]{1,2}\.\d+).*?\. (?P<data>[^\.]+)', text)
            if not match:
                if verbose:
                    print(f'failed to match {text}')
            else:
                name['type_specimen'] = f'AM {match.group("type_specimen")}'
                parts = match.group('data').split(', ')
                tags: List[models.TypeTag] = []
                for part in parts:
                    part = re.sub(r'[\(\[].*$', '', part.lower()).strip()
                    if not part:
                        continue
                    for enum, tag in (
                        (constants.SpecimenGender, models.TypeTag.Gender),
                        (constants.SpecimenAge, models.TypeTag.Age),
                        (constants.Organ, models.TypeTag.Organ),
                    ):
                        if lib.enum_has_member(enum, part):
                            enum_member: Any = enum[part]
                            if enum is constants.Organ:
                                tags.append(models.TypeTag.Organ(enum_member, '', ''))
                            else:
                                tags.append(tag(enum_member))
                            break
                    else:
                        if part in ('unknown sex', 'sex not determined'):
                            tags.append(models.TypeTag.Gender(constants.SpecimenGender.unknown))
                        elif part == 'body in alc':
                            tags.append(models.TypeTag.Organ(constants.Organ.in_alcohol, '', ''))
                        elif part in ('study skin', 'flat skin only', 'flat skin'):
                            tags.append(models.TypeTag.Organ(constants.Organ.skin, '', ''))
                        elif part in ('skin mounted', 'skin mount'):
                            tags.append(models.TypeTag.Organ(constants.Organ.skin, '', 'mounted'))
                        elif part in ('female adult', 'adult female'):
                            tags.append(models.TypeTag.Gender(constants.SpecimenGender.female))
                            tags.append(models.TypeTag.Age(constants.SpecimenAge.adult))
                        elif part in ('male adult', 'adult male'):
                            tags.append(models.TypeTag.Gender(constants.SpecimenGender.male))
                            tags.append(models.TypeTag.Age(constants.SpecimenAge.adult))
                        elif verbose:
                            print(f'do not know how to parse {part!r}')
                name['type_tags'] = tags
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    pages = lib.align_columns(pages, single_column_pages={324, 326, 330, 339, 363})
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'AM', SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for p in main():
        print(p)
