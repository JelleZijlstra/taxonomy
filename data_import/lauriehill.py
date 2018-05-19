r"""TODO:

- For pages, look for lines that either are just \d+ or that are \s{a lot}\d+$
- Look for lines that start with "\d{4}\. ". Extract name, author, year, verbatim_cit, type loc, "Range: ".
- Ignore everything else (no attempt to understand synonymy)

"""
import re
from typing import Iterable, List, Tuple

from . import lib
from .lib import DataT

SOURCE = lib.Source(
    "lauriehill-layout.txt", "New Guinea, Sulawesi (Laurie & Hill 1954).pdf"
)
NAME_RGX = re.compile(
    r"""
    (\(\?\)\s+)?
    (?P<year>\d{4})\.\s
    (?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-zäü]+\??\))?((\s\(\?\)|\svar\.(\s[A-Z]\.?)?|\saff\.|\sab\.\sloc\.)?\s[a-züä\-]{3,}){0,2})\s
    (?P<authority>(de\s)?[A-Z][a-zA-Zü\s&\.\-']+),\s
    (?P<verbatim_citation>.*([\d\)]\.\)?|text-fig\.|text-f\.|letterpress\.|Vespertilio\semarginatus\.|fig\.\s[A-Za-z]\.|[a-k]-[a-k]\.|\d[a-d]\.|a,\sb\.|with\splate\.|footnote\.|under\sVespertilio\semarginatus))(\s
    (
        (?P<loc>(Type\slocality)?(?!Type).+\."?)(\s(Range[:;]?|Extralimital\ssynonyms)\s.*\.|\s\([^\)]+\.\))?
        |Type\s(?P<type_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?\s[a-z-]+(\s[a-z]{3,})?)\s(?P<type_authority>(de\s)?([A-Z]\.\s)?[A-Z][a-zA-Zü\s,&-]+)[\.=](\s.*\.)?
    ))?(\s[:;])?$
""",
    re.VERBOSE,
)


def extract_pages(lines: Iterable[str]) -> Iterable[Tuple[int, List[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines: List[str] = []
    for line in lines:
        if line.startswith("\x0c"):
            last_line = current_lines.pop().strip()
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            if last_line.isnumeric():
                current_page = int(last_line) + 1
            else:
                match = re.search(r" {10,}(\d+)$", last_line)
                if match:
                    current_page = int(match.group(1)) + 1
                else:
                    assert (
                        False
                    ), f"failed to match {last_line!r} (current page {current_page})"
        else:
            current_lines.append(line)
    # last page
    assert current_page is not None
    yield current_page, current_lines


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    current_lines: List[str] = []
    current_pages: List[int] = []
    for page, lines in pages:
        if current_pages:
            current_pages.append(page)
        for line in lines:
            line = line.rstrip()
            if current_lines and line.startswith(" "):
                current_lines.append(line)
            else:
                if current_lines:
                    yield {"raw_text": current_lines, "pages": current_pages}
                    current_lines = []
                if re.search(r"^(\(\?\)\s+)?\d{4}\. ", line):
                    current_lines = [line]
                    current_pages = [page]


def split_fields(names: DataT) -> DataT:
    tried = succeeded = 0
    for name in names:
        tried += 1
        text = name["raw_text"]
        match = NAME_RGX.match(text)
        if not match:
            print(f'failed to match {text} (pages {name["pages"]})')
        else:
            succeeded += 1
            for group, value in match.groupdict().items():
                if value is not None:
                    name[group] = value
        yield name
    print(f"success: {succeeded}/{tried}")


def translate_type_localities(names: DataT) -> DataT:
    for name in names:
        # hack
        if "original_name" in name:
            if "kalubu" in name["original_name"]:
                name["authority"] = "Fischer"
            elif name["original_name"] == "Vespertilio muricola":
                name["authority"] = "Gray"
            elif name["original_name"] == "Sus verrucosus":
                name["authority"] = "Boie"
            elif name["original_name"] in ("Sciurus leucomus", "Sciurus rubriventer"):
                name["authority"] = "Müller & Schlegel"
        if "type_name" in name:
            if "kalubu" in name["type_name"]:
                name["type_authority"] = "Fischer"

        if "loc" in name:
            loc = name["loc"].rstrip(".")
            loc = re.sub(r"\. Range[;: ].*$", "", loc)
            loc = re.sub(r", [\d,-]+ (ft|metres|feet)$", "", loc)
            loc = re.sub(r", sea level$", "", loc)
            parts = [[part] for part in reversed(loc.split(", "))]
            type_loc = lib.extract_region(parts)
            if type_loc is not None:
                name["type_locality"] = type_loc
            # else:
            #     print('could not extract type locality from', name['loc'])
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = extract_pages(lines)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "BMNH", SOURCE)
    names = translate_type_localities(names)
    author_fixes = {
        "F. Cuvier ": "F.G. Cuvier & Geoffroy",
        "Shaw & Nodder": "Shaw",
        "Dubois": "DuBois",
        "Muller": "Müller",
        "Muller & Schlegel": "Müller & Schlegel",
        "Rummler": "Rümmler",
        "Schlegel & Muller": "Schlegel & Müller",
        "Rutimeyer": "Rütimeyer",
        "Forster & Rothschild": "Förster & Rothschild",
        "Grey": "Gray",
        "Gunther": "Günther",
        "Mueller": "Müller",
        "Lacepede": "Lacépède",
        "Zimmerman": "Zimmermann",
        "Bemmel": "Van Bemmel",
        "de Vis": "De Vis",
        "Peron": "Lesueur & Petit",
        "Deniger": "Deninger",
        "Horst & de Raadt": "Horst & De Raadt",
    }
    original_name_fixes = {
        "Nyctinomus johorensis": "Molossus (Nyctinomus) johorensis",
        "Macroglossus australis": "Macroglossus minimus var. australis",
        "Cynopterus nigrescens": "Cynopterus marginatus var. nigrescens",
        "Podabrus crassicaudatus": "Phascogale crassicaudata",
        "Vespertilio niger": "Vespertilio vampirus niger",
        "Babirussa frosti": "Babirussa babyrussa frosti",
        "Lemur spectrum": "Simia spectrum",
    }
    config = lib.NameConfig(author_fixes, original_name_fixes)
    names = lib.associate_types(names, config)
    names = lib.associate_names(names, config, start_at="Babirusa celebensis")
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # lib.print_counts(names, 'type_locality')
    lib.print_field_counts(names)
    list(names)
    return names


if __name__ == "__main__":
    for _ in main():
        print(_)
