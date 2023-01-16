import re
from typing import List

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("meesteretal.txt", "S Africa (Meester et al. 1986).pdf")


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    name_leading_spaces = 0
    starting_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            leading_spaces = lib.initial_count(line, " ")
            if name_leading_spaces and leading_spaces < name_leading_spaces + 4:
                # flush the active name
                yield {"raw_text": current_name, "pages": [starting_page]}
                current_name = []
                name_leading_spaces = 0
            if re.match(r"^(\? |c\. )? +\d{4} ?\. ", line):
                starting_page = page
                current_name = [line]
                name_leading_spaces = leading_spaces
            elif current_name:
                current_name.append(line)


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        if "$" in text:
            head, tail = text.split("$", maxsplit=1)
            if tail.strip():
                name["rest"] = tail.strip()
            match = re.match(
                r"^(?P<year>\d{4})\.\s+(?P<orig_name_author>[^,]+(,"
                r" +var\.[^,]+|(?<=[A-Z]), [^,]+)?), "
                r"(?P<verbatim_citation>.*)$",
                head.strip(),
            )
            if match:
                for k, v in match.groupdict().items():
                    if v:
                        name[k] = v
            else:
                print(f"failed to match {text}")
        else:
            match = re.match(
                r"^(?P<year>\d{4})\.\s+(?P<orig_name_author>[^,]+(,"
                r" +var\.[^,]+|(?<=[A-Z]), [^,]+)?(, Morrison-Scott & Hayman)?), "
                r"(?P<rest>.*)$",
                text,
            )
            if match:
                name["year"] = match.group("year")
                name["orig_name_author"] = match.group("orig_name_author")
                rest = match.group("rest")
                match = re.match(
                    r"(?P<verbatim_citation>.+?(: ?[,\d IBgo\-]+| (fig|pl)\. [I\d]+|"
                    r" and text|, footnote))"
                    r"\. (?P<rest>.*)$",
                    rest,
                )
                if match:
                    name.update(match.groupdict())
                elif re.search(r"\d+\.", rest):
                    name["verbatim_citation"] = rest
                else:
                    match = re.match(
                        r"^(?P<verbatim_citation>.*\d( \([^\)]+\))?)\. (?P<rest>.*)$",
                        rest,
                    )
                    if match:
                        name.update(match.groupdict())
                    else:
                        print(f"failed to match {text}")
            else:
                print(f"failed to match {text}")
        yield name


def translate_rest(names: DataT) -> DataT:
    for name in names:
        if "rest" in name:
            if " " in name["original_name"]:
                name["loc"] = name["rest"]
            else:
                name["verbatim_type"] = name["rest"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False, check=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = translate_rest(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, try_manual=True, start_at="Ichneumon dorsalis")
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
