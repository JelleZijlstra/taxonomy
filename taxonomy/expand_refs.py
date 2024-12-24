"""

Usage:

python -m taxonomy.expand_refs path/to/file [path/to/output]

"""

import argparse
import re
from functools import partial
from pathlib import Path

from taxonomy.db import helpers
from taxonomy.db.models import Article
from taxonomy.db.models.name.name import Name


def get_article(name: str) -> Article | None:
    return Article.getter("name")(name)


def resolve_name(label: str) -> Name:
    if label.isnumeric():
        return Name(int(label))
    options = list(Name.select_valid().filter(Name.corrected_original_name == label))
    if not options:
        raise ValueError(f"No name found for label {label!r}")
    elif len(options) > 1:
        for nam in options:
            nam.display(full=False)
        raise ValueError(f"Multiple names found for label {label!r}")
    return options[0]


def make_usage_list(match: re.Match[str], *, style: str) -> str:
    name = resolve_name(match.group(1))
    return name.make_usage_list(style=style)


def expand(input_text: str) -> str:
    refs = set()
    output_text = input_text
    for article_name in helpers.extract_sources(input_text):
        article = get_article(article_name)
        if article is None:
            raise ValueError(f"Article {article_name!r} not found")
        refs.add(article)
        output_text = output_text.replace(f"{{{article_name}}}", "")

    match = re.search(r"<reflist ([a-z_]+)>", output_text)
    if match is None:
        raise ValueError("No reflist found")
    style = match.group(1)
    ref_texts = [f"* {article.cite(style)}\n" for article in refs]
    output_text = output_text.replace(match.group(0), "".join(sorted(ref_texts)))
    output_text = re.sub(
        r"<usagelist (\d+|[A-Za-z ]+)>",
        partial(make_usage_list, style=style),
        output_text,
    )
    return output_text


def main(args: argparse.Namespace) -> None:
    input_text = args.input.read_text()
    output_text = expand(input_text)
    if args.output:
        args.output.write_text(output_text)
    else:
        print(output_text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Path to file containing references", type=Path)
    parser.add_argument("output", nargs="?", help="Path to output file", type=Path)
    main(parser.parse_args())
