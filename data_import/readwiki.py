import functools
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

import requests
import wikitextparser

from taxonomy import getinput
from taxonomy.db import helpers
from taxonomy.db.models import Article

CACHE_DIR = Path(__file__).parent / "wikicache"
EXCLUDED_HEADERS = {
    "General research",
    "Research",
    "Plants",
    "Sponges",
    "Cnidarians",
    "Arthropods",
    "Bryozoans",
    "Brachiopods",
    "Molluscs",
    "Echinoderms",
    "Conodonts",
    "Fish",
    "Fishes",
    "Other animals",
    "History of life in general",
    "Foraminifera",
    "Other organisms",
    "General paleontology",
    "Relevant research in other sciences",
    "Bony fish",
    "Cartilaginous fish",
    "Cephalopods",
    "Protozoa",
    "Arthropoda",
    "Related happenings in geology",
}
ONLY_IN_TABLE = True


@dataclass
class Template:
    template_name: str
    args: dict[str, str]
    heading: str | None
    subheading: str | None
    in_table: bool

    @classmethod
    def from_wtp_template(
        cls,
        template: wikitextparser.Template,
        heading: str | None,
        subheading: str | None,
        in_table: bool,
    ) -> "Template":
        return cls(
            template.name.strip().lower(),
            {arg.name.strip(): arg.value.strip() for arg in template.arguments},
            heading.strip() if heading else None,
            subheading.strip() if subheading else None,
            in_table,
        )


def get_text(name: str, language: str = "en") -> str:
    name = name.replace(" ", "_")
    cache_file = CACHE_DIR / name
    if cache_file.exists():
        return cache_file.read_text()
    result = requests.get(
        f"https://{language}.wikipedia.org/w/index.php?title={name}&action=raw"
    )
    cache_file.write_text(result.text)
    return result.text


def get_templates_wtp(text: str) -> Iterable[Template]:
    parsed = wikitextparser.parse(text)
    seen_templates: set[str] = set()
    for section in parsed.sections:
        heading = section.title
        for subsection in section.sections:
            if subsection.title == heading:
                subheading = None
            else:
                subheading = subsection.title
            for table in subsection.tables:
                for template in table.templates:
                    if template.string not in seen_templates:
                        yield Template.from_wtp_template(
                            template, heading, subheading, in_table=True
                        )
                        seen_templates.add(template.string)

            for template in subsection.templates:
                if template.string not in seen_templates:
                    yield Template.from_wtp_template(
                        template, heading, subheading, in_table=False
                    )
                    seen_templates.add(template.string)


def should_include(template: Template) -> bool:
    if ONLY_IN_TABLE and not template.in_table:
        return False
    if template.heading in EXCLUDED_HEADERS or template.subheading in EXCLUDED_HEADERS:
        return False
    if not template.template_name.startswith("cite"):
        return False
    return True


def simplify_string(text: str) -> str:
    text = (
        text.replace("<i>", "")
        .replace("</i>", "")
        .replace("''", "")
        .replace(" :", "")
        .lower()
    )
    return helpers.clean_string(text)


@functools.lru_cache
def get_dois() -> set[str]:
    articles = Article.select_valid().filter(Article.doi != None)
    return {article.doi.lower() for article in articles}


@functools.lru_cache
def get_titles() -> set[tuple[str, str]]:
    articles = Article.select_valid()
    return {
        (simplify_string(article.title or ""), article.year or "")
        for article in articles
    }


def is_already_present(template: Template) -> bool:
    dois = get_dois()
    titles = get_titles()
    if "doi" in template.args:
        doi = template.args["doi"]
        if doi.lower() in dois:
            return True
    if "chapter" in template.args and "year" in template.args:
        key = simplify_string(template.args["chapter"]), template.args["year"]
        if key in titles:
            return True
    if "title" in template.args and "year" in template.args:
        key = simplify_string(template.args["title"]), template.args["year"]
        if key in titles:
            return True
    return False


def process_article(*names: str, clear_caches: bool = False) -> None:
    if clear_caches:
        get_dois.cache_clear()
        get_titles.cache_clear()
    templates = chain.from_iterable(get_templates_wtp(get_text(name)) for name in names)
    for template in sorted(
        templates,
        key=lambda template: (
            template.args.get("journal", ""),
            template.args.get("author1", ""),
            template.args.get("title", ""),
        ),
    ):
        if not should_include(template):
            continue
        if is_already_present(template):
            print("Already present:", template)
            continue
        getinput.print_header(template.args.get("title", "(no title)"))
        handle_template(template)


def handle_template(template: Template) -> None:
    print(template, flush=True)
    if "journal" in template.args:
        print("Journal:", template.args["journal"])
    options = [
        "",
        "q",
        "quit",
        "d",
        "doi",
        "b",
        "berkeley",
        "u",
        "url",
        "s",
        "scihub",
        "p",
        "print",
    ]
    while True:
        command = getinput.get_with_completion(
            options, message="> ", history_key="readwiki", disallow_other=True
        )
        if command in ("", "q", "quit"):
            return
        elif command in ("d", "doi"):
            if "doi" in template.args:
                subprocess.run(["open", f"http://dx.doi.org/{template.args['doi']}"])
            else:
                print("no doi", flush=True)
        elif command in ("u", "url"):
            if "chapter-url" in template.args:
                subprocess.run(["open", template.args["chapter-url"]])
            elif "url" in template.args:
                subprocess.run(["open", template.args["url"]])
            else:
                print("no url", flush=True)
        elif command in ("p", "print"):
            for key, value in sorted(template.args.items()):
                print(f"{key}: {value}", flush=True)
