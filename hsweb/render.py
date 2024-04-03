import csv
import re
from collections.abc import Callable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path

import clorm

from taxonomy.db.models import Article
from taxonomy.db.models.base import BaseModel

CALL_SIGN_TO_MODEL = {model.call_sign: model for model in BaseModel.__subclasses__()}
DOCS_ROOT = Path(__file__).parent.parent / "docs"


def render_plain_text(text: str) -> str:
    return text.replace("-\\ ", "- ")


def render_row(
    row: Iterable[str], col_widths: Sequence[int], fill_char: str = " "
) -> str:
    cells = [cell.ljust(col_widths[i], fill_char) for i, cell in enumerate(row)]
    return "| " + " | ".join(cells) + " |\n"


def gould_table() -> str:
    lines = []
    with (DOCS_ROOT / "biblio" / "gould-mammals.csv").open("r") as f:
        rows = list(csv.DictReader(f))
    headings = [*rows[0]]

    col_widths = [len(heading) for heading in headings]
    for row in rows:
        for i, cell in enumerate(row.values()):
            col_widths[i] = max(col_widths[i], len(cell))
    lines.append(render_row(headings, col_widths))
    lines.append(render_row(["" for _ in headings], col_widths, "-"))
    for row in rows:
        lines.append(render_row(row.values(), col_widths))
    return "".join(lines)


MD_FUNCTIONS: dict[str, Callable[[], str]] = {"gould_table": gould_table}


def _match_to_md_ref(match: re.Match[str]) -> str:
    ref = match.group(1)
    if ref.startswith(":"):
        md_function = MD_FUNCTIONS.get(ref[1:])
        if md_function is None:
            return match.group()
        return md_function()
    full = ref.endswith("!r")
    if full:
        ref = ref.removesuffix("!r")
    if "/" in ref:
        call_sign, rest = ref.split("/", maxsplit=1)
        try:
            model_cls = CALL_SIGN_TO_MODEL[call_sign.upper()]
        except KeyError:
            return match.group()
        if rest.isnumeric():
            try:
                obj = model_cls.get(id=int(rest))
            except clorm.DoesNotExist:
                return match.group()
        elif not model_cls.label_field:
            return match.group()
        else:
            field = getattr(model_cls, model_cls.label_field)
            try:
                obj = model_cls.select().filter(field == rest).get()
            except clorm.DoesNotExist:
                return match.group()
    else:
        try:
            obj = Article.select().filter(Article.name == ref).get()
        except clorm.DoesNotExist:
            return match.group()
    obj = obj.resolve_redirect()
    return obj.markdown_link() if full else obj.concise_markdown_link()


@lru_cache(8192)
def render_markdown(text: str) -> str:
    """Turn '{x.pdf}' into '[A & B (2016](/a/123)'."""
    text = render_plain_text(text)
    text = re.sub(r"\{([^}]+)\}", _match_to_md_ref, text)
    text = re.sub(r" @$", " [brackets original]", text)
    return text
