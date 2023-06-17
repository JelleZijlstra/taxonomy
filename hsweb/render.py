import re
from functools import lru_cache

from taxonomy.db.models import Article
from taxonomy.db.models.base import BaseModel

CALL_SIGN_TO_MODEL = {model.call_sign: model for model in BaseModel.__subclasses__()}


def render_plain_text(text: str) -> str:
    return text.replace("-\\ ", "- ")


def _match_to_md_ref(match: re.Match[str]) -> str:
    ref = match.group(1)
    if "/" in ref:
        call_sign, rest = ref.split("/", maxsplit=1)
        try:
            model_cls = CALL_SIGN_TO_MODEL[call_sign.upper()]
        except KeyError:
            return match.group()
        if rest.isnumeric():
            try:
                obj = model_cls.get(id=int(rest))
            except model_cls.DoesNotExist:
                return match.group()
        elif not model_cls.label_field:
            return match.group()
        else:
            field = getattr(model_cls, model_cls.label_field)
            try:
                obj = model_cls.select().filter(field == rest).get()
            except model_cls.DoesNotExist:
                return match.group()
    else:
        try:
            obj = Article.select().filter(Article.name == ref).get()
        except Article.DoesNotExist:
            return match.group()
    return obj.resolve_redirect().concise_markdown_link()


@lru_cache(8192)
def render_markdown(text: str) -> str:
    """Turn '{x.pdf}' into '[A & B (2016](/a/123)'."""
    text = render_plain_text(text)
    text = re.sub(r"\{([^}]+)\}", _match_to_md_ref, text)
    text = re.sub(r" @$", " [brackets original]", text)
    return text
