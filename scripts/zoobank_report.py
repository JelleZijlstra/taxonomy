import argparse
import re
from typing import IO

from taxonomy import getinput
from taxonomy.db.models import Article, Name
from taxonomy.db.models.article.article import ArticleTag, PresenceStatus
from taxonomy.db.models.name import TypeTag


def string_for_status(status: PresenceStatus) -> str:
    match status:
        case PresenceStatus.absent:
            return "not in article"
        case PresenceStatus.present:
            return "present in article"
        case PresenceStatus.inferred:
            return (
                "not in article, but the article contains a name LSID that maps to this"
                " publication LSID"
            )
        case _:
            assert False, "should not happen"


def is_present(art: Article, lsid: str) -> PresenceStatus:
    pages = art.get_all_pdf_pages()
    cleaned_text = "".join(
        re.sub(r"\s", "", page).replace("-", "-").casefold() for page in pages
    ).replace("-", "")
    if lsid.casefold().replace("-", "") in cleaned_text:
        return PresenceStatus.present
    else:
        return PresenceStatus.absent


def run_names(output_f: IO[str]) -> None:
    print("## Names", file=output_f)
    print(file=output_f)
    query = Name.select_valid().filter(
        Name.type_tags.contains(f"[{TypeTag.LSIDName._tag},")
    )
    for nam in getinput.print_every_n(query, label="names"):
        tags = list(nam.get_tags(nam.type_tags, TypeTag.LSIDName))
        if len(tags) < 2 or nam.original_citation is None:
            continue
        nam.edit_until_clean()
        tags = list(nam.get_tags(nam.type_tags, TypeTag.LSIDName))
        if len(tags) < 2:
            continue
        print(f"- {str(nam)}", file=output_f)
        for tag in tags:
            presence = is_present(nam.original_citation, tag.text)
            print(
                (
                    f"  - urn:lsid:zoobank.org:act:{tag.text} –"
                    f" {string_for_status(presence)}"
                ),
                file=output_f,
            )


def run_articles(output_f: IO[str]) -> None:
    print("## References", file=output_f)
    print(file=output_f)
    for art in getinput.print_every_n(
        Article.select_valid().filter(Article.tags != None), label="articles"
    ):
        tags = list(art.get_tags(art.tags, ArticleTag.LSIDArticle))
        if len(tags) < 2:
            continue
        art.edit_until_clean()
        tags = list(art.get_tags(art.tags, ArticleTag.LSIDArticle))
        if len(tags) < 2:
            continue
        print(f"- {art.cite()}", file=output_f)
        for tag in tags:
            print(
                (
                    f"  - urn:lsid:zoobank.org:pub:{tag.text} –"
                    f" {string_for_status(tag.present_in_article)}"
                ),
                file=output_f,
            )


def run(filename: str) -> None:
    with open(filename, "w") as f:
        run_articles(f)
        print(file=f)
        run_names(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_file")
    args = parser.parse_args()
    run(args.output_file)
