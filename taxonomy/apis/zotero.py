import pprint
from collections.abc import Sequence
from functools import cache

from pyzotero import zotero

from taxonomy.config import get_options
from taxonomy.db.constants import ArticleType
from taxonomy.db.models import Taxon
from taxonomy.db.models.article.article import Article, ArticleTag
from taxonomy.db.models.citation_group.cg import CitationGroupTag
from taxonomy.db.models.person import Person

api_key = get_options().zotero_key
library_type = "group"
library_id = "5435545"  # "5620567"
ZIJLSTRA_COLLECTION = "J4J8T2B8"


def get_zotero() -> zotero.Zotero:
    return zotero.Zotero(library_id, library_type, api_key)


@cache
def get_existing_tags() -> set[str]:
    zot = get_zotero()
    tags: set[str] = set()
    for item in zot.everything(zot.collection_items(ZIJLSTRA_COLLECTION)):
        for tag in item["data"]["tags"]:
            if tag["tag"].startswith("https://"):
                tags.add(tag["tag"])
    return tags


def get_author(pers: Person) -> dict[str, object]:
    return {
        "firstName": pers.given_names or pers.initials,
        "lastName": pers.family_name,
        "creatorType": "author",
    }


def get_common_data(art: Article) -> dict[str, object]:
    data: dict[str, object] = {
        "creators": [get_author(pers) for pers in art.get_authors()],
        "date": art.year,
        "collections": [ZIJLSTRA_COLLECTION],  # Zijlstra
    }
    if art.title:
        data["title"] = art.title
    elif url := art.geturl():
        data["url"] = url
    tags = [{"tag": art.get_absolute_url()}]
    for nam in art.get_new_names():
        tags.append({"tag": f"original description of {nam!r}"})
    data["tags"] = tags
    return data


def get_zotero_item(art: Article) -> dict[str, object]:
    data: dict[str, object]
    match art.type:
        case ArticleType.JOURNAL:
            data = {
                "itemType": "journalArticle",
                **get_common_data(art),
                # Not used
                # "abstractNote": "",
                # "shortTitle": "",
                # "accessDate": "",
                # "archive": "",
                # "archiveLocation": "",
                # "libraryCatalog": "",
                # "callNumber": "",
                # "rights": "",
                # "extra": "",
                # "collections": [],
                # "relations": {},
            }
            if art.title:
                data["title"] = art.title
            if art.citation_group:
                data["publicationTitle"] = art.citation_group.name
                for tag in art.citation_group.tags:
                    if isinstance(tag, CitationGroupTag.ISSN):
                        data["ISSN"] = tag.text
            if art.volume:
                data["volume"] = art.volume
            if art.issue:
                data["issue"] = art.issue
            if art.series:
                data["series"] = art.series
            if art.end_page and art.start_page:
                data["pages"] = f"{art.start_page}-{art.end_page}"
            elif art.start_page:
                data["pages"] = art.start_page
            if art.doi:
                data["DOI"] = art.doi
            return data

        case ArticleType.BOOK:
            data = {
                "itemType": "book",
                **get_common_data(art),
                # "abstractNote": "",
                # "series": "",
                # "seriesNumber": "",
                # "volume": "",
                # "numberOfVolumes": "",
                # "edition": "",
                # "language": "",
                # "ISBN": "",
                # "shortTitle": "",
                # "accessDate": "",
                # "archive": "",
                # "archiveLocation": "",
                # "libraryCatalog": "",
                # "callNumber": "",
                # "rights": "",
                # "extra": "",
                # "collections": [],
                # "relations": {},
            }
            if art.citation_group:
                data["place"] = art.citation_group.name
            if art.publisher:
                data["publisher"] = art.publisher
            if art.pages:
                data["numPages"] = art.pages
            for tag in art.tags:
                if isinstance(tag, ArticleTag.ISBN):
                    data["ISBN"] = tag.text
            return data

        case ArticleType.THESIS:
            data = {
                "itemType": "thesis",
                **get_common_data(art),
                # "abstractNote": "",
                # "place": "",
                # "language": "",
                # "shortTitle": "",
                # "accessDate": "",
                # "archive": "",
                # "archiveLocation": "",
                # "libraryCatalog": "",
                # "callNumber": "",
                # "rights": "",
                # "extra": "",
                # "collections": [],
                # "relations": {},
            }

            if art.series:
                data["thesisType"] = art.series
            if art.citation_group:
                data["university"] = art.citation_group.name
            if art.pages:
                data["numPages"] = art.pages
            return data

        case ArticleType.CHAPTER | ArticleType.PART:
            data = {
                "itemType": "bookSection",
                **get_common_data(art),
                # "abstractNote": "",
                # "series": "",
                # "seriesNumber": "",
                # "volume": "",
                # "numberOfVolumes": "",
                # "edition": "",
                # "pages": "",
                # "language": "",
                # "shortTitle": "",
                # "accessDate": "",
                # "archive": "",
                # "archiveLocation": "",
                # "libraryCatalog": "",
                # "callNumber": "",
                # "rights": "",
                # "extra": "",
                # "collections": [],
                # "relations": {},
            }
            if art.parent:
                if art.parent.title:
                    data["bookTitle"] = art.parent.title
                if art.parent.citation_group:
                    data["place"] = art.parent.citation_group.name
                if art.parent.publisher:
                    data["publisher"] = art.parent.publisher
                for tag in art.parent.tags:
                    if isinstance(tag, ArticleTag.ISBN):
                        data["ISBN"] = tag.text
            if art.end_page and art.start_page:
                data["pages"] = f"{art.start_page}-{art.end_page}"
            elif art.start_page:
                data["pages"] = art.start_page
            return data

        case ArticleType.WEB:
            data = {
                "itemType": "webpage",
                **get_common_data(art),
                # "abstractNote": "",
                # "websiteTitle": "",
                # "websiteType": "",
                # "shortTitle": "",
                # "accessDate": "",
                # "language": "",
                # "rights": "",
                # "extra": "",
                # "collections": [],
                # "relations": {},
            }
            return data

    raise NotImplementedError(f"Article type {art.type!r} not implemented")


def upload_items(arts: Sequence[Article], *, fast_dupe_check: bool = False) -> None:
    zot = get_zotero()
    if fast_dupe_check:
        tags = get_existing_tags()
        arts = [art for art in arts if art.get_absolute_url() not in tags]
    else:
        arts = [
            art
            for art in arts
            if art.ispdf() and not zot.items(tag=art.get_absolute_url())
        ]
    if not arts:
        return
    print(f"Uploading {len(arts)} articles to Zotero after filtering")
    item_dicts = [get_zotero_item(art) for art in arts]
    pprint.pp(item_dicts)
    zot.check_items(item_dicts)
    result = zot.create_items(item_dicts)
    print(result)
    for i, art in enumerate(arts):
        try:
            item_id = result["success"][str(i)]
        except KeyError:
            print(f"Failed to upload {art}")
            continue
        attachment_result = zot.attachment_simple([str(art.get_path())], item_id)
        print(attachment_result)


def get_relevant_articles() -> Sequence[Article]:
    arts = set()
    arts.update(Article.select_valid().filter(Article.path.startswith("Chiroptera/")))

    taxon = Taxon.select_valid().filter(Taxon.valid_name == "Chiroptera").get()
    for nam in taxon.all_names_lazy():
        if nam.original_citation is not None:
            arts.add(nam.original_citation)
    print(f"Found {len(arts)} relevant articles")
    arts = {art for art in arts if not art.has_tag(ArticleTag.BatLit)}
    print(f"{len(arts)} articles without BatLit tag")
    arts = {
        art for art in arts if art.type is not ArticleType.SUPPLEMENT and art.ispdf()
    }
    return sorted(arts, key=lambda art: art.name)
