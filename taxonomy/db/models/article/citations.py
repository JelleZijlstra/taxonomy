"""Functions for citing articles."""

import re

from taxonomy.db import helpers
from taxonomy.db.constants import ArticleType, NamingConvention

from .article import Article, ArticleTag, register_cite_function


def wikify(s: str) -> str:
    """Wikifies text (e.g., turns _ into '')."""
    s = s.replace("'", "<nowiki>'</nowiki>").replace("_", "''")
    return re.sub(r"(?<!')<nowiki>'<\/nowiki>(?!')", "'", s)


def page_range(article: Article, dash: str = "-") -> str:
    # return a string representing the pages of the article
    if article.article_number:
        return article.article_number
    elif article.start_page:
        if article.end_page:
            if article.start_page == article.end_page:
                # single page
                return str(article.start_page)
            else:
                # range
                return f"{article.start_page}{dash}{article.end_page}"
        else:
            return str(article.start_page)
    else:
        return ""


def format_authors(
    art: Article,
    *,
    separator: str = ";",  # Text between two authors
    last_separator: str | None = None,  # Text between last two authors
    separator_with_two_authors: (
        None | str
    ) = None,  # Text between authors if there are only two
    capitalize_names: bool = False,  # Whether to capitalize names
    space_initials: bool = False,  # Whether to space initials
    initials_before_name: bool = False,  # Whether to place initials before the surname
    first_initials_before_name: bool = False,  # Whether to place the first author's initials before their surname
    include_initials: bool = True,  # Whether to include initials
    romanize: bool = False,  # Whether to romanize names
    include_dots: bool = True,  # Whether to include dots after initials
    before_initials: str = ",",  # set to "" to have no comma before initials
) -> str:
    if last_separator is None:
        last_separator = separator
    if separator_with_two_authors is None:
        separator_with_two_authors = last_separator
    array = art.get_authors()
    out = ""
    num_authors = len(array)
    for i, author in enumerate(array):
        # Separators
        if i > 0:
            if i < num_authors - 1:
                out += f"{separator} "
            elif i == 1:
                out += f"{separator_with_two_authors} "
            else:
                out += f"{last_separator} "

        # Process author
        if romanize:
            family_name = author.get_transliterated_family_name()
        else:
            family_name = author.family_name
        if capitalize_names:
            family_name = family_name.upper()
        if include_initials:
            initials = author.get_initials()
        else:
            initials = None
        if initials:
            if not include_dots:
                initials = initials.replace(".", "")
            if romanize:
                initials = helpers.romanize_russian(initials)
            if space_initials:
                initials = re.sub(r"\.(?![- ]|$)", ". ", initials)
            if author.tussenvoegsel:
                initials += f" {author.tussenvoegsel}"

            if first_initials_before_name if i == 0 else initials_before_name:
                author_str = f"{initials} {family_name}"
            else:
                author_str = f"{family_name}{before_initials} {initials}"
            if author.suffix and author.naming_convention in (
                NamingConvention.english,
                NamingConvention.english_peer,
                NamingConvention.ancient,
            ):
                author_str += f", {author.suffix}"
        else:
            author_str = family_name
        out += author_str
    return out


@register_cite_function("paper")
def citepaper(
    article: Article,
    *,
    include_url: bool = True,
    romanize_authors: bool = False,
    full_date: bool = False,
) -> str:
    # like citenormal(), but without WP style links and things
    return _citenormal(
        article,
        mw=False,
        include_url=include_url,
        romanize_authors=romanize_authors,
        full_date=full_date,
    )


@register_cite_function("normal")
def citenormal(article: Article) -> str:
    return _citenormal(article, mw=True)


def _citenormal(
    article: Article,
    *,
    mw: bool,
    child_article: Article | None = None,
    include_url: bool = True,
    romanize_authors: bool = False,
    full_date: bool = False,
) -> str:
    # cites according to normal WP citation style
    # if mw = False, no MediaWiki markup is used
    # this is going to be the citation
    if mw and child_article is None:
        out = "*"
    else:
        out = ""
    # replace last ; with ", and"; others with ","
    out += format_authors(
        article, separator=",", last_separator=" and", romanize=romanize_authors
    )
    if child_article is not None and child_article.type is ArticleType.CHAPTER:
        out += ". (eds.)"
    if child_article is not None and child_article.year == article.year:
        out += ". "
    elif full_date:
        out += f". {article.year}. "
    else:
        out += f". {article.numeric_year()}. "
    if mw:
        url = article.geturl()
        if url:
            out += f"[{url} "
    else:
        url = None
    # just in case it's None
    out += str(article.get_title())
    # TODO: guess whether "subscription required" is needed based on URL
    if url:
        out += "] (subscription required)"
    out += ". "
    if article.type == ArticleType.JOURNAL:
        # journals (most common case)
        if article.citation_group:
            out += f"{article.citation_group.get_citable_name()} "
        if article.is_in_press():
            out += "(in press)."
        else:
            if article.series:
                # need to catch "double series"
                series = str(article.series).replace(";", ") (")
                out += f"({series})"
            out += str(article.volume)
            if article.issue:
                out += f"({article.issue})"
            out += f":{page_range(article)}."
    elif article.type in (ArticleType.CHAPTER, ArticleType.PART):
        if article.start_page and article.end_page:
            if article.start_page == article.end_page:
                out += f"P. {article.start_page}"
            else:
                out += f"Pp. {article.start_page}–{article.end_page}"
        elif article.pages:
            out += article.pages
        enclosing = article.parent
        if not enclosing:
            out += " in Unknown."
        else:
            out += " in "
            out += _citenormal(
                enclosing, mw=mw, child_article=article, full_date=full_date
            )
    elif article.type == ArticleType.BOOK:
        out += f" {article.publisher}"
        if article.citation_group is not None:
            out += f", {article.citation_group.get_citable_name()}"
        if article.pages:
            out += f", {article.pages} pp"
        out += "."
    elif article.type == ArticleType.THESIS:
        if article.series is not None:
            out += f" {article.series} thesis"
        if article.citation_group is not None:
            out += f", {article.citation_group.get_citable_name()}"
        if article.pages:
            out += f", {article.pages} pp"
        out += "."
    if child_article is not None:
        return out
    if include_url:
        identifiers = []
        if article.url is not None:
            identifiers.append(f"URL: {article.url}")
        if not mw and article.doi:
            identifiers.append(f"doi:{article.doi}")
        if article.tags:
            for tag in article.tags:
                if isinstance(tag, ArticleTag.ISBN):
                    identifiers.append(f"ISBN {tag.text}")
                elif isinstance(tag, ArticleTag.HDL):
                    identifiers.append(f"HDL {tag.text}")
                elif isinstance(tag, ArticleTag.JSTOR):
                    identifiers.append(f"JSTOR {tag.text}")
                elif isinstance(tag, ArticleTag.PMID):
                    identifiers.append(f"PMID {tag.text}")
                elif isinstance(tag, ArticleTag.PMC):
                    identifiers.append(f"PMC {tag.text}")
        out += "".join(f" {text}" for text in identifiers)
    # final cleanup
    out = re.sub(r"\s+", " ", out).replace("..", ".").replace("-\\ ", "- ")
    if mw:
        out = wikify(out)
    return out


@register_cite_function("lemurnews")
def citelemurnews(article: Article) -> str:
    authors = format_authors(article)
    if article.type == ArticleType.JOURNAL:
        cg = article.citation_group.get_citable_name() if article.citation_group else ""
        out = (
            f"{authors}. {article.year}. {article.get_title()}. {cg} {article.volume}:"
            f" {article.start_page}–{article.end_page}."
        )
    elif article.type == ArticleType.BOOK:
        out = (
            f"{authors}. {article.year}. {article.get_title()}. {article.publisher},"
            f" {article.place_of_publication}."
        )
    else:
        raise NotImplementedError(article.type)
    # TODO: support non-journal citations
    # Ranaivoarisoa, J.F.; Ramanamahefa, R.; Louis, Jr., E.E.; Brenneman, R.A. 2006. Range extension
    # of Perrier's sifaka, <i>Propithecus perrieri</i>, in the Andrafiamena Classified Forest. Lemur
    # News 11: 17-21.

    # Book chapter
    # Ganzhorn, J.U. 1994. Les lémuriens. Pp. 70-72. In: S.M. Goodman; O. Langrand (eds.).
    # Inventaire biologique; Forêt de Zombitse. Recherches pour le Développement, Série Sciences
    # Biologiques, n° Spécial. Centre d'Information et de Documentation Scientifique et Technique,
    # Antananarivo, Madagascar.

    # Book
    # Mittermeier, R.A.; Konstant, W.R.; Hawkins, A.F.; Louis, E.E.; Langrand, O.; Ratsimbazafy,
    # H.J.; Rasoloarison, M.R.; Ganzhorn, J.U.; Rajaobelina, S.; Tattersall, I.; Meyers, D.M. 2006.
    # Lemurs of Madagascar. Second edition. Conservation International, Washington, DC, USA.

    # Thesis
    # Freed, B.Z. 1996. Co-occurrence among crowned lemurs (<i>Lemur coronatus</i>) and Sanford's
    # lemur (<i>Lemur fulvus sanfordi</i>) of Madagascar. Ph.D. thesis, Washington University, St.
    # Louis, USA.

    # Website
    # IUCN. 2008. IUCN Red List of Threatened Species. <www.iucnredlist.org>. Downloaded on 21 April
    # 2009.
    # final cleanup
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("bzn")
def citebzn(article: Article) -> str:
    # cites according to BZN citation style
    # replace last ; with " &"; others with ","
    out = format_authors(article, separator=",", include_dots=False)
    out += f" ({article.numeric_year()}) "
    if article.type == ArticleType.JOURNAL:
        out += f"{article.get_title()}. "
        if article.citation_group:
            out += f"{article.citation_group.get_citable_name()} "
        if article.series:
            # need to catch "double series"
            series = article.series.replace(";", ") (")
            out += f"({series})"
        out += f"{article.volume}: "
        out += f"{page_range(article)}."
    elif article.type == ArticleType.CHAPTER:
        out += f"{article.get_title()} [pp. {article.start_page}–{article.end_page}]."
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += " In: "
            out += format_authors(enclosing, include_dots=False).replace("(Ed", "(ed")
            out += f", {enclosing.get_title()}. "
            out += f"{enclosing.publisher}"
            if enclosing.place_of_publication:
                out += f", {enclosing.place_of_publication}"
            out += "."
    elif article.type == ArticleType.BOOK:
        out += f"{article.get_title()}."
        out += f" {article.publisher}"
        if article.place_of_publication:
            out += f", {article.place_of_publication}"
        out += "."
    # final cleanup
    return out.replace("  ", " ").replace("..", ".")


@register_cite_function("mammbiol")
def cite_mamm_biol(article: Article) -> str:
    out = format_authors(article, separator=",", include_dots=False, before_initials="")
    out += f" ({article.numeric_year()}) "
    out += f"{article.get_title()}. "
    match article.type:
        case ArticleType.JOURNAL:
            if article.citation_group:
                out += article.citation_group.get_citable_name()
                if article.is_in_press():
                    out += ", in press"
                else:
                    out += f" {article.volume}:{page_range(article)}"
                out += "."
        case ArticleType.BOOK:
            out += f"{article.publisher}"
            if article.citation_group:
                out += f", {article.citation_group.get_citable_name()}"
        case ArticleType.CHAPTER | ArticleType.PART:
            out += "In: "
            enclosing = article.get_enclosing()
            if enclosing is not None:
                out += format_authors(
                    enclosing, separator=",", include_dots=False, before_initials=""
                )
                out += " (ed) "
                out += f"{enclosing.get_title()}. {enclosing.publisher}"
                if enclosing.citation_group:
                    out += f", {enclosing.citation_group.get_citable_name()}"
                out += f", pp. {page_range(article)}"
    url = article.geturl()
    if url:
        out += f" {url}"
    return out


@register_cite_function("jhe")
def citejhe(article: Article) -> str:
    out = format_authors(article, separator=",")
    out += f", {article.year}. "
    out += f"{article.get_title()}. "
    if article.type == ArticleType.JOURNAL and article.citation_group is not None:
        out += article.citation_group.get_citable_name()
        if article.is_in_press():
            out += ", in press"
        else:
            out += f" {article.volume}, {page_range(article)}"
    elif article.type == ArticleType.BOOK:
        out += f"{article.publisher}, {article.place_of_publication}"
    elif article.type == ArticleType.CHAPTER:
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += "In: "
            out += format_authors(enclosing, separator=",")
            out += " (Eds.), "
            out += (
                f"{enclosing.get_title()}. {enclosing.publisher},"
                f" {enclosing.place_of_publication}, pp. "
            )
            out += page_range(article)
    elif article.type == ArticleType.THESIS:
        out += (
            f"{article.thesis_gettype(periods=True)} Dissertation,"
            f" {article.institution}"
        )
    else:
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("archnathist")
def citearchnathist(article: Article) -> str:
    out = []
    out.append(
        format_authors(
            article,
            separator=", ",
            last_separator=" and",
            capitalize_names=True,
            space_initials=True,
        )
    )
    out.append(f", {article.numeric_year()}. ")
    out.append(f"{article.get_title()}. ")
    if article.citation_group is not None:
        out.append(f"_{article.citation_group.get_citable_name()}_ ")
    if article.series:
        out.append(f"({article.series})")
    out.append(f"**{article.volume}**")
    if article.issue:
        out.append(f" ({article.issue})")
    out.append(f": {article.start_page}–{article.end_page}.")
    return "".join(out)


@register_cite_function("palaeontology")
def citepalaeontology(article: Article) -> str:
    # this is going to be the citation
    out = ""

    out += format_authors(
        article,
        capitalize_names=True,
        space_initials=True,
        separator=", ",
        last_separator=" and",
    )
    out += f". {article.year}. "
    if article.type == ArticleType.JOURNAL and article.citation_group is not None:
        out += f"{article.get_title()}. <i>{article.citation_group.get_citable_name()}</i>, "
        # TODO: series
        out += f"<b>{article.volume}</b>, {page_range(article)}"
    elif article.type == ArticleType.BOOK:
        out += f"<i>{article.get_title()}.</i> "
        out += f"{article.publisher}, "
        if article.place_of_publication:
            out += f"{article.place_of_publication}, "
        out += f"{article.pages} pp."
    elif article.type == ArticleType.CHAPTER:
        out += f"{article.get_title()}."
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += " <i>In</i> "
            out += format_authors(
                enclosing,
                capitalize_names=True,
                space_initials=True,
                separator=", ",
                last_separator=" and",
            )
            out += f" (eds). {enclosing.get_title()}.</i> "
            out += f"{enclosing.publisher}, "
            if enclosing.place_of_publication:
                out += f"{enclosing.place_of_publication}, "
            out += f"{enclosing.pages} pp."
    elif article.type == ArticleType.THESIS:
        out += f"<i>{article.get_title()}</i>. Unpublished "
        out += article.thesis_gettype(periods=True)
        out += f" thesis, {article.institution}"
        out += f", {article.pages} pp."
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("jpal")
def citejpal(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article,
        capitalize_names=True,
        initials_before_name=True,
        separator=",",
        last_separator=", and",
        separator_with_two_authors=" and",
        space_initials=True,
    )
    out += f" {article.year}. {article.get_title()}"
    if article.type == ArticleType.JOURNAL:
        if article.citation_group:
            out += f". {article.citation_group.get_citable_name()}, "
        if article.series:
            out += f"ser. {article.series}, "
        out += f"{article.volume}:{page_range(article)}"
    elif article.type == ArticleType.CHAPTER:
        out += f", {page_range(article)}."
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += " <i>In</i> "
            bauthors = format_authors(
                enclosing,
                capitalize_names=True,
                first_initials_before_name=True,
                initials_before_name=True,
                separator=",",
                last_separator=", and",
                separator_with_two_authors=" and",
                space_initials=True,
            )
            out += bauthors
            if "and " in bauthors:
                out += " (eds.), "
            else:
                out += " (ed.), "
            if enclosing.get_title():
                out += enclosing.get_title()
    elif article.type == ArticleType.BOOK:
        out += f". {article.publisher}"
        if article.place_of_publication:
            out += f", {article.place_of_publication}"
        if article.pages:
            out += f", {article.pages} p."
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("palevol")
def citepalevol(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article, initials_before_name=False, separator=",", space_initials=False
    )
    out += f", {article.year}. {article.get_title()}"
    if article.type == ArticleType.JOURNAL and article.citation_group is not None:
        out += f". {article.citation_group.get_citable_name()} "
        if article.series:
            out += f"ser. {article.series}, "
        out += f"{article.volume}, {page_range(article)}"
    elif article.type == ArticleType.CHAPTER:
        out += "."
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += ". In: "
            bauthors = format_authors(
                enclosing,
                initials_before_name=False,
                separator=",",
                space_initials=False,
            )
            out += bauthors
            if bauthors.count(",") > 2:
                out += " (Eds.), "
            else:
                out += " (Ed.), "
            out += f"{enclosing.get_title()}, {enclosing.publisher}"
            if enclosing.place_of_publication:
                out += f", {enclosing.place_of_publication}"
            out += f", {page_range(article)}."
    elif article.type == ArticleType.BOOK:
        out += f", {article.publisher}"
        if article.place_of_publication:
            out += f", {article.place_of_publication}"
        if article.pages:
            out += f", {article.pages} p."
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("jvp")
def citejvp(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article,
        initials_before_name=True,
        separator=",",
        last_separator=", and",
        space_initials=True,
        first_initials_before_name=False,
    )
    out += f". {article.year}. {article.get_title()}"
    if article.type == ArticleType.JOURNAL:
        if article.citation_group:
            out += f". {article.citation_group.get_citable_name()} "
        if article.series:
            out += f"ser. {article.series}, "
        out += f"{article.volume}:{page_range(article)}"
    elif article.type == ArticleType.CHAPTER:
        out += f"; pp. {page_range(article)}"
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += " in "
            bauthors = format_authors(
                enclosing,
                initials_before_name=False,
                separator=",",
                last_separator=", and",
                space_initials=True,
            )
            out += bauthors
            if bauthors.count(",") > 2:
                out += " (eds.), "
            else:
                out += " (ed.), "
            out += f"{enclosing.get_title()}. {enclosing.publisher}"
            if enclosing.place_of_publication:
                out += f", {enclosing.place_of_publication}"
    elif article.type == ArticleType.BOOK:
        out += f", {article.publisher}"
        if article.place_of_publication:
            out += f", {article.place_of_publication}"
        if article.pages:
            out += f", {article.pages} pp."
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


def getrefname(art: Article) -> str:
    # generate refname, which should usually be unique with this method
    authors = art.get_authors()
    if authors:
        author = authors[0].family_name
    else:
        author = ""
    refname = f"{author}{art.year}{art.volume}{art.start_page}"
    if refname == "":
        refname = art.get_title() or ""
    if refname.isnumeric():
        refname = f"ref{refname}"
    return refname.replace("'", "")


@register_cite_function("bibtex")
def citebibtex(article: Article) -> str:
    out = "@"

    def add(key: str, value: str | None, *, mandatory: bool = False) -> None:
        nonlocal out
        if not value:
            if mandatory:
                print(f"Bibtex error: required property {key} is empty")
            return
        out += f'\t{key} = "{value}",\n'

    if article.type == ArticleType.JOURNAL:
        out += "article"
    elif article.type == ArticleType.BOOK:
        out += "book"
    elif article.type == ArticleType.CHAPTER:
        out += "incollection"
    elif article.type == ArticleType.THESIS:
        thesis_type = article.thesis_gettype()
        if thesis_type == "PhD":
            out += "phdthesis"
        elif thesis_type == "MSc":
            out += "mscthesis"
        else:
            out += "misc"
    else:
        out += "misc"
    out += f"{{{getrefname(article)},\n"
    authors = format_authors(article, space_initials=True, separator=" and")
    # stuff that goes in every citation type
    add("author", authors, mandatory=True)
    add("year", article.year, mandatory=True)
    if article.get_title() is not None:
        title = re.sub(r"_([^_]+)_", r"\textit{\1}", article.get_title())
        title = f"{{{title}}}"
        add("title", title, mandatory=True)
    if article.type == ArticleType.THESIS:
        add("school", article.institution, mandatory=True)
    elif article.type == ArticleType.JOURNAL:
        if article.citation_group:
            add("journal", article.citation_group.get_citable_name(), mandatory=True)
        add("volume", article.volume)
        add("number", article.issue)
        add("pages", f"{article.start_page}--{article.end_page}")
    elif article.type == ArticleType.BOOK:
        add("publisher", article.publisher, mandatory=True)
        add("address", article.place_of_publication)
    isbn = article.get_identifier(ArticleTag.ISBN)
    if isbn:
        add("note", f"ISBN {isbn}")
    out += "}"
    return out


@register_cite_function("zootaxa")
def citezootaxa(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(article, separator=",", last_separator=" &")
    out += f" ({article.year}) "
    if article.type == ArticleType.JOURNAL and article.citation_group is not None:
        out += f"{article.get_title()}. <i>{article.citation_group.get_citable_name()}</i>, "
        out += f"{article.volume}, {page_range(article)}"
    elif article.type == ArticleType.CHAPTER:
        out += f"{article.get_title()}."
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += " <i>In</i>: "
            out += format_authors(enclosing, separator=",", last_separator=" &")
            out += f" (Eds), <i>{enclosing.get_title()}</i>. "
            out += f"{enclosing.publisher}, {enclosing.place_of_publication}"
            out += f", pp. {page_range(article)}"
    elif article.type == ArticleType.BOOK:
        out += f"<i>{article.get_title()}</i>. {article.publisher}"
        if article.place_of_publication:
            out += f", {article.place_of_publication}"
        if article.pages:
            out += f", {article.pages} pp."
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))


@register_cite_function("mammalia")
def citemammalia(article: Article) -> str:
    parts: list[str] = []
    parts.append(
        format_authors(
            article,
            separator=",",
            last_separator=", and",
            separator_with_two_authors=" and",
            romanize=True,
        )
    )
    parts.append(f" ({article.numeric_year()}). ")
    match article.type:
        case ArticleType.JOURNAL:
            parts.append(article.get_title())
            parts.append(". ")
            if article.citation_group is not None:
                parts.append(f"_{article.citation_group.get_citable_name()}_")
            if article.series:
                if article.series.isnumeric():
                    parts.append(f", ser. {article.series}, ")
                else:
                    parts.append(f", {article.series}, ")
            else:
                parts.append(" ")
            parts.append(f"{article.volume}: {page_range(article, dash="–")}.")
        case ArticleType.BOOK:
            parts.append(f"_{article.get_title()}_. ")
            if not article.publisher:
                raise ValueError(f"Book citation missing publisher: {article}")
            parts.append(article.publisher)
            if article.place_of_publication:
                parts.append(f", {article.place_of_publication}")
            parts.append(".")
        case ArticleType.CHAPTER | ArticleType.PART:
            parts.append(f"{article.get_title()}. ")
            enclosing = article.get_enclosing()
            if enclosing is not None:
                parent_enclosing = enclosing.get_enclosing()
                if parent_enclosing is not None:
                    enclosing = parent_enclosing
                parts.append("In: ")
                parts.append(
                    format_authors(
                        enclosing,
                        separator=",",
                        last_separator=", and",
                        separator_with_two_authors=" and",
                        romanize=True,
                    )
                )
                parts.append(" (Eds.). ")
                parts.append(f"_{enclosing.get_title()}_")
                parts.append(". ")
                parts.append(enclosing.publisher)
                if not enclosing.publisher:
                    raise ValueError(f"Book citation missing publisher: {article}")
                if enclosing.place_of_publication:
                    parts.append(f", {enclosing.place_of_publication}")
                parts.append(f", pp. {page_range(article, dash='–')}.")
        case ArticleType.WEB:
            parts.append(f"{article.get_title()}. ")
            if article.publisher:
                parts.append(article.publisher)
                parts.append(". ")
            if article.url:
                parts.append(f"URL: {article.url}")
            elif article.doi:
                parts.append(f"URL: https://doi.org/{article.doi}")
            else:
                parts.append("No URL.")
        case _:
            raise NotImplementedError(repr(article.type))
    return "".join(parts)


@register_cite_function("commons")
def citecommons(article: Article) -> str:
    return citewp(article, commons=True)


@register_cite_function("wp")
def citewp(article: Article, *, commons: bool = False) -> str:
    # cites according to {{cite journal}} etc.
    # stuff related to {{cite doi}} and friends
    # determines whether only one citation is returned or two if
    # {{cite doi}} or friends can be used
    if article.doi:
        # to fix bug 28212. Commented out for now since it seems we don't
        # need it. Or perhaps we do; I never know.
        doi = article.doi.replace("<", ".3C").replace(">", ".3E")
    else:
        doi = ""
    out1 = ""
    hdl = article.get_identifier(ArticleTag.HDL)
    if article.type == ArticleType.JOURNAL:
        label = "journal"
    elif article.type in (ArticleType.BOOK, ArticleType.CHAPTER):
        label = "book"
    elif article.type == ArticleType.THESIS:
        label = "thesis"
    elif article.type == ArticleType.MISCELLANEOUS:
        label = "web"
    elif article.type == ArticleType.WEB:
        label = "web"
    else:
        raise RuntimeError(f"unrecognized type {article.type!r}")
    paras: dict[str, str | None] = {}
    # authors
    if commons:
        # commons doesn't have last1 etc.
        paras["authors"] = "; ".join(
            author.get_full_name(family_first=True) for author in article.get_authors()
        )
    else:
        for i, author in enumerate(article.get_authors()):
            paras[f"last{i + 1}"] = author.family_name
            if author.given_names:
                paras[f"first{i + 1}"] = author.given_names
            elif author.initials:
                paras[f"first{i + 1}"] = author.initials
    # easy stuff we need in all classes
    paras["year"] = str(article.numeric_year())
    if hdl:
        paras["id"] = f"{{hdl|{hdl}}}"
    paras["jstor"] = article.get_identifier(ArticleTag.JSTOR)
    paras["pmid"] = article.get_identifier(ArticleTag.PMID)
    paras["url"] = article.url
    paras["doi"] = doi if doi else ""
    paras["pmc"] = article.get_identifier(ArticleTag.PMC)
    paras["publisher"] = article.publisher
    paras["location"] = article.place_of_publication
    paras["isbn"] = article.get_identifier(ArticleTag.ISBN)
    paras["pages"] = page_range(article)
    if article.type == ArticleType.JOURNAL:
        paras["title"] = article.get_title()
        if article.citation_group:
            paras["journal"] = article.citation_group.get_citable_name()
        paras["volume"] = article.volume
        paras["issue"] = article.issue
    elif article.type == ArticleType.BOOK:
        paras["title"] = article.get_title()
        if not paras["pages"]:
            paras["pages"] = article.pages
        paras["edition"] = article.get_identifier(ArticleTag.Edition)
    elif article.type == ArticleType.CHAPTER:
        paras["chapter"] = article.get_title()
        enclosing = article.get_enclosing()
        if enclosing is not None:
            paras["title"] = enclosing.get_title()
            paras["publisher"] = enclosing.publisher
            paras["location"] = enclosing.place_of_publication
            paras["edition"] = enclosing.get_identifier(ArticleTag.Edition)
            bauthors = enclosing.get_authors()
            for i, author in enumerate(bauthors):
                # only four editors supported
                if i < 4:
                    paras[f"editor{i + 1}-last"] = author.family_name
                    if author.given_names:
                        paras[f"editor{i + 1}-first"] = author.given_names
                    elif author.initials:
                        paras[f"editor{i + 1}-first"] = author.initials
                else:
                    # because cite book only supports four editors, we have to hack by
                    # putting the remaining editors in |editor4-last=
                    if i == 4:
                        del paras["editor4-first"]
                        paras["editor4-last"] = (
                            f"{bauthors[3].get_full_name(family_first=True)}; "
                        )
                    paras["editor4-last"] = (
                        paras["editor4-last"] or ""
                    ) + f"{bauthors[4].get_full_name(family_first=True)}; "
            # double period bug
            if "editor4-last" in paras and ";" in (paras["editor4-last"] or ""):
                paras["editor4-last"] = re.sub(
                    r"; $", "", re.sub(r"\.$", "", paras["editor4-last"] or "")
                )
            else:
                last_first = f"editor{len(bauthors)}-first"
                paras[last_first] = re.sub(r"\.$", "", paras[last_first] or "")
    elif article.type == ArticleType.THESIS:
        paras["title"] = article.get_title()
        paras["degree"] = article.thesis_gettype()
        paras["publisher"] = article.institution
        paras["pages"] = article.pages
    elif article.type == ArticleType.WEB:
        paras["title"] = article.get_title()
        paras["publisher"] = article.publisher
    out = sfn = ""
    out += f"{{{{cite {label} | "
    out += " | ".join(f"{key} = {value}" for key, value in paras.items() if value)
    out += "}}"
    # final cleanup
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", wikify(out)))
    return f"{sfn}{out1}\n{out}" if out1 else out


def getsfn(art: Article) -> str:
    sfn = "{{Sfn|"
    auts = art.get_authors()
    for aut in auts[:4]:
        sfn += f"{aut.family_name}|"
    sfn += f"{art.year}}}"
    return sfn


def getharvard(art: Article, mode: str = "normal") -> str:
    # get a Harvard citation
    # TODO: implement getting both Zijlstra et al. (2010) and (Zijlstra et al., 2010)
    authors = art.get_authors()
    out = ""
    num_authors = len(authors)
    if num_authors == 0:
        return ""  # incomplete info
    elif num_authors == 1:
        out += authors[0].family_name
    elif num_authors == 2:
        out += f"{authors[0].family_name} and {authors[1].family_name}"
    else:
        out += authors[0].family_name
        if mode == "jpal":
            out += " <i>et al.</i>"
        else:
            out += " et al."
    out += f" ({art.year})"
    return out


@register_cite_function("mammspec")
def cite_mammspec(article: Article, *, year_suffix: str = "") -> str:
    out = ""
    out += format_authors(
        article,
        separator=",",
        last_separator=", and",
        initials_before_name=True,
        first_initials_before_name=False,
        romanize=True,
    )
    out += f" {article.numeric_year()}{year_suffix}. "
    if article.type == ArticleType.JOURNAL:
        assert article.citation_group is not None, f"{article} has no citation group"
        out += f"{article.get_title()}. {article.citation_group.get_citable_name()} "
        if article.series is not None:
            out += f"({article.series})"
        out += f"{article.volume}:{page_range(article, dash='–')}"
    elif article.type in (ArticleType.CHAPTER, ArticleType.PART):
        out += f"{article.get_title()}"
        enclosing = article.get_enclosing()
        if enclosing is not None:
            out += ". In: "
            out += format_authors(enclosing, separator=",", last_separator=", and")
            out += f" (Eds), <i>{enclosing.get_title()}</i>. "
            out += f"{enclosing.publisher}, {enclosing.place_of_publication}"
            out += f", pp. {page_range(article)}"
    elif article.type == ArticleType.BOOK:
        out += f"{article.get_title()}. {article.publisher}"
        if article.citation_group:
            out += f", {article.citation_group.get_citable_name()}"
    else:
        out += f"{article.get_title()}. "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    return re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
