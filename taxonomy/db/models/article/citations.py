"""Functions for citing articles."""

import re

from .article import Article, ArticleTag, register_cite_function
from ...constants import ArticleType


def wikify(s: str) -> str:
    """Wikifies text (e.g., turns _ into '')."""
    s = s.replace("'", "<nowiki>'</nowiki>").replace("_", "''")
    return re.sub(r"(?<!')<nowiki>'<\/nowiki>(?!')", "'", s)


def page_range(article: Article) -> str:
    # return a string representing the pages of the article
    if article.start_page:
        if article.end_page:
            if article.start_page == article.end_page:
                # single page
                return str(article.start_page)
            else:
                # range
                return f"{article.start_page}-{article.end_page}"
        else:
            return str(article.start_page)
    else:
        return ""


def format_authors(
    art: Article,
    separator: str = ";",  # Text between two authors
    lastSeparator: str | None = None,  # Text between last two authors
    separatorWithTwoAuthors: None | (
        str
    ) = None,  # Text between authors if there are only two
    capitalizeNames: bool = False,  # Whether to capitalize names
    spaceInitials: bool = False,  # Whether to space initials
    initialsBeforeName: bool = False,  # Whether to place initials before the surname
    firstInitialsBeforeName: bool = False,  # Whether to place the first author's initials before their surname
    includeInitials: bool = True,  # Whether to include initials
) -> str:
    if lastSeparator is None:
        lastSeparator = separator
    if separatorWithTwoAuthors is None:
        separatorWithTwoAuthors = lastSeparator
    array = art.get_authors()
    out = ""
    num_authors = len(array)
    for i, author in enumerate(array):
        # Separators
        if i > 0:
            if i < num_authors - 1:
                out += f"{separator} "
            elif i == 1:
                out += f"{separatorWithTwoAuthors} "
            else:
                out += f"{lastSeparator} "

        # Process author
        if capitalizeNames:
            family_name = author.family_name.upper()
        else:
            family_name = author.family_name
        initials = author.get_initials()

        if spaceInitials and initials:
            initials = re.sub(r"\.(?![- ]|$)", ". ", initials)
        if initials and author.tussenvoegsel:
            initials += f" {author.tussenvoegsel}"

        if initials and includeInitials:
            if firstInitialsBeforeName if i == 0 else initialsBeforeName:
                author_str = f"{initials} {family_name}"
            else:
                author_str = f"{family_name}, {initials}"
            if author.suffix:
                author_str += ", " + author.suffix
        else:
            author_str = family_name
        out += author_str
    return out


@register_cite_function("paper")
def citepaper(article: Article) -> str:
    # like citenormal(), but without WP style links and things
    return _citenormal(article, mw=False)


@register_cite_function("normal")
def citenormal(article: Article) -> str:
    return _citenormal(article, mw=True)


def _citenormal(article: Article, *, mw: bool) -> str:
    # cites according to normal WP citation style
    # if mw = False, no MediaWiki markup is used
    # this is going to be the citation
    if mw:
        out = "*"
    else:
        out = ""
    # replace last ; with ", and"; others with ","
    out += format_authors(article, separator=",", lastSeparator=" and")
    out += f". {article.year}. "
    if mw:
        url = article.geturl()
        if url:
            out += f"[{url} "
    # just in case it's None
    out += str(article.title)
    # TODO: guess whether "subscription required" is needed based on URL
    if mw and url:
        out += "] (subscription required)"
    out += ". "
    if article.type == ArticleType.JOURNAL:
        # journals (most common case)
        if article.citation_group:
            out += f"{article.citation_group.name} "
        if article.series:
            # need to catch "double series"
            out += "(" + str(article.series).replace(";", ") (") + ")"
        out += str(article.volume)
        if article.issue:
            out += f"({article.issue})"
        out += ":" + page_range(article) + "."
    elif article.type == ArticleType.CHAPTER:
        if article.start_page == article.end_page:
            out += f"P. {article.start_page}"
        else:
            out += f"Pp. {article.start_page}–{article.end_page}"
        enclosing = article.parent
        if not enclosing:
            out += " in Unknown"
        else:
            out += " in "
            out += format_authors(enclosing, separator=",", lastSeparator=" and")
            out += " (eds.). "
            out += f"{enclosing.title}. {enclosing.publisher}"
            if enclosing.pages:
                out += f", {enclosing.pages} pp"
        out += "."
    elif article.type == ArticleType.BOOK:
        out += f" {article.publisher}"
        if article.pages:
            out += f", {article.pages} pp"
        out += "."
    elif article.type == ArticleType.WEB:
        out += f" URL: {article.url}."
    if not mw and article.doi:
        out += f" doi:{article.doi}"
    # final cleanup
    out = re.sub(r"\s+", " ", out).replace("..", ".")
    if mw:
        out = wikify(out)
    return out


@register_cite_function("lemurnews")
def citelemurnews(article: Article) -> str:
    authors = format_authors(article)
    if article.type == ArticleType.JOURNAL:
        cg = article.citation_group.name if article.citation_group else ""
        out = (
            f"{authors}. {article.year}. {article.title}. {cg} {article.volume}:"
            f" {article.start_page}–{article.end_page}."
        )
    elif article.type == ArticleType.BOOK:
        out = (
            f"{authors}. {article.year}. {article.title}. {article.publisher},"
            f" {article.place_of_publication}."
        )
    # TODO: support non-journal citations
    # Ranaivoarisoa, J.F.; Ramanamahefa, R.; Louis, Jr., E.E.; Brenneman, R.A. 2006. Range extension
    # of Perrier’s sifaka, <i>Propithecus perrieri</i>, in the Andrafiamena Classified Forest. Lemur
    # News 11: 17-21.

    # Book chapter
    # Ganzhorn, J.U. 1994. Les lémuriens. Pp. 70-72. In: S.M. Goodman; O. Langrand (eds.).
    # Inventaire biologique; Forêt de Zombitse. Recherches pour le Développement, Série Sciences
    # Biologiques, n° Spécial. Centre d’Information et de Documentation Scientifique et Technique,
    # Antananarivo, Madagascar.

    # Book
    # Mittermeier, R.A.; Konstant, W.R.; Hawkins, A.F.; Louis, E.E.; Langrand, O.; Ratsimbazafy,
    # H.J.; Rasoloarison, M.R.; Ganzhorn, J.U.; Rajaobelina, S.; Tattersall, I.; Meyers, D.M. 2006.
    # Lemurs of Madagascar. Second edition. Conservation International, Washington, DC, USA.

    # Thesis
    # Freed, B.Z. 1996. Co-occurrence among crowned lemurs (<i>Lemur coronatus</i>) and Sanford’s
    # lemur (<i>Lemur fulvus sanfordi</i>) of Madagascar. Ph.D. thesis, Washington University, St.
    # Louis, USA.

    # Website
    # IUCN. 2008. IUCN Red List of Threatened Species. <www.iucnredlist.org>. Downloaded on 21 April
    # 2009.
    # final cleanup
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


@register_cite_function("bzn")
def citebzn(article: Article) -> str:
    # cites according to BZN citation style
    # replace last ; with " &"; others with ","
    out = "<b>"
    out += format_authors(article, separator=",", lastSeparator=" &")
    out += "</b> " + article.year + ". "
    if article.type == ArticleType.JOURNAL:
        out += article.title
        out += ". "
        if article.citation_group:
            out += f"<i>{article.citation_group.name}</i>, "
        if article.series:
            # need to catch "double series"
            out += "(" + article.series.replace(";", ") (") + ")"
        out += "<b>" + article.volume + "</b>: "
        out += page_range(article) + "."
    elif article.type == ArticleType.CHAPTER:
        out += f"{article.title}."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>in</i> "
            out += format_authors(enclosing).replace("(Ed", "(ed")
            out += ", <i>" + enclosing.title + "</i>. "
            out += enclosing.pages + " pp. " + enclosing.publisher
            if enclosing.place_of_publication:
                out += ", " + enclosing.place_of_publication
            out += "."
    elif article.type == ArticleType.BOOK:
        out += "<i>" + article.title + ".</i>"
        if article.pages:
            out += " " + article.pages + " pp."
        out += " " + article.publisher
        if article.place_of_publication:
            out += ", " + article.place_of_publication
        out += "."
    # final cleanup
    out = out.replace("  ", " ").replace("..", ".")
    return out


@register_cite_function("jhe")
def citejhe(article: Article) -> str:
    out = format_authors(article, separator=",")
    out += ", " + article.year + ". "
    out += article.title + ". "
    if article.type == ArticleType.JOURNAL:
        out += article.citation_group.name
        if article.is_in_press():
            out += ", in press"
        else:
            out += " " + article.volume + ", " + page_range(article)
    elif article.type == ArticleType.BOOK:
        out += article.publisher + ", " + article.place_of_publication
    elif article.type == ArticleType.CHAPTER:
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += "In: "
            out += format_authors(enclosing, separator=",")
            out += " (Eds.), "
            out += (
                enclosing.title
                + ". "
                + enclosing.publisher
                + ", "
                + enclosing.place_of_publication
                + ", pp. "
            )
            out += page_range(article)
    elif article.type == ArticleType.THESIS:
        out += (
            article.thesis_gettype(periods=True)
            + " Dissertation, "
            + article.institution
        )
    else:
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


@register_cite_function("palaeontology")
def citepalaeontology(article: Article) -> str:
    # this is going to be the citation
    out = ""

    out += format_authors(
        article,
        capitalizeNames=True,
        spaceInitials=True,
        separator=", ",
        lastSeparator=" and",
    )
    out += f". {article.year}. "
    if article.type == ArticleType.JOURNAL:
        out += article.title + ". <i>" + article.citation_group.name + "</i>, "
        # TODO: series
        out += "<b>" + article.volume + "</b>, " + page_range(article)
    elif article.type == ArticleType.BOOK:
        out += "<i>" + article.title + ".</i> "
        out += article.publisher + ", "
        if article.place_of_publication:
            out += article.place_of_publication + ", "
        out += article.pages + " pp."
    elif article.type == ArticleType.CHAPTER:
        out += f"{article.title}."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>In</i> "
            out += format_authors(
                enclosing,
                capitalizeNames=True,
                spaceInitials=True,
                separator=", ",
                lastSeparator=" and",
            )
            out += " (eds). " + enclosing.title + ".</i> "
            out += enclosing.publisher + ", "
            if enclosing.place_of_publication:
                out += enclosing.place_of_publication + ", "
            out += enclosing.pages + " pp."
    elif article.type == ArticleType.THESIS:
        out += "<i>" + article.title + "</i>. Unpublished "
        out += article.thesis_gettype(periods=True)
        out += f" thesis, {article.institution}"
        out += f", {article.pages} pp."
    else:
        out += article.title + ". "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


@register_cite_function("jpal")
def citejpal(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article,
        capitalizeNames=True,
        initialsBeforeName=True,
        separator=",",
        lastSeparator=", and",
        separatorWithTwoAuthors=" and",
        spaceInitials=True,
    )
    out += " " + article.year + ". " + article.title
    if article.type == ArticleType.JOURNAL:
        if article.citation_group:
            out += f". {article.citation_group.name}, "
        if article.series:
            out += f"ser. {article.series}, "
        out += article.volume + ":" + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += ", " + page_range(article) + "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>In</i> "
            bauthors = format_authors(
                enclosing,
                capitalizeNames=True,
                firstInitialsBeforeName=True,
                initialsBeforeName=True,
                separator=",",
                lastSeparator=", and",
                separatorWithTwoAuthors=" and",
                spaceInitials=True,
            )
            out += bauthors
            if "and " in bauthors:
                out += " (eds.), "
            else:
                out += " (ed.), "
            out += enclosing.title
    elif article.type == ArticleType.BOOK:
        out += ". " + article.publisher
        if article.place_of_publication:
            out += ", " + article.place_of_publication
        if article.pages:
            out += ", " + article.pages + " p."
    else:
        out += article.title + ". "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


@register_cite_function("palevol")
def citepalevol(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article, initialsBeforeName=False, separator=",", spaceInitials=False
    )
    out += f", {article.year}. {article.title}"
    if article.type == ArticleType.JOURNAL:
        out += ". " + article.citation_group.name + " "
        if article.series:
            out += "ser. " + article.series + ", "
        out += article.volume + ", " + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += ". In: "
            bauthors = format_authors(
                enclosing, initialsBeforeName=False, separator=",", spaceInitials=False
            )
            out += bauthors
            if bauthors.count(",") > 2:
                out += " (Eds.), "
            else:
                out += " (Ed.), "
            out += enclosing.title
            out += ", " + enclosing.publisher
            if enclosing.place_of_publication:
                out += ", " + enclosing.place_of_publication
            out += ", " + page_range(article) + "."
    elif article.type == ArticleType.BOOK:
        out += ", " + article.publisher
        if article.place_of_publication:
            out += ", " + article.place_of_publication
        if article.pages:
            out += ", " + article.pages + " p."
    else:
        out += article.title + ". "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


@register_cite_function("jvp")
def citejvp(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(
        article,
        initialsBeforeName=True,
        separator=",",
        lastSeparator=", and",
        spaceInitials=True,
        firstInitialsBeforeName=False,
    )
    out += ". " + article.year + ". " + article.title
    if article.type == ArticleType.JOURNAL:
        if article.citation_group:
            out += f". {article.citation_group.name} "
        if article.series:
            out += f"ser. {article.series}, "
        out += article.volume + ":" + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += "; pp. " + page_range(article)
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " in "
            bauthors = format_authors(
                enclosing,
                initialsBeforeName=False,
                separator=",",
                lastSeparator=", and",
                spaceInitials=True,
            )
            out += bauthors
            if bauthors.count(",") > 2:
                out += " (eds.), "
            else:
                out += " (ed.), "
            out += enclosing.title
            out += ". " + enclosing.publisher
            if enclosing.place_of_publication:
                out += ", " + enclosing.place_of_publication
    elif article.type == ArticleType.BOOK:
        out += ", " + article.publisher
        if article.place_of_publication:
            out += ", " + article.place_of_publication
        if article.pages:
            out += ", " + article.pages + " pp."
    else:
        out += article.title + ". "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


def getrefname(art: Article) -> str:
    # generate refname, which should usually be unique with this method
    authors = art.get_authors()
    if authors:
        author = authors[0].family_name
    else:
        author = ""
    refname = author + art.year + art.volume + art.start_page
    if refname == "":
        refname = art.title
    if refname.isnumeric():
        refname = "ref" + refname
    return refname.replace("'", "")


@register_cite_function("bibtex")
def citebibtex(article: Article) -> str:
    out = "@"

    def add(key: str, value: str | None, mandatory: bool = False) -> None:
        nonlocal out
        if not value:
            if mandatory:
                print("Bibtex error: required property " + key + " is empty")
            return
        out += "\t" + key + ' = "' + value + '",\n'

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
    out += "{" + getrefname(article) + ",\n"
    authors = format_authors(article, spaceInitials=True, separator=" and")
    # stuff that goes in every citation type
    add("author", authors, True)
    add("year", article.year, True)
    title = re.sub(r"_([^_]+)_", r"\textit{\1}", article.title)
    title = f"{{{title}}}"
    add("title", title, True)
    if article.type == ArticleType.THESIS:
        add("school", article.institution, True)
    elif article.type == ArticleType.JOURNAL:
        if article.citation_group:
            add("journal", article.citation_group.name, True)
        add("volume", article.volume)
        add("number", article.issue)
        add("pages", article.start_page + "--" + article.end_page)
    elif article.type == ArticleType.BOOK:
        add("publisher", article.publisher, True)
        add("address", article.place_of_publication)
    isbn = article.getIdentifier(ArticleTag.ISBN)
    if isbn:
        add("note", "{ISBN} " + isbn)
    out += "}"
    return out


@register_cite_function("zootaxa")
def citezootaxa(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += format_authors(article, separator=",", lastSeparator=" &")
    out += f" ({article.year}) "
    if article.type == ArticleType.JOURNAL:
        out += article.title + ". <i>" + article.citation_group.name + "</i>, "
        out += article.volume + ", " + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += article.title + "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>In</i>: "
            out += format_authors(enclosing, separator=",", lastSeparator=" &")
            out += " (Eds), <i>" + enclosing.title + "</i>. "
            out += enclosing.publisher + ", " + enclosing.place_of_publication
            out += ", pp. " + page_range(article)
    elif article.type == ArticleType.BOOK:
        out += "<i>" + article.title + "</i>. " + article.publisher
        if article.place_of_publication:
            out += ", " + article.place_of_publication
        if article.pages:
            out += f", {article.pages} pp."
    else:
        out += article.title + ". "
        out += "<!--Unknown citation type; fallback citation-->"
    # final cleanup
    out += "."
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", out))
    return out


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
    hdl = article.getIdentifier(ArticleTag.HDL)
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
        coauthors: list[str] = []
        for i, author in enumerate(article.get_authors()):
            if i < 9:
                # templates only support up to 9 authors
                paras[f"last{i + 1}"] = author.family_name
                if author.given_names:
                    paras[f"first{i + 1}"] = author.given_names
                elif author.initials:
                    paras[f"first{i + 1}"] = author.initials
            else:
                coauthors.append(author.get_full_name(family_first=True))
        if coauthors:
            paras["coauthors"] = "; ".join(coauthors)
    # easy stuff we need in all classes
    paras["year"] = article.year
    if hdl:
        paras["id"] = "{{hdl|" + hdl + "}}"
    paras["jstor"] = article.getIdentifier(ArticleTag.JSTOR)
    paras["pmid"] = article.getIdentifier(ArticleTag.PMID)
    paras["url"] = article.url
    paras["doi"] = doi if doi else ""
    paras["pmc"] = article.getIdentifier(ArticleTag.PMC)
    paras["publisher"] = article.publisher
    paras["location"] = article.place_of_publication
    paras["isbn"] = article.getIdentifier(ArticleTag.ISBN)
    paras["pages"] = page_range(article)
    if article.type == ArticleType.JOURNAL:
        paras["title"] = article.title
        if article.citation_group:
            paras["journal"] = article.citation_group.name
        paras["volume"] = article.volume
        paras["issue"] = article.issue
    elif article.type == ArticleType.BOOK:
        paras["title"] = article.title
        if not paras["pages"]:
            paras["pages"] = article.pages
        paras["edition"] = article.getIdentifier(ArticleTag.Edition)
    elif article.type == ArticleType.CHAPTER:
        paras["chapter"] = article.title
        enclosing = article.getEnclosing()
        if enclosing is not None:
            paras["title"] = enclosing.title
            paras["publisher"] = enclosing.publisher
            paras["location"] = enclosing.place_of_publication
            paras["edition"] = enclosing.getIdentifier(ArticleTag.Edition)
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
                            bauthors[3].get_full_name(family_first=True) + "; "
                        )
                    paras["editor4-last"] = (
                        (paras["editor4-last"] or "")
                        + bauthors[4].get_full_name(family_first=True)
                        + "; "
                    )
            # double period bug
            if "editor4-last" in paras and ";" in (paras["editor4-last"] or ""):
                paras["editor4-last"] = re.sub(
                    r"; $", "", re.sub(r"\.$", "", paras["editor4-last"] or "")
                )
            else:
                last_first = f"editor{len(bauthors)}-first"
                paras[last_first] = re.sub(r"\.$", "", paras[last_first] or "")
    elif article.type == ArticleType.THESIS:
        paras["title"] = article.title
        paras["degree"] = article.thesis_gettype()
        paras["publisher"] = article.institution
        paras["pages"] = article.pages
    elif article.type == ArticleType.WEB:
        paras["title"] = article.title
        paras["publisher"] = article.publisher
    out = sfn = ""
    out += "{{cite " + label + " | "
    out += " | ".join(f"{key} = {value}" for key, value in paras.items() if value)
    out += "}}"
    # final cleanup
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", wikify(out)))
    return f"{sfn + out1}\n{out}" if out1 else out


def getsfn(art: Article) -> str:
    sfn = "{{Sfn|"
    auts = art.get_authors()
    for aut in auts[:4]:
        sfn += aut.family_name + "|"
    sfn += art.year + "}}"
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
        out += authors[0].family_name + " and " + authors[1].family_name
    else:
        out += authors[0].family_name
        if mode == "jpal":
            out += " <i>et al.</i>"
        else:
            out += " et al."
    out += " (" + art.year + ")"
    return out
