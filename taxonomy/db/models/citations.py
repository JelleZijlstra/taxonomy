"""Functions for citing articles."""

import re
from typing import Dict, List, Optional

from .article import Article, Tag, register_cite_function
from ..constants import ArticleType


def wikify(s: str) -> str:
    """Wikifies text (e.g., turns <i> into '')."""
    s = s.replace("'", "<nowiki>'</nowiki>")
    s = re.sub(r"<\/?i>", "'", s)
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
    out += article.getAuthors(separator=",", lastSeparator=" and")
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
        out += f"{article.journal} "
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
            out += article.getEnclosingAuthors(separator=",", lastSeparator=" and")
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
    authors = article.getAuthors()
    if article.type == ArticleType.JOURNAL:
        out = f"{authors}. {article.year}. {article.title}. {article.journal} {article.volume}: {article.start_page}–{article.end_page}."
    elif article.type == ArticleType.BOOK:
        out = f"{authors}. {article.year}. {article.title}. {article.publisher}, {article.place_of_publication}."
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
    out += article.getAuthors(separator=",", lastSeparator=" &")
    out += "</b> " + article.year + ". "
    if article.type == ArticleType.JOURNAL:
        out += article.title
        out += ". "
        out += f"<i>{article.journal}</i>, "
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
            out += article.getEnclosingAuthors().replace("(Ed", "(ed")
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
    out = article.getAuthors(separator=",")
    out += ", " + article.year + ". "
    out += article.title + ". "
    if article.type == ArticleType.JOURNAL:
        out += article.abbreviatedJournal()
        if article.isinpress():
            out += ", in press"
        else:
            out += " " + article.volume + ", " + page_range(article)
    elif article.type == ArticleType.BOOK:
        out += article.publisher + ", " + article.place_of_publication
    elif article.type == ArticleType.CHAPTER:
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += "In: "
            out += article.getEnclosingAuthors(separator=",")
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

    out += article.getAuthors(
        capitalizeNames=True, spaceInitials=True, separator=", ", lastSeparator=" and"
    )
    out += f". {article.year}. "
    if article.type == ArticleType.JOURNAL:
        out += article.title + ". <i>" + article.journal + "</i>, "
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
            out += article.getEnclosingAuthors(
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
    out += article.getAuthors(
        capitalizeNames=True,
        initialsBeforeName=True,
        separator=",",
        lastSeparator=", and",
        separatorWithTwoAuthors=" and",
        spaceInitials=True,
    )
    out += " " + article.year + ". " + article.title
    if article.type == ArticleType.JOURNAL:
        out += f". {article.journal}, "
        if article.series:
            out += f"ser. {article.series}, "
        out += article.volume + ":" + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += ", " + page_range(article) + "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>In</i> "
            bauthors = article.getEnclosingAuthors(
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
    out += article.getAuthors(
        initialsBeforeName=False, separator=",", spaceInitials=False
    )
    out += f", {article.year}. {article.title}"
    if article.type == ArticleType.JOURNAL:
        out += ". " + article.abbreviatedJournal() + " "
        if article.series:
            out += "ser. " + article.series + ", "
        out += article.volume + ", " + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += ". In: "
            bauthors = article.getEnclosingAuthors(
                initialsBeforeName=False, separator=",", spaceInitials=False
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
    out += article.getAuthors(
        initialsBeforeName=True,
        separator=",",
        lastSeparator=", and",
        spaceInitials=True,
        firstInitialsBeforeName=False,
    )
    out += ". " + article.year + ". " + article.title
    if article.type == ArticleType.JOURNAL:
        out += f". {article.journal} "
        if article.series:
            out += f"ser. {article.series}, "
        out += article.volume + ":" + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += "; pp. " + page_range(article)
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " in "
            bauthors = article.getEnclosingAuthors(
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


@register_cite_function("bibtex")
def citebibtex(article: Article) -> str:
    # lambda function to add a property to the output
    def add(key: str, value: Optional[str], mandatory: bool = False) -> None:
        nonlocal out
        if not value:
            if mandatory:
                print("Bibtex error: required property " + key + " is empty")
            return
        out += "\t" + key + ' = "' + value + '",\n'

    out = "@"
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
    out += "{" + article.getrefname() + ",\n"
    authors = article.getAuthors(spaceInitials=True, separator=" and")
    # stuff that goes in every citation type
    add("author", authors, True)
    add("year", article.year, True)
    title = "{" + article.title.replace("<i>", "\textit{").replace("</i>", "}") + "}"
    add("title", title, True)
    if article.type == ArticleType.THESIS:
        add("school", article.institution, True)
    elif article.type == ArticleType.JOURNAL:
        add("journal", article.journal, True)
        add("volume", article.volume)
        add("number", article.issue)
        add("pages", article.start_page + "--" + article.end_page)
    elif article.type == ArticleType.BOOK:
        add("publisher", article.publisher, True)
        add("address", article.place_of_publication)
    isbn = article.getIdentifier(Tag.ISBN)
    if isbn:
        add("note", "{ISBN} " + isbn)
    out += "}"
    return out


@register_cite_function("zootaxa")
def citezootaxa(article: Article) -> str:
    # this is going to be the citation
    out = ""
    out += article.getAuthors(separator=",", lastSeparator=" &")
    out += f" ({article.year}) "
    if article.type == ArticleType.JOURNAL:
        out += article.title + ". <i>" + article.journal + "</i>, "
        out += article.volume + ", " + page_range(article)
    elif article.type == ArticleType.CHAPTER:
        out += article.title + "."
        enclosing = article.getEnclosing()
        if enclosing is not None:
            out += " <i>In</i>: "
            out += article.getEnclosingAuthors(separator=",", lastSeparator=" &")
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


@register_cite_function("wp")
def citewp(article: Article) -> str:
    # cites according to {{cite journal}} etc.
    # stuff related to {{cite doi}} and friends
    # determines whether only one citation is returned or two if
    # {{cite doi}} or friends can be used
    verbosecite = article.global_p.verbosecite
    if article.doi:
        # to fix bug 28212. Commented out for now since it seems we don't
        # need it. Or perhaps we do; I never know.
        doi = article.doi.replace("<", ".3C").replace(">", ".3E")
    else:
        doi = ""
    out1 = ""
    if doi:
        # {{cite doi}}
        out1 = "{{cite doi|" + doi + "}}"
    elif article.getIdentifier(Tag.JStor):
        # {{cite jstor}}
        out1 = "{{cite jstor|" + article.getIdentifier(Tag.JStor) + "}}"  # type: ignore
    elif article.getIdentifier(Tag.HDL):
        # {{cite hdl}}
        out1 = "{{cite hdl|" + article.getIdentifier(Tag.HDL) + "}}"  # type: ignore
    if not verbosecite and out1:
        return out1
    if article.type == ArticleType.JOURNAL:
        label = "journal"
    elif article.type in (ArticleType.BOOK, ArticleType.CHAPTER):
        label = "book"
    elif article.type == ArticleType.THESIS:
        label = "thesis"
    elif article.type == ArticleType.MISCELLANEOUS:
        return out1
    else:
        raise RuntimeError(f"unrecognized type {article.type}")
    paras: Dict[str, Optional[str]] = {}
    # authors
    authors = article.getAuthors(asArray=True)
    coauthors: List[str] = []
    for i, author in enumerate(authors):
        if i < 9:
            # templates only support up to 9 authors
            paras[f"last{i + 1}"] = author[0]
            if author[1]:
                paras[f"first{i + 1}"] = author[1]
        else:
            coauthors.append(", ".join(author))
    if coauthors:
        paras["coauthors"] = "; ".join(coauthors)
    # easy stuff we need in all classes
    paras["year"] = article.year
    if article.getIdentifier(Tag.HDL):
        paras["id"] = "{{hdl|" + article.getIdentifier(Tag.HDL) + "}}"  # type: ignore
    paras["jstor"] = article.getIdentifier(Tag.JStor)
    paras["pmid"] = article.getIdentifier(Tag.PMID)
    paras["url"] = article.url
    paras["doi"] = doi if doi else ""
    paras["pmc"] = article.getIdentifier(Tag.PMC)
    paras["publisher"] = article.publisher
    paras["location"] = article.place_of_publication
    paras["isbn"] = article.getIdentifier(Tag.ISBN)
    paras["pages"] = page_range(article)
    if article.type == ArticleType.JOURNAL:
        paras["title"] = article.title
        paras["journal"] = article.journal
        paras["volume"] = article.volume
        paras["issue"] = article.issue
    elif article.type == ArticleType.BOOK:
        paras["title"] = article.title
        if not paras["pages"]:
            paras["pages"] = article.pages
        paras["edition"] = article.getIdentifier(Tag.Edition)
    elif article.type == ArticleType.CHAPTER:
        paras["chapter"] = article.title
        enclosing = article.getEnclosing()
        if enclosing is not None:
            paras["title"] = enclosing.title
            paras["publisher"] = enclosing.publisher
            paras["location"] = enclosing.place_of_publication
            paras["edition"] = enclosing.getIdentifier(Tag.Edition)
            bauthors = article.getEnclosingAuthors(asArray=True)
            if bauthors:
                for i, author in enumerate(bauthors):
                    # only four editors supported
                    if i < 4:
                        paras[f"editor{i + 1}-last"] = author[0]
                        paras[f"editor{i + 1}-first"] = author[1]
                    else:
                        # because cite book only supports four editors, we have to hack by
                        # putting the remaining editors in |editor4-last=
                        if i == 4:
                            del paras["editor4-first"]
                            paras["editor4-last"] = ", ".join(bauthors[3]) + "; "
                        paras["editor4-last"] = (
                            (paras["editor4-last"] or "") + ", ".join(author) + "; "
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
    if article.global_p.includerefharv:
        paras["ref"] = "harv"
    out = sfn = ""
    if article.global_p.includesfn:
        out = sfn = "<!--" + article.getsfn() + "-->"
    out += "{{cite " + label + " | "
    out += " | ".join(f"{key} = {value}" for key, value in paras.items() if value)
    out += "}}"
    # final cleanup
    out = re.sub(r"\s+", " ", re.sub(r"(?<!\.)\.\.(?!\.)", ".", wikify(out)))
    return f"{sfn + out1}\n{out}" if verbosecite and out1 else out
