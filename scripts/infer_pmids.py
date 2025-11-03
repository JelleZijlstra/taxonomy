import argparse
import sys
import time
from collections.abc import Iterable

from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article.api_data import infer_pmid_for_article
from taxonomy.db.models.article.article import Article, ArticleTag
from taxonomy.db.models.citation_group.cg import CitationGroup


def iter_articles_missing_pmid(*, only_if_existing: bool = False) -> Iterable[Article]:
    cgs_with_pmids = set()
    if only_if_existing:
        arts = Article.select_valid().filter(
            Article.tags.contains(f"[{ArticleTag.PMID._tag},")
        )
        for art in arts:
            if art.has_tag(ArticleTag.PMID):
                if art.citation_group:
                    cgs_with_pmids.add(art.citation_group)
    for art in Article.select_valid().filter(
        Article.type == ArticleType.JOURNAL, Article.year > "1950"
    ):
        if art.get_identifier(ArticleTag.PMID):
            continue
        if cgs_with_pmids and art.citation_group not in cgs_with_pmids:
            continue
        yield art


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Infer and add PMID tags to articles")
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of articles processed"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between API calls (politeness)",
    )
    parser.add_argument(
        "--enable-metadata",
        action="store_true",
        help="Also try title/journal/year search on Europe PMC (more coverage, slightly riskier)",
    )
    parser.add_argument(
        "--assume-journal-consistency",
        action="store_true",
        help="If one article from a journal does not have a PMID, assume others won't either (speeds up processing)",
    )
    parser.add_argument(
        "--only-if-existing",
        action="store_true",
        help="Only run for articles in journals where we already have a PMID",
    )
    args = parser.parse_args(argv)

    processed = 0
    added = 0
    skipped_journals: set[CitationGroup] = set()
    num_skipped_due_to_journal = 0
    for art in iter_articles_missing_pmid(only_if_existing=args.only_if_existing):
        if args.limit is not None and processed >= args.limit:
            break
        processed += 1
        if art.citation_group in skipped_journals:
            num_skipped_due_to_journal += 1
            continue

        pmid = infer_pmid_for_article(art, allow_metadata=args.enable_metadata)
        if pmid:
            print(f"{art.id}: {art.name} -> PMID {pmid}")
            if args.apply:
                art.add_tag(ArticleTag.PMID(pmid))
            added += 1
        else:
            print(f"{art.id}: {art.name} -> no PMID found")
            if args.assume_journal_consistency and art.citation_group:
                skipped_journals.add(art.citation_group)

        if args.sleep:
            time.sleep(args.sleep)

    print(
        f"Processed {processed} articles; {'added' if args.apply else 'would add'} {added} PMIDs."
    )
    if num_skipped_due_to_journal > 0:
        print(
            f"Skipped {num_skipped_due_to_journal} articles due to journal consistency."
        )
    if not args.apply:
        print("Dry run; rerun with --apply to write tags.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
