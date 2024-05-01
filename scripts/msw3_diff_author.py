import json
import re
from collections import Counter

from taxonomy.db import helpers
from taxonomy.db.constants import CommentKind
from taxonomy.db.models import Article, NameComment


def main() -> None:
    art = Article.getter("name")("Mammalia-review (MSW3)")
    assert art is not None
    comments = NameComment.select_valid().filter(
        NameComment.kind == CommentKind.structured_quote, NameComment.source == art
    )
    year_diffs = 0
    year_diff_counter: Counter[int] = Counter()
    actual_year_diffs = 0
    authority_diffs = 0
    for comment in comments:
        name = comment.name
        msw3_data = json.loads(comment.text)
        actual_year = str(name.numeric_year())
        msw3_year = re.sub(r"[ \-].*", "", msw3_data["Date"])
        if msw3_year != actual_year:
            print(f"{name}: year: {msw3_data['Date']} != {actual_year}")
            year_diffs += 1
        diff = abs(int(msw3_year) - int(actual_year))
        year_diff_counter[diff] += 1
        if msw3_data["ActualDate"] and msw3_data["ActualDate"] != actual_year:
            print(f"{name}: actual year: {msw3_data['ActualDate']} != {actual_year}")
            actual_year_diffs += 1
        authority = helpers.romanize_russian(name.taxonomic_authority())
        msw3_author = (
            msw3_data["Author"]
            .replace(", and ", " & ")
            .replace(" and ", " & ")
            .lstrip("(")
            .rstrip(")")
        )
        msw3_author = re.sub(r"</?i>", "", msw3_author)
        msw3_author = re.sub(r"\b[A-ZÉ]\. ", "", msw3_author)
        msw3_author = re.sub(r" in .*$", "", msw3_author)
        authority = clean_up_author(authority)
        msw3_author = clean_up_author(msw3_author)
        if msw3_author != authority:
            print(f"{name}: authority: {msw3_author} != {authority}")
            authority_diffs += 1
    print(f"year diffs: {year_diffs}, {year_diff_counter}")
    print(f"actual year diffs: {actual_year_diffs}")
    print(f"authority diffs: {authority_diffs}")


def clean_up_author(authority: str) -> str:
    authority = authority.replace("Geoffroy Saint-Hilaire", "Geoffroy")
    authority = re.sub(r"^(de|De|von|Von|van|Van) ", "", authority)
    authority = authority.replace("ue", "ü")
    authority = authority.replace("ae", "ä")
    return authority.replace(" [von Waldheim]", "")


if __name__ == "__main__":
    main()
