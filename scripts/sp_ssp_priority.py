from taxonomy.db import helpers
from taxonomy.db.constants import AgeClass, Group, Rank
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.name.name import NameTag
from taxonomy.db.models.taxon.lint import get_expected_base_name_report

ONLY_WITH_SAME_PAPER = True

taxa = Taxon.select_valid().filter(Taxon.rank.is_in([Rank.species, Rank.subspecies]))

for taxon in taxa:
    break
    if taxon.base_name.group is not Group.species:
        continue
    if taxon.base_name.original_rank is Rank.species:
        continue
    report = get_expected_base_name_report(taxon)
    if taxon.base_name in report.possibilities:
        continue
    if taxon.base_name.original_citation is not None:
        if not any(
            poss.original_citation == taxon.base_name.original_citation
            for poss in report.possibilities
        ):
            continue
    elif not any(
        poss.get_date_object() == taxon.base_name.get_date_object()
        for poss in report.possibilities
    ):
        continue
    print("--------------------------------")
    print(f"Taxon {taxon} has base name {taxon.base_name} but should be one of:")
    for poss in report.possibilities:
        print(f"  - {poss}")
    if taxon.base_name.original_citation:
        print(f"{taxon.base_name.original_citation.cite()}")

# Additional analysis: compare base name to other names in same year
print("\n==== Same-year priority analysis for species/subspecies ====")
for taxon in taxa:
    # Exclude for now
    if taxon.age not in (AgeClass.extant, AgeClass.recently_extinct):
        continue
    if taxon.rank is not Rank.species:
        continue
    # if taxon.base_name.original_rank is Rank.species:
    #     continue
    base: Name = taxon.base_name
    base_year = base.valid_numeric_year()
    if not base_year:
        continue
    # Gather other names with the same numeric year
    same_year: list[Name] = [
        nam
        for nam in taxon.get_names()
        if nam != base
        and nam.valid_numeric_year() == base_year
        and nam.resolve_variant() == nam
    ]
    if ONLY_WITH_SAME_PAPER:
        same_year = [
            nam
            for nam in same_year
            if nam.original_citation is not None
            and nam.original_citation == base.original_citation
        ]
    if not same_year:
        continue
    ranks = {nam.original_rank for nam in same_year} | {base.original_rank}
    if len(ranks) == 1:
        continue
    earlier_pub: list[Name] = []
    diff_rank: list[Name] = []
    priority_sel: list[Name] = []

    # Check if base_name has a SelectionOfPriority tag
    base_has_selection = any(
        isinstance(tag, NameTag.SelectionOfPriority) for tag in base.tags
    )

    for other in same_year:
        # (1) earlier publication: only consider if both dates are more specific than just the year
        if (
            base.year is not None
            and other.year is not None
            and "-" in base.year
            and "-" in other.year
            and helpers.is_valid_date(base.year)
            and helpers.is_valid_date(other.year)
        ):
            if base.get_date_object() < other.get_date_object():
                earlier_pub.append(other)

        # (2) difference in rank
        if base.original_rank != other.original_rank:
            diff_rank.append(other)

        # (3) priority selection
        # Consider either a SelectionOfPriority tag on the base name, or on 'other' selecting over base
        if base_has_selection:
            priority_sel.append(other)
        elif any(
            isinstance(tag, NameTag.SelectionOfPriority)
            and getattr(tag, "over_name", None) is base
            for tag in other.tags
        ):
            priority_sel.append(other)

    print("--------------------------------")
    print(f"Taxon {taxon} base {base} ({base.year}): same-year comparisons")
    if earlier_pub:
        print("  Earlier publication:")
        for n in earlier_pub:
            print(f"   - {n} ({n.year})")
    if diff_rank:
        print("  Difference in rank:")
        for n in diff_rank:
            print(
                f"   - {n} (rank {n.original_rank.name if n.original_rank else 'unknown'})"
            )
    if priority_sel:
        print("  Priority selection (SelectionOfPriority):")
        for n in priority_sel:
            print(f"   - {n}")
    remaining = set(same_year) - set(earlier_pub) - set(diff_rank) - set(priority_sel)
    if remaining:
        print("  Remaining same-year names:")
        for n in remaining:
            print(f"   - {n}")
