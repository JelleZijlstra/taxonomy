from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from taxonomy.db.constants import AgeClass, Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models import Article, CitationGroup, Person, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryStatus,
    ClassificationEntryTag,
)
from taxonomy.db.models.classification_entry.lint import (
    _is_misspelling_of,
    check_auxiliary_name,
    check_mapped_name,
    check_missing_mapped_name,
    check_needs_auxiliary_name,
    check_parent_cycle,
    check_parent_rank,
    check_tags,
    check_verbatim_parent,
    infer_duplicate,
    materialize_classification_entry,
)
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.name import Name, NameTag
from taxonomy.db.models.name.lint import infer_tags_from_mapped_entries
from taxonomy.db.models.person import AuthorTag

_ARTICLE = Article.virtual(name="classification source")


class _FakeQuery(list[Any]):
    def filter(self, *conditions: object) -> _FakeQuery:
        return self


def _make_ce(
    *, parent: ClassificationEntry | None, auxiliary: bool
) -> ClassificationEntry:
    tags = (ClassificationEntryTag.AuxiliaryName,) if auxiliary else ()
    return ClassificationEntry.virtual(
        article=_ARTICLE,
        name="classification entry",
        rank=Rank.genus,
        parent=parent,
        tags=tags,
    )


def _make_source_ce(
    *, parent: ClassificationEntry, verbatim_parent: ClassificationEntry
) -> ClassificationEntry:
    return ClassificationEntry.virtual(
        article=_ARTICLE,
        name="source entry",
        rank=Rank.genus,
        parent=parent,
        tags=(ClassificationEntryTag.VerbatimParent(verbatim_parent),),
    )


def _make_name(root_name: str, *tags: NameTag) -> Name:
    taxon = Taxon.virtual(rank=Rank.species, valid_name=root_name)
    return Name.virtual(
        group=Group.species,
        root_name=root_name,
        status=Status.valid,
        taxon=taxon,
        author_tags=(),
        tags=tags,
    )


def test_infer_duplicate_uses_structured_merge_fix() -> None:
    article = object()
    parent = object()
    mapped_name = object()
    tag = ClassificationEntryTag.PageLink("https://example.com", "2")
    dupe = SimpleNamespace(
        id=1,
        article=article,
        name="Example",
        parent=parent,
        mapped_name=mapped_name,
        rank=Rank.genus,
        authority=None,
        year=None,
        page="1",
        tags=(),
    )
    ce = SimpleNamespace(
        id=2,
        article=article,
        name="Example",
        parent=parent,
        mapped_name=mapped_name,
        rank=Rank.genus,
        authority=None,
        year=None,
        page="2",
        tags=(tag,),
        status=ClassificationEntryStatus.valid,
    )
    with patch.object(
        ClassificationEntry, "select_valid", return_value=_FakeQuery([dupe])
    ):
        (issue,) = infer_duplicate.linter(
            cast(ClassificationEntry, ce), LintConfig(autofix=False)
        )

    assert isinstance(issue, LintIssue)
    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert dupe.page == "1, 2"
    assert dupe.tags == (tag,)
    assert ce.parent is dupe
    assert ce.status is ClassificationEntryStatus.redirect


def test_verbatim_parent_is_valid() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    ce = _make_source_ce(parent=parent, verbatim_parent=auxiliary)

    assert list(check_verbatim_parent(ce, LintConfig())) == []


def test_verbatim_parent_must_be_auxiliary() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    target = _make_ce(parent=parent, auxiliary=False)
    ce = _make_source_ce(parent=parent, verbatim_parent=target)

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert str(messages[0]).endswith(
        f"VerbatimParent target {target} is not an AuxiliaryName CE [verbatim_parent]"
    )


def test_verbatim_parent_must_have_same_parent() -> None:
    auxiliary = _make_ce(parent=_make_ce(parent=None, auxiliary=False), auxiliary=True)
    ce = _make_source_ce(
        parent=_make_ce(parent=None, auxiliary=False), verbatim_parent=auxiliary
    )

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert str(messages[0]).endswith(
        f"VerbatimParent target {auxiliary} does not have the same parent [verbatim_parent]"
    )


def test_auxiliary_name_requires_same_rank_parent() -> None:
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = correct_name
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.species
    auxiliary.mapped_name = misspelling

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert str(messages[0]).endswith(
        "AuxiliaryName CE has parent of different rank genus [auxiliary_name]"
    )


def test_auxiliary_name_requires_misspelling_mapping() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = _make_name("correct")
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = _make_name("not a misspelling")

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert str(messages[0]).endswith(
        "AuxiliaryName CE does not map to a misspelling of its parent name [auxiliary_name]"
    )


def test_auxiliary_name_accepts_misspelling_mapping() -> None:
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectOriginalSpellingOf(correct_name)
    )
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = correct_name
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = misspelling

    assert list(check_auxiliary_name(auxiliary, LintConfig())) == []


def test_parent_rank_allows_same_rank_auxiliary_subspecies() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    parent.rank = Rank.subspecies
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.subspecies

    assert list(check_parent_rank(auxiliary, LintConfig())) == []


def test_parent_cycle_lint() -> None:
    first = _make_ce(parent=None, auxiliary=False)
    second = _make_ce(parent=first, auxiliary=False)
    first.parent = second

    assert list(check_parent_cycle.linter(first, LintConfig())) == [
        "parent cycle detected"
    ]


def test_parent_cycle_lint_accepts_acyclic_tree() -> None:
    root = _make_ce(parent=None, auxiliary=False)
    child = _make_ce(parent=root, auxiliary=False)

    assert list(check_parent_cycle.linter(child, LintConfig())) == []


def test_is_misspelling_of() -> None:
    correct = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct)
    )

    assert _is_misspelling_of(misspelling, correct)


def test_missing_mapped_name_exposes_structured_fix_and_decorator_code() -> None:
    ce = _make_ce(parent=None, auxiliary=False)
    ce.mapped_name = None
    inferred = _make_name("inferred")

    with patch(
        "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
        return_value=[inferred],
    ):
        (issue,) = check_missing_mapped_name(
            ce, LintConfig(autofix=False, interactive=False)
        )

    assert issue.code == "missing_mapped_name"
    assert issue.fix is not None
    assert ce.mapped_name is None

    with patch(
        "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
        return_value=[inferred],
    ):
        assert (
            list(
                check_missing_mapped_name(
                    ce, LintConfig(autofix=True, interactive=False)
                )
            )
            == []
        )

    assert ce.mapped_name is inferred


def test_missing_mapped_name_is_owned_by_materialize() -> None:
    ce = _make_ce(parent=None, auxiliary=False)
    ce.mapped_name = None
    ce.tags = (ClassificationEntryTag.Materialize(parent_taxon_id=1),)  # type: ignore[assignment]

    assert (
        list(
            check_missing_mapped_name(ce, LintConfig(autofix=False, interactive=False))
        )
        == []
    )
    assert list(check_mapped_name(ce, LintConfig())) == []


def test_materialize_accepted_entry_creates_taxon_and_base_name() -> None:
    parent_taxon = Taxon.virtual(
        valid_name="Punctoidea", rank=Rank.superfamily, age=AgeClass.extant
    )
    parent_name = Name.virtual(
        taxon=parent_taxon, group=Group.family, root_name="Punct", status=Status.valid
    )
    parent_ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Punctoidea",
        rank=Rank.superfamily,
        mapped_name=parent_name,
        tags=(),
    )
    materialize_tag = ClassificationEntryTag.Materialize()
    ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodontidae",
        rank=Rank.family,
        parent=parent_ce,
        page="12",
        year="1895",
        tags=(materialize_tag,),
    )
    created_taxon = Taxon.virtual(
        valid_name="Endodontidae",
        rank=Rank.family,
        age=AgeClass.extant,
        parent=parent_taxon,
    )
    created_name = Name.virtual(
        taxon=created_taxon,
        group=Group.family,
        root_name="Endodont",
        status=Status.valid,
    )
    taxon_values: dict[str, object] = {}
    name_values: dict[str, object] = {}

    def create_taxon(**kwargs: object) -> Taxon:
        taxon_values.update(kwargs)
        return created_taxon

    def create_name(**kwargs: object) -> Name:
        name_values.update(kwargs)
        return created_name

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[],
        ),
        patch.object(Taxon, "create", side_effect=create_taxon),
        patch.object(Name, "create", side_effect=create_name),
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )
        assert issue.fix is not None
        assert issue.fix.is_virtual_safe is False
        assert issue.fix.apply() is True

    assert taxon_values == {
        "valid_name": "Endodontidae",
        "rank": Rank.family,
        "age": AgeClass.extant,
        "parent": parent_taxon,
    }
    assert name_values["taxon"] is created_taxon
    assert name_values["root_name"] == "Endodont"
    assert name_values["status"] is Status.valid
    assert ce.mapped_name is created_name
    assert materialize_tag not in ce.tags


def test_materialize_synonym_creates_name_on_parent_taxon_only() -> None:
    accepted_taxon = Taxon.virtual(
        valid_name="Endodonta minor", rank=Rank.species, age=AgeClass.extant
    )
    accepted_name = Name.virtual(
        taxon=accepted_taxon,
        group=Group.species,
        root_name="minor",
        status=Status.valid,
    )
    parent_ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodonta minor",
        rank=Rank.species,
        mapped_name=accepted_name,
        tags=(),
    )
    ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodonta parva",
        rank=Rank.synonym_species,
        parent=parent_ce,
        page="14",
        tags=(ClassificationEntryTag.Materialize(),),
    )
    created_name = Name.virtual(
        taxon=accepted_taxon,
        group=Group.species,
        root_name="parva",
        status=Status.synonym,
    )
    name_values: dict[str, object] = {}

    def create_name(**kwargs: object) -> Name:
        name_values.update(kwargs)
        return created_name

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[],
        ),
        patch.object(Taxon, "create") as create_taxon,
        patch.object(Name, "create", side_effect=create_name),
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )
        assert issue.fix is not None
        assert issue.fix.apply() is True

    create_taxon.assert_not_called()
    assert name_values["taxon"] is accepted_taxon
    assert name_values["status"] is Status.synonym
    assert name_values["root_name"] == "parva"


def test_materialize_original_citation_uses_exact_act_authors() -> None:
    sitthivong = Person.virtual(family_name="Sitthivong")
    brakels = Person.virtual(family_name="Brakels")
    wang = Person.virtual(family_name="Wang")
    poyarkov = Person.virtual(family_name="Poyarkov")
    article = Article.virtual(
        name="Laodracon nov.pdf",
        year="2023-10-10",
        author_tags=tuple(
            AuthorTag.Author(person=person)
            for person in (sitthivong, brakels, wang, poyarkov)
        ),
    )
    parent_taxon = Taxon.virtual(
        valid_name="Agamidae", rank=Rank.family, age=AgeClass.extant
    )
    ce = ClassificationEntry.virtual(
        article=article,
        name="Laodracon",
        rank=Rank.genus,
        page="1044",
        authority="Brakels, Sitthivong, Wang & Poyarkov",
        year="2023",
        tags=(
            ClassificationEntryTag.Materialize(parent_taxon_id=1),
            ClassificationEntryTag.OriginalCitation,
        ),
    )
    created_taxon = Taxon.virtual(
        valid_name="Laodracon",
        rank=Rank.genus,
        age=AgeClass.extant,
        parent=parent_taxon,
    )
    created_name = Name.virtual(
        taxon=created_taxon,
        group=Group.genus,
        root_name="Laodracon",
        status=Status.valid,
    )
    name_values: dict[str, object] = {}

    def create_name(**kwargs: object) -> Name:
        name_values.update(kwargs)
        return created_name

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[],
        ),
        patch(
            "taxonomy.db.models.classification_entry.lint._materialization_parent_taxon",
            return_value=parent_taxon,
        ),
        patch.object(Taxon, "create", return_value=created_taxon),
        patch.object(Name, "create", side_effect=create_name),
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )
        assert issue.fix is not None
        assert issue.fix.apply() is True

    assert name_values["original_citation"] is article
    assert name_values["year"] == "2023-10-10"
    assert name_values["author_tags"] == (
        AuthorTag.Author(person=brakels),
        AuthorTag.Author(person=sitthivong),
        AuthorTag.Author(person=wang),
        AuthorTag.Author(person=poyarkov),
    )


def test_materialize_later_combination_creates_base_and_combination_names() -> None:
    goin = Person.virtual(family_name="Goin")
    woodley = Person.virtual(family_name="Woodley")
    citation_group = CitationGroup.virtual(name="Zoological Journal")
    pinheiro = Person.virtual(family_name="Pinheiro")
    kok = Person.virtual(family_name="Kok")
    revision = Article.virtual(
        name="Nesorohyla nov.pdf",
        year="2018-07-06",
        author_tags=(AuthorTag.Author(person=pinheiro), AuthorTag.Author(person=kok)),
    )
    genus_taxon = Taxon.virtual(
        valid_name="Nesorohyla", rank=Rank.genus, age=AgeClass.extant
    )
    genus_name = Name.virtual(
        taxon=genus_taxon,
        group=Group.genus,
        root_name="Nesorohyla",
        status=Status.valid,
    )
    parent_ce = ClassificationEntry.virtual(
        article=revision,
        name="Nesorohyla",
        rank=Rank.genus,
        mapped_name=genus_name,
        tags=(),
    )
    materialize_tag = ClassificationEntryTag.Materialize()
    base_name_tag = ClassificationEntryTag.MaterializeBaseName(
        original_name="Hyla kanaima",
        page="135",
        verbatim_citation=(
            "Goin, C. J. & Woodley, J. D. 1969. A new tree-frog from Guyana. Zoological Journal 48: 135–140."
        ),
        citation_group=citation_group,
    )
    base_author_tags = (
        ClassificationEntryTag.MaterializeBaseNameAuthor(person=goin, order=1),
        ClassificationEntryTag.MaterializeBaseNameAuthor(person=woodley, order=2),
    )
    ce = ClassificationEntry.virtual(
        article=revision,
        name="Nesorohyla kanaima",
        rank=Rank.species,
        parent=parent_ce,
        page="231",
        authority="Goin & Woodley",
        year="1969",
        tags=(materialize_tag, base_name_tag, *base_author_tags),
    )
    created_taxon = Taxon.virtual(
        valid_name="Nesorohyla kanaima",
        rank=Rank.species,
        age=AgeClass.extant,
        parent=genus_taxon,
    )
    base_name = Name.virtual(
        taxon=created_taxon,
        group=Group.species,
        root_name="kanaima",
        status=Status.valid,
    )
    combination_name = Name.virtual(
        taxon=created_taxon,
        group=Group.species,
        root_name="kanaima",
        status=Status.synonym,
    )
    created_name_values: list[dict[str, object]] = []

    def create_name(**kwargs: object) -> Name:
        created_name_values.append(kwargs)
        return base_name if len(created_name_values) == 1 else combination_name

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[],
        ),
        patch(
            "taxonomy.db.models.classification_entry.lint._materialized_base_name_candidates",
            return_value=[],
        ),
        patch.object(Taxon, "create", return_value=created_taxon),
        patch.object(Name, "create", side_effect=create_name),
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )
        assert issue.fix is not None
        assert issue.fix.apply() is True

    assert len(created_name_values) == 2
    base_values, combination_values = created_name_values
    assert base_values["original_name"] == "Hyla kanaima"
    assert base_values["original_citation"] is None
    assert base_values["verbatim_citation"] == base_name_tag.verbatim_citation
    assert base_values["citation_group"] is citation_group
    assert base_values["author_tags"] == (
        AuthorTag.Author(person=goin),
        AuthorTag.Author(person=woodley),
    )
    assert base_values["status"] is Status.valid
    assert combination_values["original_name"] == "Nesorohyla kanaima"
    assert combination_values["original_citation"] is revision
    assert combination_values["author_tags"] == (
        AuthorTag.Author(person=pinheiro),
        AuthorTag.Author(person=kok),
    )
    assert combination_values["year"] == "2018-07-06"
    assert combination_values["status"] is Status.synonym
    assert (
        combination_values["nomenclature_status"] is NomenclatureStatus.name_combination
    )
    assert combination_values["tags"] == (NameTag.NameCombinationOf(base_name),)
    assert "type_tags" not in combination_values
    assert created_taxon.base_name is base_name
    assert ce.mapped_name is combination_name
    assert materialize_tag not in ce.tags
    assert base_name_tag not in ce.tags
    assert not any(tag in ce.tags for tag in base_author_tags)


def test_materialize_parent_chain_is_deferred_without_becoming_unresolved() -> None:
    parent = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodontidae",
        rank=Rank.family,
        mapped_name=None,
        tags=(ClassificationEntryTag.Materialize(parent_taxon_id=1),),
    )
    ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodonta",
        rank=Rank.genus,
        parent=parent,
        mapped_name=None,
        tags=(ClassificationEntryTag.Materialize(),),
    )
    with patch(
        "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
        return_value=[],
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )

    assert issue.fix is not None
    assert issue.fix.is_virtual_safe is False


def test_materialize_cross_source_parent_uses_reconciled_taxon() -> None:
    parent_taxon = Taxon.virtual(
        valid_name="Atlantihyla", rank=Rank.genus, age=AgeClass.extant
    )
    parent_name = Name.virtual(
        taxon=parent_taxon,
        group=Group.genus,
        root_name="Atlantihyla",
        status=Status.valid,
    )
    parent_ce = ClassificationEntry.virtual(
        article=Article.virtual(name="Hylini revision.pdf"),
        name="Atlantihyla",
        rank=Rank.genus,
        mapped_name=parent_name,
        tags=(),
    )
    materialize = ClassificationEntryTag.Materialize()
    materialize_parent = ClassificationEntryTag.MaterializeParent(ce=parent_ce)
    ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Atlantihyla melissa",
        rank=Rank.species,
        parent=None,
        page="739",
        year="2020",
        tags=(materialize, materialize_parent),
    )
    created_taxon = Taxon.virtual(
        valid_name="Atlantihyla melissa",
        rank=Rank.species,
        age=AgeClass.extant,
        parent=parent_taxon,
    )
    created_name = Name.virtual(
        taxon=created_taxon,
        group=Group.species,
        root_name="melissa",
        status=Status.valid,
    )

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[],
        ),
        patch.object(Taxon, "create", return_value=created_taxon) as create_taxon,
        patch.object(Name, "create", return_value=created_name),
    ):
        (issue,) = materialize_classification_entry(
            ce, LintConfig(autofix=False, interactive=False)
        )
        assert issue.fix is not None
        assert issue.fix.apply() is True

    assert create_taxon.call_args.kwargs["parent"] is parent_taxon
    assert ce.mapped_name is created_name
    assert materialize not in ce.tags
    assert materialize_parent not in ce.tags


def test_materialize_tag_is_removed_when_mapping_already_exists() -> None:
    ce = _make_ce(parent=None, auxiliary=False)
    ce.mapped_name = _make_name("existing")
    tag = ClassificationEntryTag.Materialize(parent_taxon_id=1)
    ce.tags = (tag,)  # type: ignore[assignment]

    (issue,) = materialize_classification_entry(
        ce, LintConfig(autofix=False, interactive=False)
    )
    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert ce.tags == ()


def test_satisfied_materialize_removes_base_name_instructions_atomically() -> None:
    ce = _make_ce(parent=None, auxiliary=False)
    ce.mapped_name = _make_name("Nesorohyla kanaima")
    goin = Person.virtual(family_name="Goin")
    retained = ClassificationEntryTag.OriginalCombination("Hyla kanaima")
    materialize = ClassificationEntryTag.Materialize()
    base_name = ClassificationEntryTag.MaterializeBaseName(
        original_name="Hyla kanaima",
        page="135",
        verbatim_citation="Goin & Woodley, 1969: 135–140.",
        citation_group=CitationGroup.virtual(name="Zoological Journal"),
    )
    author = ClassificationEntryTag.MaterializeBaseNameAuthor(person=goin, order=1)
    ce.tags = (retained, materialize, base_name, author)  # type: ignore[assignment]

    (issue,) = materialize_classification_entry(
        ce, LintConfig(autofix=False, interactive=False)
    )

    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert ce.tags == (retained,)


def test_mapped_entry_removes_orphaned_base_name_instructions() -> None:
    ce = _make_ce(parent=None, auxiliary=False)
    ce.mapped_name = _make_name("Nesorohyla kanaima")
    goin = Person.virtual(family_name="Goin")
    retained = ClassificationEntryTag.OriginalCombination("Hyla kanaima")
    base_name = ClassificationEntryTag.MaterializeBaseName(
        original_name="Hyla kanaima",
        page="135",
        verbatim_citation="Goin & Woodley, 1969: 135–140.",
        citation_group=CitationGroup.virtual(name="Zoological Journal"),
    )
    author = ClassificationEntryTag.MaterializeBaseNameAuthor(person=goin, order=1)
    ce.tags = (retained, base_name, author)  # type: ignore[assignment]

    issues = list(check_tags(ce, LintConfig(autofix=False, interactive=False)))
    cleanup_issues = [
        issue for issue in issues if "remove satisfied materialization" in str(issue)
    ]

    assert len(cleanup_issues) == 1
    assert cleanup_issues[0].fix is not None
    assert cleanup_issues[0].fix.apply() is True
    assert ce.tags == (retained,)


def test_etymology_detail_is_transferred_to_mapped_name() -> None:
    taxon = Taxon.virtual(valid_name="Endodonta", rank=Rank.genus, age=AgeClass.extant)
    name = Name.virtual(
        taxon=taxon,
        group=Group.genus,
        root_name="Endodonta",
        status=Status.valid,
        original_citation=None,
        type_tags=(),
    )
    ce = ClassificationEntry.virtual(
        article=_ARTICLE,
        name="Endodonta",
        rank=Rank.genus,
        mapped_name=name,
        citation=None,
        tags=(ClassificationEntryTag.EtymologyDetail("Named for its teeth."),),
    )
    with (
        patch.object(Name, "get_classification_entries", return_value=[ce]),
        patch.object(Name, "resolve_variant", return_value=name),
    ):
        (issue,) = infer_tags_from_mapped_entries(
            name, LintConfig(autofix=False, interactive=False)
        )

    assert issue.fix is not None
    assert issue.fix.apply() is True
    (tag,) = name.type_tags
    assert tag.text == "Named for its teeth."
    assert tag.source is _ARTICLE


def test_needs_auxiliary_name_finds_misspelled_sibling() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    correct_ce = _make_ce(parent=parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = misspelling

    with patch(
        "taxonomy.db.models.classification_entry.lint._get_same_level_ces",
        return_value=[misspelled_ce, correct_ce],
    ):
        messages = list(
            check_needs_auxiliary_name(misspelled_ce, LintConfig(autofix=False))
        )

    assert len(messages) == 1
    assert "convert to AuxiliaryName under sibling CE" in str(messages[0])


def test_needs_auxiliary_name_autofix() -> None:
    original_parent = _make_ce(parent=None, auxiliary=False)
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    correct_ce = _make_ce(parent=original_parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=original_parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = misspelling
    child = _make_ce(parent=misspelled_ce, auxiliary=False)

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint._get_same_level_ces",
            return_value=[misspelled_ce, correct_ce],
        ),
        patch.object(misspelled_ce, "get_children", return_value=[child]),
    ):
        messages = list(check_needs_auxiliary_name(misspelled_ce, LintConfig()))

    assert messages == []
    assert misspelled_ce.has_tag(ClassificationEntryTag.AuxiliaryName)
    assert misspelled_ce.parent is correct_ce
    assert child.parent is correct_ce
    assert [
        tag.ce
        for tag in child.get_tags(child.tags, ClassificationEntryTag.VerbatimParent)
    ] == [misspelled_ce]
