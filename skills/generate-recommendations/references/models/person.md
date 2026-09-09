# Person recommendations

Read this file for Person merges or selective reassignment of references. For model
semantics, consult [docs/person.md](../../../../docs/person.md). For author creation
owned by Article intake, read the [Article reference](article.md#author-references).

## Merge or reassign references

Schema-version 2 `merge_person` combines two records for the same human identity. It
requires complete source and target Person-field snapshots plus guarded ID lists for
every Article, Name, Book, patronym, collector, involvement, and redirect reference. The
action moves all references to the canonical target, unions compatible tags and
biographical identifiers, clears transferred metadata from the source, and retains the
source name as a hard redirect. If the union contains multiple ORCIDs, it records a
reviewed `IgnoreLint("multiple_orcids")` tag on the canonical Person. Planning rejects
stale reference sets, redirect targets, and conflicting birth, death, biography, or Open
Library identifiers.

Schema-version 2 `reassign_person_references` handles an identity-safe subset of a
potential Person merge without redirecting the source. It requires complete source and
target Person-field snapshots plus guarded ID lists for every Article, Name, Book,
patronym, collector, and involvement reference. The action moves those current
references and one specified ORCID from the source to the target, but otherwise leaves
the source as an ordinary Person. Use it for ambiguous abbreviated forms such as
`C. Jones`: reviewed existing references may belong to `Craig M. Jones`, while a future
`C. Jones` reference may denote somebody else.
