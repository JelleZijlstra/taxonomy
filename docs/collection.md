# Collection

A collection is an institution that holds biological specimens, often a natural history
museum or university.

## Fields

Collections have the following fields:

- _label_: A unique label for the collection, usually the standard abbreviation for the
  collection (e.g., [AMNH](/c/AMNH) for the American Museum of Natural History). If
  multiple collections share the same abbreviation, a disambiguator is included in
  parentheses. For example, the Universities of Ankara, Antananarivo, and Arizona share
  the abbreviation UA, so their collections are distinguished as
  [UA (Ankara)](</c/UA_(Ankara)>), [UA (Antananarivo)](</c/UA_(Antananarivo)>) and
  [UA (Arizona)](</c/UA_(Arizona)>). For some collections (usually associated with
  European universities), the city name is used as the label instead, because there is
  no well-established abbreviation (e.g. [Göttingen](/c/Göttingen)). For subcollections,
  the label of the parent collection should be used with an additional identifier after
  it, e.g. "BMNH (mammals)".
- _name_: The full English name of the institution. If the name is written in the Latin
  alphabet, the native name may be used instead.
- _location_: The [Region](region) in which the collection is physically located.
- _city_: The city in which the collection is physically located, as a string. This
  helps sort together similar collections in regions that have many.
- _comment_: Any comments on the collection. This may include previous names and notes
  on the history of the institution.
- _parent_: Parent collection for a sub-collection.

Sometimes specimens are included in private collections. For these, the label should be
"(last name) collection" and the name should be "Collection of (full name)". For
example, the [Dickey collection](/c/Dickey_collection) has "Collection of Donald R.
Dickey" as its full name.

There are a few special collections for dealing with unusual cases:

- [lost](/c/lost), which includes type specimens that are known to no longer be in
  existence.
- [untraced](/c/untraced). The fate of specimens included in this collection is not
  completely clear, but based on credible sources it appears unlikely that the type
  specimen is in a recognized collection.
- [in situ](/c/in_situ), for specimens which were left _in situ_ in the wild. This
  usually applies to trace fossils. A special case here is
  [_Brontopodus pentadactylus_](/n/83199): a museum was built around the _in situ_
  tracksite.
