# Location

Locations are used to indicate the occurrence of taxa, mainly their type localities.
They are meant to represent named places with as much precision as the source material
allows, both in space and (for fossils) in time.

For example, a type locality given only as French Guiana belongs in the
[French Guiana](/l/French_Guiana) Location, while a source that identifies a particular
town or collecting site should use a Location for that place. Broad island groups,
mountain ranges, river basins, and similar geographic areas should also remain broad
when the source does not identify a particular point within them.

Localities that are not precisely indicated or that need more investigation may use a
more general Location representing a region and stratigraphic unit (for example,
[Morrison Formation (Wyoming)](</l/Morrison_Formation_(Wyoming)>)) or all fossils from a
region (for example, [Wyoming fossil](/l/Wyoming_fossil)). For extant taxa, historically
the database placed all localities in the same [region](region) in a single locality,
but more recently we have started using more specific locations.

Mark a Location with the _General_ tag when it intentionally represents a broad or
imprecise area rather than a single locality. A general Location should be replaced or
split only when source evidence supports a more precise assignment. Locations should be
considered general if they represent an area with a larger radius than about 10 km.

Use the _Unplaced_ tag instead when the source identifies a particular locality, but
that locality has not yet been securely placed in the Region hierarchy. _General_ and
_Unplaced_ should normally be mutually exclusive: _General_ describes the precision of
the source evidence, whereas _Unplaced_ records incomplete geographic interpretation of
a specific place.

## Naming convention

Location names have up to three parts:

```text
Name (disambiguator): modifier
```

The _name_ is preferably a geographic feature such as a town, river, mountain, cave, or
collecting site. The _disambiguator_ and _modifier_ are optional. Parentheses are
reserved for disambiguators, and modifiers follow a colon and a space.

Use a modern, neutral geographic name when an obsolete source-era name has a clear
modern equivalent. Preserve the historical wording in `location_detail`, not in the
canonical Location name. In particular, do not perpetuate historical names now widely
regarded as slurs. If the old term had variable boundaries and no exact modern
equivalent, choose a conservative modern description, mark it _General_, and explain the
correspondence in the evidence or comment. If no modern description can preserve the
historical extent without falsely equating it with one present-day unit, retain the old
name only with an explicit `(historical region)` disambiguator; for example,
`Prussia (historical region)`, not bare `Prussia`.

Prefer the full form of generic geographic abbreviations in canonical names: for
example, use `Mount Moriah`, `Fort Thomas`, and `Malheur National Wildlife Refuge`
rather than `Mt. Moriah`, `Ft. Thomas`, and `Malheur NWR`. Retain an abbreviation when
it is genuinely part of the official proper name. Preserve diacritics and the preferred
modern spelling; source spellings belong in `location_detail` or, when independently
useful for lookup, in an alias.

For example:

- `Castries (Hérault): 1 km N`
- `Foo River (California): mouth`
- `Eastgate (Barstovian)`
- `Maastricht Formation (Limburg, Netherlands)`

Use a disambiguator only when the base name is not globally unique. It should normally
name an enclosing [Region](region), an assigned [Period](period), or an assigned
stratigraphic unit. Qualified Region and stratigraphic-unit names may be written in
parenthetical, comma-separated, or unqualified form when the relationship is
unambiguous; for example, the Region `Limburg (Netherlands)` supports the disambiguator
`Limburg, Netherlands`. A stable geographic qualifier may exceptionally disambiguate
places with the same base name in the same Region; document such an exception with an
`IgnoreLintLocation` tag if the disambiguator lint cannot verify it. The generic
disambiguators `island` and `region` are always allowed for geographic features whose
scope would otherwise be unclear, as in `Saint Martin (island)` and `Guinea (region)`.
If there is a modifier and the location is outside the region designated by the base
name (for example, the nearby coast, or an "X km N" locality that crosses a border), the
region of the base name is acceptable as a disambiguator; this can be indicated with a
"NearbyRegion" tag on the location.

The assigned Region is structured data and should not normally be repeated at the end of
the Location name merely because the source printed a full political hierarchy. Thus, a
locality assigned to the Sint Eustatius Region should normally be `English Quarter`, not
`English Quarter, Sint Eustatius`. Use a parenthetical disambiguator when the shorter
base name is not globally unique. A longer hierarchical phrase may be retained when all
of its components are needed to identify the feature or distinguish a source locality,
but the verbatim hierarchy should otherwise be preserved in `location_detail`.

The modifier distinguishes a more specific place associated with the base feature. It
may be free-form text such as `mouth`, `upper`, or `near the bridge`. A completely
specified distance and direction is standardized using abbreviated units and compass
directions, without _of_; north/south components precede east/west components. Thus, use
`Monterey: 2 km S 1 mi W`, not `1 mi W, 2 kilometers south of Monterey` or
`Monterey (2 km S 1 mi W)`.

Distinguish a locality _defined by_ an offset from a named feature that is merely
_located by_ an offset. If the source gives only "2 km south of Monterey", use
`Monterey: 2 km S`. If the source gives a separately named feature such as "Bats Cave,
1.5 km east of English Harbour", use `Bats Cave` as the canonical name and retain the
offset in `location_detail`, unless the offset is needed to distinguish this Bats Cave
from another locality. Do not copy elevations, coordinates, political hierarchies, or
other locating evidence into the base name.

A locality defined by a coordinate pair may use standardized latitude and longitude as
its modifier, as in `Vilacota, Tacna, Peru: 17.145759°S 70.054278°W`. Use latitude
followed by longitude, separated by one space. The same coordinates must also be stored
in the Location's `latitude` and `longitude` fields. This form is appropriate when the
coordinates distinguish a source locality; it should not be added merely because
coordinates happen to be available for an otherwise adequately named feature.

All Locations based on the same feature should use the same base name and, when one is
needed, the same disambiguator. This keeps the base Location and its modified Locations
next to one another in alphabetical order:

```text
Castle Brace (Dominica)
Castle Brace (Dominica): 1 mi N
Castle Brace (Dominica): 2 mi SW
```

For a name containing several nested geographic components, choose one primary anchor
and use the same component order throughout that family of Locations. Put the primary
anchor first, followed by increasingly specific components; for example, use
`Steens Mountain, Little Blitzen Gorge, T33S, R33E, Sec. 10` consistently rather than
also creating `Steens Mountain, T33S, R33E, Sec. 10, Little Blitzen Gorge`. Two names
that contain the same components in a different order are duplicate candidates, not
automatically distinct localities. Preserve the source's original component order in
`location_detail`.

When a Location combines two or more parallel geographic components with _and_, put the
components in alphabetical order. This gives equivalent source phrasings one canonical
name: use `Bengal and Sri Lanka`, not `Sri Lanka and Bengal`, and
`Carinthia, Carniola, Styria, and Tyrol`, not an order copied from one particular
source. Alphabetize by the geographic component itself, ignoring relational words such
as `near`.

Except for coordinate-defined localities described above, coordinates, elevations,
source wording, and other evidence do not belong in the modifier merely to make a name
unique. Store them in their structured fields or in `location_detail`. Some numbered
sites, quarries, camps, and similar localities have no useful geographic anchor; they
may use their established site name as the base name.

### Coasts and offshore localities

Distinguish terrestrial coastal areas from adjacent marine waters. Use _coast_ for a
shoreline or coastal strip on land and place the Location in the corresponding land
Region, as in `Gulf of Guinea coast`. Use the modifier _coastal waters_ for a marine
locality associated with a named land area and place it in the appropriate ocean Region,
as in `Japan: coastal waters`. A named sea may be retained as the base feature, as in
`North Sea: Yorkshire coastal waters`.

Treat source phrases such as _offshore_, _off the coast of_, and _coastal waters_ as the
same kind of marine locality and prefer _coastal waters_ in the canonical Location name.
_Offshore_ may remain when it is part of a quantified locality description, as in
`Petit Manan Lighthouse: 40 mi offshore`; there it expresses the recorded offset, not a
separate category of marine Location.

The word _coast_ in a source does not by itself decide between these interpretations.
Use the description of the occurrence or specimen to distinguish an animal inhabiting a
coastal strip from one taken in nearby water; an explicitly stranded or beached marine
animal is terrestrial for this purpose. If the evidence still does not resolve the
distinction, use a _General_ Location under the lowest Region that contains both
possibilities rather than silently choosing land or water.

## Aliases and merges

Use one valid Location for one geographic locality. Alternate spellings, historical
names, punctuation variants, translated names, abbreviations, and reordered locality
components should not remain as separate valid Locations when the evidence shows that
they denote the same place.

When consolidating duplicate Locations, reassign all type-locality and occurrence
references to the selected canonical Location and retain the other record as an alias.
An alias points to the canonical Location and should not retain independent references.
The alias is useful when its name is an established source spelling, a likely search
term, or the target of an existing link. A trivial typographical error need not be
created as a new alias if it has never had an independent record or link.

Merging copies metadata that can be preserved without choosing between conflicting
claims. Missing periods, stratigraphic units, coordinates, numerical ages, and source
citations are copied to the canonical Location; compatible partial coordinate pairs are
completed; tags are combined; and distinct comments, `location_detail`, and `age_detail`
text are appended with the source alias identified. Conflicting scalar values or
coordinate pairs remain on the alias and produce a warning instead of overwriting the
canonical value. Review those warnings after applying a merge. Keep source wording and
provenance in the relevant `LocationDetail` or occurrence record even after the
canonical Location has been selected.

Punctuation-insensitive or accent-insensitive name matches, reordered components, and
shared coordinates are review leads only. Same-name places, nested features, and
separately named sites can legitimately be close together or share rounded published
coordinates.

## Fields

Locations have the following fields:

- _name_: The name of the location.
- _status_ and _parent_: A Location may be valid, deleted, or an alias. An alias has a
  parent pointing to its canonical Location.
- _region_: The [region](region) the location is physically in.
- _latitude_ and _longitude_: The point or coordinate extent that defines an exact
  Location. Use source coordinates or a securely identified geographic feature. Do not
  store a representative point or centroid for a general Location.
- _min period_ and _max period_: The youngest and oldest [period](period) the location
  is correlated with. Often these two will be the same, but sometimes the age of a
  location is only known to be within a range of several periods. These periods must be
  GTS units or biostratigraphic zones.
- _stratigraphic unit_: The stratigraphic unit, such as a formation, that the location
  derives from. This is also a [period](period).
- _tags_: Various extra information about the location. Current tags include:
  - _General_, which indicates that the Location intentionally represents a broad or
    imprecise area rather than a single locality
  - _Unplaced_, which indicates that a source identifies a particular locality but its
    placement in the Region hierarchy remains uncertain
  - _NearbyRegion_, which records a nearby Region used as the anchor or disambiguator
    for an offset locality outside that Region
  - _IgnoreLintLocation_, which records a reviewed exception to a named Location lint;
    it should include a useful explanation whenever the reason is not self-evident
  - Three tags indicating that the location corresponds to a location in another
    database: _PBDB_ for the [Paleobiology Database](https://paleobiodb.org/#/), _NOW_
    for the [New and Old Worlds](https://nowdatabase.org/) database, and _ETMNA_ for
    [Appendix I to Janis et al. (2008)](/a/North_America_Tertiary-localities.pdf).

When a Region's children exhaustively cover it, a specific Location should be assigned
to the appropriate child Region. A Location may remain directly under the parent only
when it is _General_, _Unplaced_, or the Region is explicitly marked as incompletely
divided; see [Region](region).

There are a few other fields, but these are currently not widely used.
