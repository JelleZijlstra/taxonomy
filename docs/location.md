# Location

Locations are used to indicate the occurrence of taxa, mainly their type localities.
They are meant to represent named places with as much precision as the source material
allows, both in space and (for fossils) in time. We use named Locations rather than just
listing geographic coordinates because many sources do not give precise coordinates and
when they do the coordinates are prone to typographical errors. Named Locations allow us
to associate records from the same place and cross-check the name of the Location
against other forms of geographical data.

Ideally Locations represent a single, small area (< 5 km). For fossils, all records in
the same Location should be of the same age, to the extent the source material allows.

Sometimes we don't know the precise origin of a record, or for convenience records in
the database are for now lumped together. In those cases we use _general_ localities.
For extant taxa, we historically used general localities corresponding to entire
[Regions](region), such as [French Guiana](/l/French_Guiana). Now we are in the process
of moving such occurrences to more precise Locations; the general Location is kept for
records where no more precise evidence is available. For fossils we similarly use
general locations representing all fossils from a particular region (for example,
[Wyoming fossil](/l/Wyoming_fossil)) or from a region and stratigraphic unit (for
example, [Morrison Formation (Wyoming)](</l/Morrison_Formation_(Wyoming)>)).

## Naming convention

Location names have up to three parts:

```text
Name (disambiguator): modifier
```

The _name_ is the main identifier for the location. It should be simple and concise. The
_disambiguator_ and _modifier_ are optional. Parentheses are reserved for
disambiguators, and modifiers follow a colon and a space.

### name

The name should preferably a geographic feature such as a town, river, mountain, cave,
or collecting site. Use modern names, even when the source material uses obsolete names.
In particular, avoid names now considered offensive. Expand abbreviations such as "Mt.",
"Ft.", or "St.", unless the abbreviated form is actually part of the official name. Use
concise names, ideally the name of a single geographical feature, even if the source may
use a more elaborate description.

Locations should preferably denote precise locations (at the scale of a few kilometers
at most), but if necessary a general location can be used encompassing a larger area if
the sources are not precise. In this case, the name can be the name of a region, or a
stratigraphic unit to cover all fossils found in a particular stratigraphic unit in an
area.

When a Location combines two or more parallel geographic components with _and_, put the
components in alphabetical order. This gives equivalent source phrasings one canonical
name: use `Bengal and Sri Lanka`, not `Sri Lanka and Bengal`, and
`Carinthia, Carniola, Styria, and Tyrol`, not an order copied from one particular
source. Alphabetize by the geographic component itself, ignoring relational words such
as `near`.

### disambiguator

A disambiguator within parentheses should be used when the name is not globally unique.
It should normally name an enclosing [Region](region), an assigned [Period](period), or
an assigned stratigraphic unit. Qualified Region and stratigraphic-unit names may be
written in parenthetical, comma-separated, or unqualified form when the relationship is
unambiguous; for example, the Region `Limburg (Netherlands)` supports the disambiguator
`Limburg, Netherlands`. A stable geographic qualifier may exceptionally disambiguate
places with the same base name in the same Region; document such an exception with an
`IgnoreLintLocation` tag if the disambiguator lint cannot verify it. The generic
disambiguators `island`, `region`, and `historical region` are always allowed for
geographic features whose scope would otherwise be unclear, as in
`Saint Martin (island)` and `Guinea (region)`. If there is a modifier and the location
is outside the region designated by the base name (for example, the nearby coast, or an
"X km N" locality that crosses a border), the region of the base name is acceptable as a
disambiguator; this can be indicated with a "NearbyRegion" tag on the location.

### modifier

The modifier indicates a modification or subpart of the overall location. Common
categories include:

- Geographic offsets such as "1 km N", when the source gives a location like "1 km north
  of some town". This should be standardized to use "km", "m", or "mi", in a format like
  "2 km S 1 km W" or "3.1 mi W".
- Coordinates or PLSS data, when there are multiple collecting locations denoted by the
  same geographic name but with slightly different coordinates
- Subdivisions, such as the upper part or mouth of a river.

Modifiers should be concise and use a small, standardized vocabulary.

### Examples

- `Castries (Hérault): 1 km N`
- `Foo River (California): mouth`
- `Eastgate (Barstovian)`
- `Maastricht Formation (Limburg, Netherlands)`

All Locations based on the same feature should use the same base name and, when one is
needed, the same disambiguator. This keeps the base Location and its modified Locations
next to one another in alphabetical order:

```text
Castle Brace (Dominica)
Castle Brace (Dominica): 1 mi N
Castle Brace (Dominica): 2 mi SW
```

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
    imprecise area rather than a single locality. Locations should be considered general
    if they represent an area with a larger radius than about 10 km.
  - _Unplaced_, which indicates that a source identifies a particular locality but its
    physical location remains unknown. This is mutually exclusive with General.
  - _NearbyRegion_, which records a nearby Region used as the anchor or disambiguator
    for an offset locality outside that Region
  - _PLSS_, which records a standardized Public Land Survey System description and its
    resolved BLM CadNSDI township identifier
  - _CoordinatesFromName_, _CoordinatesFromOccurrenceRecord_, _CoordinatesFromPLSS_,
    _CoordinatesFromGeoNames_, _CoordinatesFromNominatim_,
    _CoordinatesFromLocationName_, and _CoordinatesManual_, which document the evidence
    supporting the coordinate fields
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

## Aliases and merges

Use one valid Location for one geographic locality. Alternate spellings, historical
names, punctuation variants, translated names, abbreviations, and reordered locality
components should not remain as separate valid Locations when the evidence shows that
they denote the same place.

Merging copies metadata that can be preserved without choosing between conflicting
claims. Missing periods, stratigraphic units, coordinates, numerical ages, and source
citations are copied to the canonical Location; compatible partial coordinate pairs are
completed; tags are combined; and distinct comments, `location_detail`, and `age_detail`
text are appended with the source alias identified. Conflicting scalar values or
coordinate pairs remain on the alias and produce a warning instead of overwriting the
canonical value. Review those warnings after applying a merge. Keep source wording and
provenance in the relevant `LocationDetail` or occurrence record even after the
canonical Location has been selected.

## Public Land Survey System descriptions

The database uses the U.S. Public Land Survey System (PLSS) description for locating and
cross-validating locations. This uses a _PLSS_ Location tag, which is usually derived
from evidence on Name or OccurrenceRecord objects.

The tag has one primary `text` field, a township-level CadNSDI `plss_id`, and an
optional `comment`. Use the comment to document source errors, historical-county issues,
or other reviewed discrepancies. The `plss_id` is the BLM `PLSSID`; it records the
state, principal meridian, township, range, fractions, and duplicate code. Section,
aliquot, and government-lot information remains in `text` and is resolved within that
township.

Use this canonical form:

```text
T27S R31E, Willamette Meridian
T27S R31E Sec. 3, Willamette Meridian
T27S R31E Sec. 3 SW¼NE¼, Willamette Meridian
T27S R31E Sec. 3 Lot 4, Willamette Meridian
```

- Attach `T` and `R` directly to the number and direction, with one space between the
  township and range.
- Use no commas between township, range, section, and subdivision. Use exactly `Sec.`
  for a section and `Lot` for a government lot.
- Use `¼`, `½`, and `¾`. Write aliquot parts without spaces, in legal-description order;
  for example, `SW¼NE¼` is the southwest quarter of the northeast quarter.
- Use the full BLM principal-meridian name after a comma. Do not use variable
  abbreviations such as `W.M.`. The state is already represented by the Region hierarchy
  and is not repeated in the tag text.
- Do not pad numbers with zeroes.

The parser accepts common source variants such as `Township 27 South, Range 31 East`,
`T. 27 S., R. 31 E.`, `Section`, `1/4`, and an omitted principal meridian. Serialization
into a reviewed tag uses only the canonical form above.

## Coordinate provenance

Every populated `latitude` and `longitude` pair should be supported by at least one
coordinate-provenance tag. The tag records the evidence used:

- _CoordinatesFromName_ references a linked Name with coordinate evidence in a
  `Coordinates` or `LocationDetail` tag.
- _CoordinatesFromOccurrenceRecord_ records the identifier of a linked occurrence record
  with `Coordinates` or parseable `VerbatimCoordinates` evidence.
- _CoordinatesFromPLSS_ records the BLM township `plss_id` of the reviewed _PLSS_ tag
  whose polygon bounds supplied the coordinates.
- _CoordinatesFromGeoNames_ records a GeoNames identifier.
- _CoordinatesFromNominatim_ records the OpenStreetMap object type, object identifier,
  category, and whether the point or bounding box was used. Do not store Nominatim's
  installation-specific `place_id`.
- _CoordinatesFromLocationName_ indicates that the canonical Location name itself has a
  coordinate modifier.
- _CoordinatesManual_ records a reviewed manual choice and requires a comment explaining
  its basis. Use it when the paper trail cannot be represented by one of the structured
  provenance tags—for example, a reviewed correction of a bad published coordinate or a
  coordinate recovered from an unparsed comment. The comment should name the evidence
  and preserve uncertainty or competing placements.
