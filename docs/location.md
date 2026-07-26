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

## Naming convention

Location names have up to three parts:

```text
Name (disambiguator): modifier
```

The _name_ is preferably a geographic feature such as a town, river, mountain, cave, or
collecting site. The _disambiguator_ and _modifier_ are optional. Parentheses are
reserved for disambiguators, and modifiers follow a colon and a space.

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

The modifier distinguishes a more specific place associated with the base feature. It
may be free-form text such as `mouth`, `upper`, or `near the bridge`. A completely
specified distance and direction is standardized using abbreviated units and compass
directions, without _of_; north/south components precede east/west components. Thus, use
`Monterey: 2 km S 1 mi W`, not `1 mi W, 2 kilometers south of Monterey` or
`Monterey (2 km S 1 mi W)`.

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

Except for coordinate-defined localities described above, coordinates, elevations,
source wording, and other evidence do not belong in the modifier merely to make a name
unique. Store them in their structured fields or in `location_detail`. Some numbered
sites, quarries, camps, and similar localities have no useful geographic anchor; they
may use their established site name as the base name.

## Fields

Locations have the following fields:

- _name_: The name of the location.
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
- _tags_: Various extra information about the location. Current tags include _General_,
  which indicates that the Location intentionally represents a broad or imprecise area
  rather than a single locality, and three tags indicating that the location corresponds
  to a location in another database: _PBDB_ for the
  [Paleobiology Database](https://paleobiodb.org/#/), _NOW_ for the
  [New and Old Worlds](https://nowdatabase.org/) database, and _ETMNA_ for
  [Appendix I to Janis et al. (2008)](/a/North_America_Tertiary-localities.pdf).

There are a few other fields, but these are currently not widely used.
