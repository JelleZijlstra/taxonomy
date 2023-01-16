# Location

Locations are used to indicate the occurrence of taxa, mainly their type localities.

For extant taxa, we only use locations that coincide with the [region](region) they are
in. For example, all extant taxa with their type locality in the
[French Guiana](/r/French_Guiana) region are listed in the
[French Guiana](/l/French_Guiana) location. For fossil taxa, we instead create different
locations for each fossil site, generally with as much precision as the sources allow.
Type localities that are not precisely indicated or that need more investigation are
listed in more general locations. These may represent all fossils found within a
particular region and stratigraphic unit (e.g.,
[Morrison Formation (Wyoming)](</l/Morrison_Formation_(Wyoming)>)) or even all fossils
found within a region (e.g., [Wyoming fossil](/l/Wyoming_fossil)). Such general
locations should be replaced with more specific ones as we gather more information.

## Fields

Locations have the following fields:

- _name_: The name of the location.
- _region_: The [region](region) the location is physically in.
- _min period_ and _max period_: The youngest and oldest [period](period) the location
  is correlated with. Often these two will be the same, but sometimes the age of a
  location is only known to be within a range of several periods. These periods must be
  GTS units or biostratigraphic zones.
- _stratigraphic unit_: The stratigraphic unit, such as a formation, that the location
  derives from. This is also a [period](period).
- _tags_: Various extra information about the location. Current tags include _General_,
  which indicates the location is a general location like
  [Wyoming fossil](/l/Wyoming_fossil), the contents of which should be distributed to
  more precise locations, and three tags indicating that the location corresponds to a
  location in another database: _PBDB_ for the
  [Paleobiology Database](https://paleobiodb.org/#/), _NOW_ for the
  [New and Old Worlds](https://nowdatabase.org/) database, and _ETMNA_ for
  [Appendix I to Janis et al. (2008)](/a/North_America_Tertiary-localities.pdf).

There are a few other fields, but these are currently not widely used.
