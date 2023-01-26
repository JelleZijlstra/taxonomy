# Taxon

Taxa are the units of taxonomy, such as species, families, and unranked clades. They are
important as an organizational tool because their organization defines the taxonomy we
use to retrieve names in the database, but the database does not record much information
directly about taxa; our focus is on nomenclatural data such as the type locality and
type specimen, which is a property of the [name](name), not the taxon.

## Fields

Taxa have the following fields:

- _valid name_: The current name of the taxon, as a string.
- _base name_: Reference to the [name](name) that forms the basis of this taxon. For
  example, for the taxon [_Agathaeromys_](/t/Agathaeromys), the base name is the name
  [_Agathaeromys_](/n/Agathaeromys) Zijlstra et al., 2010. Sometimes multiple taxa share
  the same base name: the base name of both the family [Muridae](/t/Muridae) and the
  subfamily [Murinae](/t/Murinae) is [Murina](/n/50456) Illiger, 1811.
- _rank_: The rank of the taxon, such as _order_ or _species_. The special _unranked_
  rank is used for unranked clades.
- _parent_: The taxon that includes this taxon. This is not set for the root of the
  taxon hierarchy, which is called [root](/t/root).
- _age_: The kind of material that this taxon is based on and whether it is still
  around. The most common options are _extant_ and _fossil_. The options are:
  - _extant_: The taxon is currently extant. Example: [_Homo sapiens_](/t/Homo_sapiens)
  - _recently_extinct_: The taxon went extinct after the year 1500. This matches the
    common cutoff for databases of extant taxa (e.g., the IUCN Red List and the Mammal
    Diversity Database), so keeping this separate from the next one (for earlier
    Holocene extinctions) makes it easier to compare taxonomies among these databases.
    Example: [_Thylacinus cynocephalus_](/t/Thylacinus_cynocephalus).
  - _holocene_: The taxon survived into the [Holocene](/p/Holocene), but went extinct
    before 1500. Example: [_Mammuthus primigenius_](/t/Mammuthus_primigenius).
  - _fossil_: The taxon is known only from pre-Holocene body fossils. Example:
    [_Tyrannosaurus rex_](/t/Tyrannosaurus_rex).
  - _egg_: The taxon is based on a fossil egg. Example:
    [_Dendroolithus dendriticus_](/t/Dendroolithus_dendriticus).
  - _track_: The taxon is based on fossil footprints. Example:
    [_Grallator cursorius_](/t/Grallator_cursorius).
  - _coprolite_: The taxon is based on fossil feces. Example:
    [_Revueltobromus complexus_](/t/Revueltobromus_complexus).
  - _burrow_: The taxon is based on a fossil burrow. Example:
    [_Daimonelix_](/t/Daimonelix).
  - _bite trace_: The taxon is based on a fossil biting trace. Example:
    [_Brutalichnus brutalis_](/t/Brutalichnus_brutalis).
  - _ichno_: Other kinds of trace fossils, such as the resting trace
    [_Ursalveolous carpathicus_](/t/Ursalveolous_carpathicus).
  - _redirect_: The taxon was merged into another one in the database.
  - _removed_: The taxon has been removed from the database. (Such taxa are not actually
    removed to avoid breaking links.)
