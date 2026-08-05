# Type localities

The database contains data about numerous type localities, expressed as a link from a
[name](/docs/name) to a [location](/docs/location).

This document sets out how the database treats type localities; refer also to the
Location documentation for information on how Locations are named and organized.

## What is the type locality?

According to the Code:

> The type locality of a nominal species-group taxon is the geographical (and, where
> relevant, stratigraphical) place of capture, collection or observation of the
> name-bearing type; if there are syntypes and no lectotype has been designated, the
> type locality encompasses the localities of all of them

Thus, the type locality is the place of origin of the type specimen(s).

The simplest case is if there is a single type specimen from a precisely known place. In
that case, the type locality should be set to a Location representing that place.
Sometimes the origin is not precisely known, in which case a _general_ Location can be
used, one representing a larger geographic area. If possible, prefer reusing existing
general Locations with modern names over creating separate Locations for historical,
essentially synonymous geographical names. In particular, prefer reusing an existing
general Location that corresponds exactly to its Region where feasible. If the known
area has a name but spans multiple child Regions, create or reuse a general Location for
that area directly under the lowest Region that contains the whole area.

A general Location represents a known but broad geographic area. An _Unplaced_ Location
instead represents a specific locality named by a source whose physical position has not
been identified. The `TypeTag.ImpreciseLocality` tag applies to a Name whose general
type locality cannot be made more precise from the available evidence. It does not
replace the linked general Location and should not be used for a specific but
geographically unresolved locality.

If a neotype has been validly designated, the type locality is the origin of the
neotype, and evidence prior to the designation of the neotype is not relevant.
Similarly, if a lectotype has been designated, the type locality is the origin of the
lectotype, even if other paralectotypes (previous syntypes) come from different areas.
If there are syntypes, they may come from different places. In that case, a general
Location can be created, named e.g. "A and B".

Many older names lack explicitly designated type specimens. In that case, the best
evidence for the type locality is the stated distribution of the animal. Sometimes later
authors will have "restricted" the type locality to a more specific area. Occasionally
this is based on historical evidence establishing where the species was first observed
or collected; at other times it is pure guesswork or convenience. The former category is
valid; the latter category is questionable but we often accept such "restrictions" for
convenience.

## Evidence

The first evidence for the type locality that should be consulted is the original
description of the animal. Other useful sources are those that are based directly on
information associated with the type specimens (such as museum catalogs), those that
refine the type locality based on explicitly cited primary information, and geographical
sources such as gazetteers that reflect expert study. Sources that merely refine or
restate a type locality without evidence are less persuasive.

## Legacy and imprecise data

Historically, the database placed type localities of extant animals in broad Location
objects directly associated with a Region; all type localities in the same region would
be lumped together. Similarly, some fossil type localities are lumped in imprecise
localities, often named "Region Pleistocene" for Pleistocene fossils and "Region fossil"
for fossils of any age from that region. Over time these should be moved to more precise
localities.
