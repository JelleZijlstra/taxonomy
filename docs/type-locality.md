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

When sources consistently identify a named place, it is sufficient to represent that
place even if its modern identity or exact position is unknown. Use an _Unplaced_
Location under the narrowest Region supported by the evidence, retaining the source name
according to the Location naming conventions. Coordinates are not required. Do not leave
such a Name in a regionwide Location merely because the place has not yet been found on
a map.

`TypeTag.NoLocation` records that a particular source supplies no locality information.
It does not imply that other sources lack that information and is not a substitute for
`TypeTag.ImpreciseLocality`.

If a neotype has been validly designated, the type locality is the origin of the
neotype, and evidence prior to the designation of the neotype is not relevant.
Similarly, if a lectotype has been designated, the type locality is the origin of the
lectotype, even if other paralectotypes (previous syntypes) come from different areas.
If syntypes or otherwise unresolved original type material came from multiple places,
assign the Name to the lowest suitable Region-wide Location: the Recent Location named
exactly for its Region, or the "Region fossil" Location for fossil material. Add one
`TypeTag.PartialTypeLocality` for each individually known locality. At least two
distinct partial localities are required, and this representation is allowed only when
`species_type_kind` is `syntypes` or unset. A later lectotype designation replaces this
arrangement with the locality of the lectotype.

Conflicting accounts of the origin of a single type are not multiple type localities. Do
not represent competing interpretations with `PartialTypeLocality` tags; resolve the
conflict from the type evidence or leave it explicitly unresolved.

Many older names lack explicitly designated type specimens. In that case, the best
evidence for the type locality is the stated distribution of the animal. Sometimes later
authors will have "restricted" the type locality to a more specific area. Occasionally
this is based on historical evidence establishing where the species was first observed
or collected; at other times it is pure guesswork or convenience. The former category is
valid; the latter category is questionable but we often accept such "restrictions" for
convenience.

A documented, generally accepted restriction may be followed without repeating the
historical investigation when there is no conflicting type evidence. Cite the source and
identify the assignment as a later restriction, rather than presenting it as the
locality stated in the original description. A restriction does not override the origin
of a validly designated neotype or lectotype.

For names based on domestic breeds without a more specific type origin, it is acceptable
to use the primary breeding area mentioned in the source, or the area suggested by the
scientific name. Record the basis of the choice; an exhaustive reconstruction of breed
history is unnecessary for this purpose.

## Evidence

Begin with evidence already stored on the Name, including sourced `LocationDetail` and
`SpecimenDetail` quotations and type-designation information. These can be sufficient to
assign a Location without reopening the cited publication. Preserve quotations as source
data and record geographic interpretations separately. Consult the publication or
additional sources when the stored evidence is incomplete, ambiguous, conflicting, or
depends on missing context; research should address a question that could change the
assignment.

This research order does not change the relative weight of evidence. The original
description is the starting point for the original type locality, subject to subsequent
neotype or lectotype designations. Other useful sources are those based directly on
information associated with the type specimens (such as museum catalogs), those that
refine the type locality based on explicitly cited primary information, and geographical
sources such as gazetteers that reflect expert study. Sources that merely refine or
restate a type locality without evidence are less persuasive, apart from accepted
restrictions used as described above.

For marine animals, follow the source's named geographic feature when assigning the
Location to a land or sea Region; see
[Coasts and offshore localities](location#coasts-and-offshore-localities). For
archaeological or subfossil types, represent the age of the material even when the taxon
is extant; see [Temporal context](location#temporal-context).

## Legacy and imprecise data

Historically, the database placed type localities of extant animals in broad Location
objects directly associated with a Region; all type localities in the same region would
be lumped together. Similarly, some fossil type localities are lumped in imprecise
localities, often named "Region Pleistocene" for Pleistocene fossils and "Region fossil"
for fossils of any age from that region. Over time these should be moved to more precise
localities.
