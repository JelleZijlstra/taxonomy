---
name: divide-region
description:
  Divide an existing Region into child subdivisions and produce staged, reviewable
  recommendations to redistribute Locations, type localities, Collections,
  CitationGroups, and other Region-linked data. Use when subdividing a country or other
  broad Region after the proposed child-region list needs human review.
---

# Divide Region

The database uses Regions for representing geographic areas in the context of type
localities, occurrences, etc. Generally, these represent political areas. This file
provides a workflow for going from a single broad region to smaller regions representing
subdivisions of the original region.

This workflow provides a way to divide a region into child regions, reassign data linked
to the larger region, and perform associated improvements on the linked data.

Before starting, read `docs/region.md`, `docs/location.md`, `docs/type-locality.md`, and
`skills/generate-recommendations/SKILL.md`, and follow their current conventions.

# 1. Identify proposed child regions

Figure out the child regions that should be added to the database. For a country, this
should generally be the first-level political subdivisions (e.g. states, provinces,
etc.). Create a recommendations manifest (using the skill in
`skills/generate-recommendations/SKILL.md`) that creates the new regions, and return to
the user asking to apply the proposed changes. The set of child regions should
completely divide the existing parent region. If the parent region currently has the
`IncompletelyDivided` tag, design a new region hierarchy that encompasses the existing
child regions. If any of the proposed regions have name collisions with similar regions
elsewhere in the world, add a disambiguator to the name.

# 2. Reassign data to the new regions

Once the user has created the new child regions, start working on a new manifest file
(again using the skill in `skills/generate-recommendations/SKILL.md`) that reassigns
data from the original region to the new child regions. Re-read the live database first
and verify the expected child names, IDs, kinds, and parent relationships. This will
involve:

- Citation groups: primarily journals and names of cities where books were published.
- Collections: museums where specimens are held. Use the `city` field on the collection.
- Precise Locations: Location objects that already refer to a precise site should be
  reassigned to the region where they are located. In some cases, you may be able to use
  existing Nominatim or GeoNames mappings to assign locations to child regions.
  Nearest-place matching is useful for generating a candidate, but validate the proposed
  child using the returned administrative hierarchy or boundary containment when
  available, especially near borders or for ambiguous names. Retain uncertain cases for
  manual review.
- General Locations: You'll often find type localities associated with a general
  locality for e.g. the whole country. Those should be reassigned to more precise
  Locations representing precise sites where possible. If there is not enough evidence
  to place them more precisely, they can stay in the higher-level Location. See
  `docs/location.md` for more information on how to organize locations.

Before researching individual rows, inventory ordinary references with
`region.get_direct_backrefs()`. In addition to the common objects above, this catches
rare `Specimen.region`, `Period.region`, and `StratigraphicUnit.region` references.
Audit these rather than assuming they must move: a Period or stratigraphic unit may
legitimately span multiple child Regions.

While researching locations, you may have to do more research to figure out where a
particular site is located in order to assign it to a child region. If you are able to
figure out the geographical coordinates of the site in the process, add a recommendation
that adds the coordinates to the Location object.

## Evidence

You may use any evidence inside or outside the data to inform your proposed
reassignments. It may often be helpful to look at existing `LocationDetail` tags on
linked type localities as many will contain the name of the administrative subdivision
where the site is located; though be careful because some of these may be outdated. You
can also use existing geographical coordinates on a Location, though sometimes these may
be inaccurate or imprecise.

## Validation

Follow the review, dry-run, and virtual-lint steps in the recommendation-generation
workflow for each manifest. Virtual lint is best-effort: database-wide queries cannot
see newly proposed child Regions. In particular, the first-stage virtual lint cannot
show the `fully_divided_region` findings that become active after the child Regions are
created. Do not treat a clean virtual-lint result as validation that the second-stage
redistribution is complete.

Reconcile the completed second-stage manifest with the initial inventory. Every valid
direct reference to the parent Region must have either a recommendation or a documented
reason to remain. Similarly, account for every Name linked to a general parent-level
Location that is being refined. Assert in the generator that every specific Location
directly assigned to the parent is moved to a child Region or changed to General or
Unplaced.
