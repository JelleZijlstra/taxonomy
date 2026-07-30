# Region

A region is a part of the world that is recognized as separate for the purposes of this
database. All independent countries are regions, as are subdivisions of many countries,
as explained in detail in "[Geography](geography)".

## Fields

Regions have the following fields:

- _name_: The name of the region. This should be the normal English-language name for
  the region; in practice we'll almost certainly follow whatever Wikipedia uses as the
  article title.
- _kind_: The kind of region. Options include "continent", "country", "subnational",
  "planet", "other", "county", and various other kinds of country subdivision.
- _parent_: The region that includes this region. This is always set except for
  [Earth](/r/Earth), the root of the region tree.
- _comment_: A comment on the region, which may explain its definition.
- _tags_: A list of tags applied to the region.

When a Region's child Regions exhaustively cover it, every Location directly assigned to
the parent must be _General_ (spanning multiple children) or _Unplaced_ (its child
Region is unknown). A Region whose children cover only part of it instead has the
`IncompletelyDivided` Region tag; specific Locations may remain directly assigned to the
uncovered part of such a Region.
