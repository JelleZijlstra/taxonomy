# Region

A region is a part of the world that is recognized as separate for the purposes of this database.
All independent countries are regions, as are subdivisions of many countries, as explained in
detail in "[Geography](geography)".

## Fields

Regions have the following fields:

- _name_: The name of the region. This should be the normal English-language
  name for the region; in practice we'll almost certainly follow whatever
  Wikipedia uses as the article title.
- _kind_: The kind of region. Currently, the options are "continent", "country",
  "subnational", "planet", "other", and "county"; a more detailed nomenclature
  may prove useful.
- _parent_: The region that includes this region. This is always set except
  for [Earth](/r/Earth), the root of the region tree.
- _comment_: A comment on the region, which may explain its definition.
