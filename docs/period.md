# Period

Periods are slices of the Earth's history that are used to date fossils. They come in
two main kinds:

- The units of the
  [Geological Time Scale](https://www.geosociety.org/GSA/Education_Careers/Geologic_Time_Scale/GSA/timescale/home.aspx)
  (GTS), as defined by the Geological Society of America. Not all units are currently
  included in the database, because I have had little need for the pre-Triassic ones.
- Various local or regional biostratigraphic zonations, such as the continental land
  mammal age systems. These are discussed in more detail in "[Chronology](chronology)".

## Fields

Periods have the following fields:

- _name_: The name of the period.
- _system_: The kind of period, which is either _gts_ (units of the GTS, such as
  [Eocene](/p/Eocene)) or one of several biostratigraphic options, such as _nalma_ for
  the [Wasatchian](/p/Wasatchian).
- _rank_: The rank of the unit within its system, such as _epoch_ for the GTS.
- _parent_: The parent unit of this unit, such as [Late Triassic](/p/Late_Triassic) for
  the [Rhaetian](/p/Rhaetian).
- _prev_: The unit preceding or underlying this one, such as [Norian](/p/Norian) for the
  Rhaetian.
- _next_: The unit succeeding or overlying this one, such as [Hettangian](/p/Hettangian)
  for the Rhaetian.
- _min age_: The minimum possible age of this period, in millions of years.
- _max age_: The maximum possible age of this period, in millions of years.
- _min period_: The youngest GTS period that this period is correlated to. For example,
  the [Blancan](/p/Blancan) North American Land Mammal Age correlates approximately to
  the [Zanclean](/p/Zanclean) through [Gelasian](/p/Gelasian) ages, so its min_period is
  the Gelasian.
- _max period_: The oldest GTS period that this period is correlated to.
- _region_: The [region](region) that this period exists in.
- _comment_: Any comment on the period, usually containing references to relevant
  literature.
