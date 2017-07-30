import peewee


def occ(t, loc, source=None, replace_source=False, **kwargs):
    if source is None:
        source = s
    try:
        o = t.at(loc)
    except Occurrence.DoesNotExist:
        o = t.add_occurrence(loc, source, **kwargs)
        print('ADDED: %s' % o)
    else:
        print('EXISTING: %s' % o)
        if replace_source and o.source != source:
            o.source = source
            o.s(**kwargs)
            o.save()
            print('Replaced source: %s' % o)
    return o

def occur(t, locs, source=None, replace_source=False, **kwargs):
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


def biggest_localities(limit=50):
    query = Location \
        .select(Location, peewee.fn.Count(Occurrence.id).alias('num_occurrences')) \
        .join(Occurrence, peewee.JOIN_LEFT_OUTER) \
        .group_by(Location.id) \
        .order_by(peewee.fn.Count(Occurrence.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_occurrences) for t in query]))


def biggest_ranges(limit=50):
    query = Taxon \
        .select(Taxon, peewee.fn.Count(Occurrence.id).alias('num_occurrences')) \
        .join(Occurrence, peewee.JOIN_LEFT_OUTER) \
        .group_by(Taxon.id) \
        .order_by(peewee.fn.Count(Occurrence.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_occurrences) for t in query]))


def mocc(t, locs, source=None, replace_source=False, **kwargs):
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)

def multi_taxon(ts, loc, source=None, replace_source=False, **kwargs):
    for t in ts:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)

def unrecorded_taxa(root):
    def has_occurrence(taxon):
        return taxon.occurrences.count()

    if root.age == AGE_FOSSIL:
        return

    if root.rank == SPECIES:
        if not has_occurrence(root) and not any(has_occurrence(child) for child in root.children):
            print(root)
    else:
        for taxon in root.children:
            unrecorded_taxa(taxon)

def move_localities(period):
    for location in Location.filter(Location.max_period == period, Location.min_period == period):
        location.min_period = location.max_period = None
        location.stratigraphic_unit = period
        location.save()

def move_to_stratigraphy(loc, period):
    loc.stratigraphic_unit = period
    loc.min_period = loc.max_period = None
    loc.save()

def h(author, year, uncited=False):
    query = Name.filter(Name.authority % author, Name.year == year)
    if uncited:
        query = query.filter(Name.original_citation >> None)
    return list(query)
