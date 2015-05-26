# Create some DB entries to see how it goes

import models
import constants

root_taxon = models.Taxon.create(rank=constants.ROOT, valid_name="root")

agath_t = models.Taxon.create(rank=constants.GENUS, valid_name="Agathaeromys", parent=root_taxon)
agath_n = models.Name.create(authority="Zijlstra, Madern & Van den Hoek Ostende", year=2010, group=constants.GROUP_GENUS, base_name="Agathaeromys", status=constants.STATUS_VALID, taxon=agath_t)
