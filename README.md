_taxonomy_ is a tool for maintaining a taxonomic database. I use it for my personal
database of mammals and other animals. It is focused on providing comprehensive data
about the nomenclature of each name: type specimens, type localities, citations, type
species, and much more.

The database is publicly accessible at [hesperomys.com](http://hesperomys.com). The
frontend is maintained in the separate
[hesperomys](htttps://github.com/JelleZijlstra/hesperomys) project.

Main entry points:

- `python -m hsweb` will run a GraphQL server that the `hesperomys` frontend can
  communicate with to render the web app
- `python -m taxonomy.shell` will open a shell that enables editing the database. This
  is my primary interface for working with the database.
