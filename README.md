*taxonomy* is a tool for maintaining a taxonomic database. I use it
for my personal database of mammals and other animals. It is focused
on providing comprehensive data about the nomenclature of each name:
type specimens, type localities, citations, type species, and much
more.

This is a personal project and so far I haven't spent any effort to
make it usable by other people. It will probably work if you
`pip install` it from source, but you'll have to make a database
yourself.

The database is currently in SQLite format because that's the most
convenient for my current setup, but this isn't essential to the
project. It was previously in a MYSQL database (which worked mostly
fine), and before that in a set of spreadsheets (which I'm glad I
no longer need).

In the future I plan to extend this project with a website enabling
people to browse the taxonomy.