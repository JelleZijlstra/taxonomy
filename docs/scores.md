One of my long-term goals is to record comprehensive nomenclatural data about covered
groups, especially mammals. This includes recording the type locality, type specimen,
original name, and various other fields, and locating a copy of the original description
("original_citation" field).

As a tool to track my progress towards these goals, I have some code that counts what
percentage of names in a group have all the required data. This page summarizes some
output from these scripts, as of March 13, 2023 (database version 23.3.0).

Some highlights:

- Type specimen is recorded for 64% of extant mammals and 65% of all mammals
- Type locality is recorded for 99.8% of extant mammals and 92% of all mammals
- Original citation is recorded for 72% of extant mammals and 71% of all mammals
- Among names for which the original citation is recorded, a raw citation
  ("verbatim_citation") is recorded for 95% of extant mammals and 90% of all mammals.

Notes on how to read the scores below:

- Percentages are computed among eligible names only, e.g. the "type" field is
  applicable only to family-group and genus-group names that are not emendations, nomina
  nuda, or similar.
- Fields with 100% coverage are omitted (e.g., most lists below omit the
  "corrected_original_name" field, which is virtually always set when applicable).
- The "Overall score" computes the overall proportion of data recorded across all
  fields. It is not directly interpretable and can change over time as I add or remove
  fields, but it is helpful to provide a way to compare across groups.

## Scores for all mammals

### Extant only

- Total names: 40931 (high: 1041, family: 3251, genus: 4825, species: 31814)
- `type_specimen`: 16765 of 26295 (63.76%)
- `collection`: 17579 of 26879 (65.40%)
- `genus_type_kind`: 2095 of 3065 (68.35%)
- `original_citation`: 29648 of 40931 (72.43%)
- `type`: 6040 of 6418 (94.11%)
- `original_rank`: 38590 of 40781 (94.63%)
- `verbatim_citation`: 10685 of 11283 (94.70%)
- `page_described`: 38966 of 40931 (95.20%)
- `name_complex`: 3712 of 3829 (96.94%)
- `species_name_complex`: 27815 of 28484 (97.65%)
- `type_tags`: 27456 of 27815 (98.71%)
- `original_name`: 40781 of 40931 (99.63%)
- `type_locality`: 26809 of 26879 (99.74%)
- `year`: 40930 of 40931 (100.00%)
- Overall score: 92.10

### Fossil only

- Total names: 34456 (high: 257, family: 2226, genus: 8499, species: 23474)
- `type_specimen`: 14747 of 22450 (65.69%)
- `collection`: 14899 of 22618 (65.87%)
- `original_citation`: 23624 of 34455 (68.56%)
- `genus_type_kind`: 5489 of 7434 (73.84%)
- `type_locality`: 18734 of 22618 (82.83%)
- `type_tags`: 21371 of 25657 (83.30%)
- `page_described`: 29256 of 34455 (84.91%)
- `verbatim_citation`: 9268 of 10831 (85.57%)
- `type`: 9613 of 10106 (95.12%)
- `original_rank`: 31641 of 33169 (95.39%)
- `original_name`: 33170 of 34455 (96.27%)
- `species_name_complex`: 22068 of 22789 (96.84%)
- `name_complex`: 8152 of 8212 (99.27%)
- `year`: 34324 of 34455 (99.62%)
- `corrected_original_name`: 33169 of 33170 (100.00%)
- Overall score: 88.61

### All age classes

- Total names: 76147 (high: 1299, family: 5542, genus: 13503, species: 55803)
- `type_specimen`: 31776 of 49210 (64.57%)
- `collection`: 32754 of 49973 (65.54%)
- `original_citation`: 53765 of 76146 (70.61%)
- `genus_type_kind`: 7684 of 10623 (72.33%)
- `verbatim_citation`: 20163 of 22381 (90.09%)
- `page_described`: 68887 of 76146 (90.47%)
- `type_tags`: 49256 of 53996 (91.22%)
- `type_locality`: 45930 of 49973 (91.91%)
- `type`: 15842 of 16729 (94.70%)
- `original_rank`: 70920 of 74689 (94.95%)
- `species_name_complex`: 50348 of 51758 (97.28%)
- `original_name`: 74690 of 76146 (98.09%)
- `name_complex`: 12012 of 12196 (98.49%)
- `year`: 76013 of 76146 (99.83%)
- `corrected_original_name`: 74689 of 74690 (100.00%)
- Overall score: 90.44

### Extant mammal completion rates by order

This section lists the percent completion rate for various fields by order among extant
mammals. For example, the line "Pholidota 12.82 (5/39) 100" means that among 100 total
names in Pholidota, 39 require a type specimen (they are species-group names that are
not emendations, nomina nuda, etc.), and for 5 (12.82%) the database records a type
specimen.

For the most part, the trend is that scores are better for small mammals (e.g.,
Chiroptera) and worse for large mammals (e.g., Artiodactyla). This partly reflects my
interests and partly reflects the fact that descriptions of large mammal names are more
likely to be in old or difficult-to-access literature.

### type_specimen

- Pholidota 12.82 (5/39) 100
- Proboscidea 14.89 (7/47) 99
- Tubulidentata 15.79 (3/19) 35
- Perissodactyla 20.00 (61/305) 674
- Sirenia 20.69 (6/29) 112
- Cingulata 34.78 (40/115) 267
- Pilosa 36.36 (36/99) 240
- Artiodactyla 37.59 (1214/3230) 5938
- Carnivora 46.01 (1558/3386) 5572
- Hyracoidea 46.72 (64/137) 169
- Monotremata 48.48 (16/33) 108
- Notoryctemorphia 50.00 (1/2) 12
- Primates 51.37 (880/1713) 3053
- Diprotodontia 52.42 (249/475) 890
- Afrosoricida 54.07 (73/135) 266
- Macroscelidea 67.59 (73/108) 157
- Peramelemorphia 68.52 (37/54) 129
- Lagomorpha 69.45 (441/635) 845
- Eulipotyphla 70.80 (1101/1555) 2069
- Dermoptera 73.08 (19/26) 65
- Microbiotheria 75.00 (3/4) 17
- Rodentia 75.85 (7739/10203) 13659
- Dasyuromorphia 76.62 (118/154) 269
- Chiroptera 79.57 (2637/3314) 5014
- Didelphimorphia 79.77 (272/341) 605
- Scandentia 85.34 (99/116) 179
- Paucituberculata 100.00 (13/13) 32

### original_citation

- Notoryctemorphia 33.33 (4/12) 12
- Tubulidentata 45.71 (16/35) 35
- Proboscidea 46.46 (46/99) 99
- Monotremata 52.78 (57/108) 108
- Sirenia 54.46 (61/112) 112
- Cingulata 64.04 (171/267) 267
- Dasyuromorphia 65.06 (175/269) 269
- Pilosa 65.83 (158/240) 240
- Diprotodontia 65.84 (586/890) 890
- Eulipotyphla 65.93 (1364/2069) 2069
- Peramelemorphia 66.67 (86/129) 129
- Perissodactyla 66.91 (451/674) 674
- Artiodactyla 67.45 (4005/5938) 5938
- Primates 69.54 (2123/3053) 3053
- Lagomorpha 70.53 (596/845) 845
- Carnivora 70.98 (3955/5572) 5572
- Hyracoidea 72.19 (122/169) 169
- Afrosoricida 74.81 (199/266) 266
- Rodentia 76.01 (10382/13659) 13659
- Microbiotheria 76.47 (13/17) 17
- Chiroptera 77.30 (3876/5014) 5014
- Scandentia 78.77 (141/179) 179
- Macroscelidea 78.98 (124/157) 157
- Dermoptera 81.54 (53/65) 65
- Didelphimorphia 84.46 (511/605) 605
- Pholidota 86.00 (86/100) 100
- Paucituberculata 93.75 (30/32) 32

### type_locality

I have been making a concerted effort to record type localities for every extant mammal.
There are significant gaps remaining among the ungulates, primarily in the horses and
the domesticated bovids, and a handful of names have remained elusive in other groups.

- Perissodactyla 91.22 (291/319) 674
- Artiodactyla 98.98 (3301/3335) 5938
- Macroscelidea 99.07 (107/108) 157
- Rodentia 99.94 (10290/10296) 13659
- Chiroptera 99.97 (3396/3397) 5014
- Notoryctemorphia 100.00 (2/2) 12
- Microbiotheria 100.00 (4/4) 17
- Paucituberculata 100.00 (13/13) 32
- Tubulidentata 100.00 (20/20) 35
- Dermoptera 100.00 (26/26) 65
- Sirenia 100.00 (33/33) 112
- Monotremata 100.00 (36/36) 108
- Pholidota 100.00 (39/39) 100
- Proboscidea 100.00 (47/47) 99
- Peramelemorphia 100.00 (57/57) 129
- Pilosa 100.00 (99/99) 240
- Scandentia 100.00 (118/118) 179
- Cingulata 100.00 (120/120) 267
- Hyracoidea 100.00 (138/138) 169
- Afrosoricida 100.00 (138/138) 266
- Dasyuromorphia 100.00 (163/163) 269
- Didelphimorphia 100.00 (346/346) 605
- Diprotodontia 100.00 (489/489) 890
- Lagomorpha 100.00 (641/641) 845
- Eulipotyphla 100.00 (1574/1574) 2069
- Primates 100.00 (1805/1805) 3053
- Carnivora 100.00 (3508/3508) 5572

## Nonmammals

This section lists scores for a few taxa outside Mammalia that have reasonably good
coverage in the database.

### Testudinata

- Total names: 5284 (high: 188, family: 455, genus: 987, species: 3654)
- `original_citation`: 2638 of 5284 (49.92%)
- `genus_type_kind`: 546 of 835 (65.39%)
- `type_specimen`: 2048 of 3037 (67.43%)
- `collection`: 2281 of 3215 (70.95%)
- `page_described`: 4475 of 5284 (84.69%)
- `species_name_complex`: 3017 of 3292 (91.65%)
- `name_complex`: 838 of 912 (91.89%)
- `original_rank`: 4951 of 5266 (94.02%)
- `type_locality`: 3033 of 3215 (94.34%)
- `type`: 1236 of 1294 (95.52%)
- `type_tags`: 3484 of 3605 (96.64%)
- `verbatim_citation`: 2632 of 2646 (99.47%)
- `original_name`: 5266 of 5284 (99.66%)
- `year`: 5283 of 5284 (99.98%)
- Overall score: 89.48

### Gymnophiona

- Total names: 380 (high: 27, family: 21, genus: 53, species: 279)
- `original_citation`: 195 of 380 (51.32%)
- `name_complex`: 42 of 51 (82.35%)
- `collection`: 232 of 272 (85.29%)
- `type_specimen`: 228 of 267 (85.39%)
- `original_rank`: 334 of 376 (88.83%)
- `page_described`: 353 of 380 (92.89%)
- `genus_type_kind`: 43 of 46 (93.48%)
- `species_name_complex`: 261 of 275 (94.91%)
- `original_name`: 376 of 380 (98.95%)
- Overall score: 92.41

### Pterosauria

- Total names: 752 (high: 47, family: 95, genus: 240, species: 370)
- `original_citation`: 517 of 752 (68.75%)
- `genus_type_kind`: 169 of 227 (74.45%)
- `page_described`: 613 of 752 (81.52%)
- `type_specimen`: 286 of 335 (85.37%)
- `collection`: 310 of 351 (88.32%)
- `original_rank`: 720 of 752 (95.74%)
- `type_tags`: 490 of 507 (96.65%)
- `species_name_complex`: 344 of 353 (97.45%)
- `type_locality`: 346 of 351 (98.58%)
- `verbatim_citation`: 233 of 235 (99.15%)
- `name_complex`: 233 of 234 (99.57%)
- Overall score: 93.25

### Ichthyosauria

- Total names: 429 (high: 22, family: 33, genus: 104, species: 270)
- `original_citation`: 235 of 429 (54.78%)
- `type_specimen`: 142 of 244 (58.20%)
- `collection`: 147 of 252 (58.33%)
- `genus_type_kind`: 56 of 91 (61.54%)
- `type_tags`: 277 of 316 (87.66%)
- `type_locality`: 222 of 252 (88.10%)
- `page_described`: 380 of 429 (88.58%)
- `original_rank`: 389 of 417 (93.29%)
- `species_name_complex`: 239 of 253 (94.47%)
- `type`: 124 of 130 (95.38%)
- `original_name`: 417 of 429 (97.20%)
- `verbatim_citation`: 189 of 194 (97.42%)
- `year`: 428 of 429 (99.77%)
- Overall score: 87.97

### Saurischia (except Neornithes)

- Total names: 2830 (high: 142, family: 213, genus: 1122, species: 1353)
- `original_citation`: 2173 of 2830 (76.78%)
- `page_described`: 2288 of 2830 (80.85%)
- `genus_type_kind`: 864 of 1062 (81.36%)
- `collection`: 1127 of 1310 (86.03%)
- `type_specimen`: 1126 of 1301 (86.55%)
- `type_tags`: 1922 of 2075 (92.63%)
- `type_locality`: 1235 of 1310 (94.27%)
- `original_rank`: 2707 of 2807 (96.44%)
- `species_name_complex`: 1293 of 1315 (98.33%)
- `original_name`: 2807 of 2830 (99.19%)
- `type`: 1267 of 1275 (99.37%)
- `name_complex`: 1093 of 1098 (99.54%)
- `year`: 2828 of 2830 (99.93%)
- Overall score: 93.50

### Crocodylomorpha

- Total names: 1668 (high: 48, family: 137, genus: 499, species: 984)
- `original_citation`: 1072 of 1668 (64.27%)
- `type_specimen`: 629 of 915 (68.74%)
- `collection`: 653 of 936 (69.76%)
- `genus_type_kind`: 309 of 437 (70.71%)
- `page_described`: 1336 of 1668 (80.10%)
- `type_locality`: 818 of 936 (87.39%)
- `type_tags`: 1050 of 1174 (89.44%)
- `type`: 571 of 600 (95.17%)
- `species_name_complex`: 906 of 945 (95.87%)
- `original_rank`: 1579 of 1639 (96.34%)
- `original_name`: 1639 of 1668 (98.26%)
- `name_complex`: 477 of 479 (99.58%)
- `verbatim_citation`: 594 of 596 (99.66%)
- Overall score: 89.80

### Loricifera

- Total names: 69 (high: 2, family: 3, genus: 17, species: 47)
- `type_specimen`: 44 of 46 (95.65%)
- Overall score: 99.76
