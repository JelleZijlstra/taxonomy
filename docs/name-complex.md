# Name complex

A name complex is a group of genus names that share the same grammatical
characteristics, such as grammatical gender and stem. For example, all names ending with
_-therium_ form the [_therium_](/nc/therium) name complex; they are all neuter and have
a stem ending with _-theri-_.

## Fields

Name complexes have the following fields:

- _code article_: reference to the location in the Code that regulates this kind of name
  complex. See [below](#kinds-of-name-complexes) for more detail.
- _label_: a unique label that identifies the complex. For complexes based on Latin
  words, the label is the Latin word; for others it might be a brief description (e.g.,
  [defaulted_masculine](/nc/61)).
- _stem_: for name complexes based on Latin or Grek words, the common ending of names in
  the complex.
- _gender_: the grammatical gender of names in this name complex.
- _stem remove_ and _stem add_: the stem of family-group names based on names in this
  complex is formed by first removing the _remove_ string from the end of the name, then
  adding the _add_ string. For example, [_-therium_](/nc/therium) has _remove_ of _-um_
  and _add_ the empty string, so the family group stem of a name like
  [_Nyctitherium_](/n/Nyctitherium) is formed by removing _-um_, yielding _Nyctitheri-_.
  But for [_-odus_](/nc/odus), the _remove_ is _-us_ and the _add_ is _-ont_, so the
  stem of [_Ptilodus_](/n/Ptilodus) is formed by removing -_us_, then adding _-ont_,
  which yields _Ptilodont-_.
- _comment_: a comment, usually explaining the etymology of the name.

## Kinds of name complexes

We recognize name complexes based on the following articles of the _Code_:

- Art. 30.1.1: A Latin word, such as [_lorica_](/nc/lorica).
- Art. 30.1.2: An Ancient Greek word transcribed into Latin, such as
  [_therium_](/nc/therium), derived from Greek θηρίον.
- Art. 30.1.3: A Greek word with a Latinized ending, such as [_hyus_](/nc/hyus), an
  alteration of Greek ὗς. The distinction between this and the previous category is not
  always clear, but there is usually little practical difference between the two.
- Art. 30.1.4.2: Latin words of common gender, which default to the masculine. Currently
  the only example is [_otis_](/nc/otis).
- Art. 30.1.4.3: Names ending in _-ops_, which default to masculine. These form the
  "[_ops_ masculine](/nc/95)" complex.
- Art. 30.1.4.4: A few specified endings that result in masculine names, including
  [_oides_](/nc/oides).
- Art. 30.2.1: A gendered word in a modern European language written in the Latin
  alphabet. There are currently no examples in the database.
- Art. 30.2.2: The name is not based on a classical root, but the gender is expressly
  specified in the original description. There are different complexes for each
  combination of gender and stem, such as
  [expressly_specified_feminine_stem_a](/nc/351).
- Art. 30.2.3: The name is not explicitly specified, but is indicated by the gender of
  an included species. This similarly includes multiple complexes, such as
  [indicated_neuter](/nc/602). This complex includes the name [_Kuehneon_](/n/1699),
  which is neuter because it included a species named _duchyense_, a neuter form.
- Art. 30.2.4: If all else fails, genus names default to feminine if they end in _-a_,
  neuter if they end in _-um_, and masculine otherwise. There is no hard-and-fast rule
  to determine the stem for such names, so sometimes I have to guess based on the
  general form of the name and the treatment of similar names. An example complex is
  [defaulted_masculine_stem_oo](/nc/919).

I have encountered a few other categories that are useful in practice:

- _bad transliteration_: Names that are incorrectly transliterated from Greek, but that
  don't fall under Art. 30.1.3 (for example, _-merix_ instead of _-meryx_). The Code
  doesn't provide explicit guidance for such names, but I interpret them as having the
  gender of their Greek root. The question is usually academic because these names tend
  to be incorrect subsequent spellings.
- _unknown obvious stem_: Names that look classical in form and have an obvious stem,
  but the exact derivation of which is not clear. An example is
  [_madataeus_](/nc/madataeus): it is clearly a masculine noun with stem _-us_, but its
  exact derivation is not currently clear.
- _stem expressly set_: The stem has been expressly set to something different than
  expected. Usually this comes up when the family-group name associated with the name is
  otherwise preoccupied. For example, [_Ellobius_](/n/Ellobius) would be expected to be
  in the [_bius_](/nc/bius) name complex, but that would produce a stem _Ellobi-_, which
  conflicts with a different family-group name, so the stem has been set to _Ellobius-_
  and the name is in the [stem_expressly_set_masculine](/nc/937) complex.
