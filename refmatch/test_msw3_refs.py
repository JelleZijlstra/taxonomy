from refmatch import msw3_refs


def split_text(text: str) -> list[str]:
    text = msw3_refs.normalize_reference_text(text)
    reference = msw3_refs.Reference(msw3_refs.SECTION, text, text)
    return [ref.text for ref in msw3_refs.split_embedded_references(reference)]


def test_split_embedded_reference_with_bracketed_year() -> None:
    refs = split_text(
        "Clark, J. W. 1873a. On the skull of a seal. "
        "Proceedings of the Zoological Society of London, 1873:556-557. "
        "Clark, J. W. 1873b [1874]. On the eared seals of the Auckland Islands. "
        "Proceedings of the Zoological Society of London, 1873:750-760."
    )

    assert len(refs) == 2
    assert refs[0].startswith("Clark, J. W. 1873a.")
    assert refs[1].startswith("Clark, J. W. 1873b [1874].")


def test_split_multiple_embedded_references() -> None:
    refs = split_text(
        "Zholnerovskaya, E. I., D. I. Bibikov, and V. I. Ermolaev. 1990. "
        "Immunogeneticheskii analiz. Journal, 195:15-24. "
        "Zhou Jia-di, Li Si-hua, and Gu Jing-he. 1985. "
        "[The preliminary observation on the mammals]. Acta Theriologica Sinica, "
        "5(2):160. Zhou Kai-ya, Qian Wei-juan, and Li Yue-min. 1978. "
        "[Recent advances in the study of the baiji]. Journal, 1:8-13."
    )

    assert len(refs) == 3
    assert refs[1].startswith("Zhou Jia-di")
    assert refs[2].startswith("Zhou Kai-ya")


def test_do_not_split_author_list_continuation() -> None:
    refs = split_text(
        "Abe, H., S. Shiraishi, and S. Arai. 1991. "
        "A new mole from Uotsuri-jima, the Ryukyu Islands. "
        "Journal of the Mammalogical Society of Japan, 15:47-60."
    )

    assert len(refs) == 1


def test_st_leger_is_not_treated_as_initials() -> None:
    refs = split_text(
        "Saint Girons, M. C. 1972. Rectification a propos des auteurs. "
        "Mammalia, 36:166-167. St. Leger, J. 1930. "
        "On two species of Dendromus. Annals and Magazine of Natural History, "
        "ser. 10, 6:622."
    )

    assert len(refs) == 2
    assert refs[1].startswith("St. Leger, J. 1930.")


def test_split_word_seq_chapter_field() -> None:
    refs = split_text(
        r"Baker, A. N. 1985. Pygmy right whale - Caperea marginata. "
        r"Pp. 345-354, in Handbook of marine mammals: The sirenians and "
        r"baleen whales, (S. H. Ridgway and R. Harrison, eds.). "
        r"Academic Press, London, 3:1-362. SEQ CHAPTER \h \r 1"
        r"Baker, A. N., A. N. H. Smith, and F. B. Pichler. 2002. "
        r"Geographical variation in Hector's dolphin. Journal, 32:713-727."
    )

    assert len(refs) == 2
    assert refs[0].startswith("Baker, A. N. 1985.")
    assert refs[1].startswith("Baker, A. N., A. N. H. Smith")


def test_split_word_hyperlink_field_before_next_reference() -> None:
    refs = split_text(
        "Pérez, E. M. 1992. Agouti paca. Mammalian Species, 404:1-7. "
        'HYPERLINK "http://example.com" \\t "wsr" Pérez de Val, J., '
        "J. Juste, and J. Castroviejo. 1995. A review of Zenkerella insignis. "
        "Mammalia, 59(3):441-443."
    )

    assert len(refs) == 2
    assert refs[0].startswith("Pérez, E. M. 1992.")
    assert refs[1].startswith("Pérez de Val, J.")


def test_month_between_authors_and_year_is_reference_start() -> None:
    refs = split_text(
        "Fernández-Salvador, R. 2002. Microtus cabrerae Thomas, 1906. "
        "Pp. 386-389, in Atlas de los Mamíferos terrestres de España. "
        "Dirección General de Conservación de la Naturaleza-SECEM-SECEMU. "
        "Fernando, P., T. N. C. Vidya, J. Payne, M. Stuewe, G. Davison, "
        "R. J. Alfred, P. Andau, E. Bosi, A. Kilbourn, and D. J. Melnick."
        "August 2003. DNA Analysis indicates that Asian elephants are native "
        "to Borneo. PLoS Biology, http://biology.plosjournals.org."
    )

    assert len(refs) == 2
    assert refs[1].startswith("Fernando, P.")
