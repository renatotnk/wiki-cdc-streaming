"""src/shared/text_normalization.py -- SPEC-phase3-silver-cdf.md Section 12."""

import unicodedata

from src.shared.text_normalization import normalize_nfc


def test_nfc_and_nfd_variants_of_the_same_title_normalize_to_the_same_value():
    nfc = unicodedata.normalize("NFC", "Café Müller")
    nfd = unicodedata.normalize("NFD", "Café Müller")
    assert nfc != nfd  # distinct byte sequences before normalization

    assert normalize_nfc(nfc) == normalize_nfc(nfd)


def test_already_nfc_text_is_unchanged():
    value = unicodedata.normalize("NFC", "北京市")
    assert normalize_nfc(value) == value


def test_none_is_passed_through():
    assert normalize_nfc(None) is None


def test_multilingual_and_emoji_content_is_preserved_not_stripped():
    value = "مقالة عن ويكيبيديا 🎉"
    assert normalize_nfc(value) == unicodedata.normalize("NFC", value)
