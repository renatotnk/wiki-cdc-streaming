"""Unicode NFC normalization, reused across phases.

Wikipedia titles/usernames can arrive NFC- or NFD-normalized depending on
the client that produced the edit (macOS filesystems and some IMEs favor
NFD). Two strings that render identically and *mean* the same page/user can
therefore compare unequal byte-for-byte -- not a validity problem (both are
perfectly legitimate multilingual text), but a uniqueness problem: dedup
keyed on raw string equality would treat them as distinct. Normalizing to
NFC before the uniqueness dedup (SPEC-phase3-silver-cdf.md Section 6/8)
collapses that distinction without touching the text's actual content or
rejecting anything.
"""

import unicodedata


def normalize_nfc(value: str | None) -> str | None:
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)
