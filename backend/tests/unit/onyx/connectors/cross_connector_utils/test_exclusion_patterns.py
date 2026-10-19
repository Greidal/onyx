import pytest

from onyx.connectors.cross_connector_utils.exclusion_patterns import (
    is_excluded,
    normalize_exclusion_patterns,
)


def test_patterns_are_normalized() -> None:
    assert normalize_exclusion_patterns(["/Fotos/", " ", "INBOX.Trash.", ""]) == [
        "fotos",
        "inbox.trash",
    ]
    assert normalize_exclusion_patterns(None) == []


@pytest.mark.parametrize(
    "path, pattern, excluded",
    [
        # A plain name excludes that folder anywhere, and everything below it.
        ("Allgemein/CreativeCloud/logo.pdf", "creativecloud", True),
        ("Allgemein/CreativeCloudAlt/logo.pdf", "creativecloud", False),
        # A path only that folder.
        ("Allgemein/CreativeCloud/logo.pdf", "allgemein/creativecloud", True),
        ("Ressorts/CreativeCloud/logo.pdf", "allgemein/creativecloud", False),
        # Globs.
        ("Allgemein/CreativeCloud/logo.pdf", "*/creativecloud/*", True),
        ("Allgemein/CreativeCloud", "allgemein/creativecloud/*", True),
        ("Ressorts/MK/Satz.indd", "*.indd", True),
        ("Ressorts/MK/Satz.pdf", "*.indd", False),
    ],
)
def test_paths(path: str, pattern: str, excluded: bool) -> None:
    assert is_excluded(path, [pattern]) is excluded


def test_case_insensitive_and_leading_slash() -> None:
    assert is_excluded("/Fotos/2025/a.jpg", normalize_exclusion_patterns(["FOTOS"]))


@pytest.mark.parametrize(
    "mailbox, pattern, excluded",
    [
        ("INBOX.Trash", "trash", True),
        ("INBOX.spambucket", "*spam*", True),
        ("INBOX.MAUS.SonstigerSpam", "*spam*", True),
        ("INBOX.CoT.Unzustellbar.Abmeldung newsletter", "unzustellbar", True),
        ("INBOX.MAUS.Räume", "*räume*", True),
        ("INBOX.MAUS.Anfragen&Angebote", "trash", False),
        ("INBOX/Sent", "inbox/sent", True),
    ],
)
def test_mailboxes(mailbox: str, pattern: str, excluded: bool) -> None:
    assert is_excluded(mailbox, [pattern], separators="./") is excluded


def test_no_patterns_exclude_nothing() -> None:
    assert not is_excluded("anything", [])
