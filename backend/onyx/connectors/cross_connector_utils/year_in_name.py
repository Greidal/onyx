"""Project year from folder, book or chapter names ("MKOR_26", "CONNECTA-2025")."""

import re

# "CONNECTA-2026", "Lageplan_2025", "Messe 2026"
_FOUR_DIGIT_YEAR = re.compile(r"(?<!\d)(20[0-3]\d)(?!\d)")
# "MKOR_26", "DES25", "Maus 26", "00_CAT26": two digits after a letter, at the end
_TWO_DIGIT_YEAR = re.compile(r"[A-Za-z][ _-]?(\d{2})$")


def year_in_name(name: str) -> str | None:
    if match := _FOUR_DIGIT_YEAR.search(name):
        return match.group(1)
    match = _TWO_DIGIT_YEAR.search(name.strip())
    if match and 15 <= int(match.group(1)) <= 39:
        return f"20{match.group(1)}"
    return None


def year_in_names(names: list[str]) -> str | None:
    """Year of the most specific name that has one (names go general to specific)."""
    for name in reversed(names):
        if year := year_in_name(name):
            return year
    return None
