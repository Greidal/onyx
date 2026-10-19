"""Exclusion patterns for hierarchical names such as folder paths or mailboxes."""

import fnmatch
import re


def normalize_exclusion_patterns(patterns: list[str] | None) -> list[str]:
    """Lower-cases patterns and drops blanks and outer separators."""
    return [
        cleaned
        for raw in patterns or []
        if (cleaned := raw.strip().strip("/.").lower())
    ]


def is_excluded(name: str, patterns: list[str], separators: str = "/") -> bool:
    """True if the name, one of its ancestors, or one of its segments matches a
    glob pattern from normalize_exclusion_patterns. Case-insensitive.

    For "Allgemein/CreativeCloud/logo.pdf":
    - "CreativeCloud" excludes every folder of that name and all below it,
    - "Allgemein/CreativeCloud" only that folder,
    - "*/CreativeCloud/*" and "*.pdf" work as globs.
    """
    if not patterns:
        return False
    lowered = name.strip(separators).lower()
    if not lowered:
        return False
    split = re.compile(f"[{re.escape(separators)}]")
    prefixes = [lowered[: m.start()] for m in split.finditer(lowered)] + [lowered]
    segments = split.split(lowered)
    for pattern in patterns:
        for candidate in (*prefixes, *segments):
            if fnmatch.fnmatchcase(candidate, pattern):
                return True
        # "x/*" should also exclude the folder "x" itself.
        if any(
            fnmatch.fnmatchcase(prefix + separators[0], pattern) for prefix in prefixes
        ):
            return True
    return False
