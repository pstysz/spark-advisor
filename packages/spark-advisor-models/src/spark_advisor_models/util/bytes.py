import re

_MEMORY_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*([kmgt])?b?$", re.IGNORECASE)
_SUFFIX_TO_MB: dict[str, int] = {"m": 1, "g": 1024, "t": 1024 * 1024}


def parse_memory_string(value: str) -> int:
    """Parse Spark memory string (e.g. '1g', '512m', '2048') to megabytes."""
    if not value or not value.strip():
        return 0
    match = _MEMORY_PATTERN.match(value.strip())
    if not match:
        return 0
    number = float(match.group(1))
    suffix = (match.group(2) or "m").lower()
    if suffix == "k":
        return max(1, int(number / 1024))
    return int(number * _SUFFIX_TO_MB.get(suffix, 1))


def format_bytes(n: int | float) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"
