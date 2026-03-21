import re

_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)")


def parse_spark_version(version: str) -> tuple[int, int] | None:
    """Parse Spark version string to (major, minor) tuple. Returns None if unparseable."""
    if not version:
        return None
    match = _VERSION_PATTERN.match(version.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))
