from __future__ import annotations

from collections.abc import Sequence
from typing import Any

QUANTILE_MAP: dict[float, int] = {0.0: 0, 0.25: 1, 0.5: 2, 0.75: 3, 1.0: 4}


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def percentile_value(values: Sequence[Any], quantile: float) -> int:
    if not values:
        return 0
    idx = QUANTILE_MAP.get(float(quantile), 2)  # default: mediana
    if idx >= len(values):
        idx = len(values) - 1
    return _safe_int(values[idx])


def median_value(values: Sequence[Any]) -> int:
    return percentile_value(values, 0.5)


def quantiles_5(values: Sequence[Any]) -> tuple[int, int, int, int, int]:
    if not values:
        return (0, 0, 0, 0, 0)

    def at(i: int) -> int:
        j = min(i, len(values) - 1)
        return _safe_int(values[j])

    return (at(0), at(1), at(2), at(3), at(4))
