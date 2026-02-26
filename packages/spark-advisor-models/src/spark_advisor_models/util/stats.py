from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

QUANTILE_MAP: dict[float, int] = {0.0: 0, 0.25: 1, 0.5: 2, 0.75: 3, 1.0: 4}


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def percentile_value(values: Sequence[Any], quantile: float) -> float:
    if not values:
        return 0.0
    idx = QUANTILE_MAP.get(float(quantile))
    if idx is None:
        raise ValueError(f"Unsupported quantile: {quantile}. Supported: {sorted(QUANTILE_MAP)}")
    if idx >= len(values):
        idx = len(values) - 1
    return _safe_float(values[idx])


def median_value(values: Sequence[Any]) -> float:
    return percentile_value(values, 0.5)


def quantiles_5(values: Sequence[Any]) -> tuple[float, float, float, float, float]:
    if not values:
        return (0.0, 0.0, 0.0, 0.0, 0.0)

    def at(i: int) -> float:
        j = min(i, len(values) - 1)
        return _safe_float(values[j])

    return (at(0), at(1), at(2), at(3), at(4))
