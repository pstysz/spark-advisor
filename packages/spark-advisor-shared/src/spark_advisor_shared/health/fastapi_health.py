from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter

if TYPE_CHECKING:
    from collections.abc import Callable


def create_health_router(
    readiness_checks: dict[str, Callable[[], bool]] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/health", tags=["health"])
    checks = readiness_checks or {}

    @router.get("/live")
    def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    def readiness() -> dict[str, Any]:
        results: dict[str, str] = {}
        all_ok = True
        for name, check_fn in checks.items():
            try:
                ok = check_fn()
            except Exception:
                ok = False
            results[name] = "ok" if ok else "fail"
            if not ok:
                all_ok = False

        return {
            "status": "ok" if all_ok else "degraded",
            "checks": results,
        }

    return router
