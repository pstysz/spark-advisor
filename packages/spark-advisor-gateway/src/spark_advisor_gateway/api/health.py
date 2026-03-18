from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request

from spark_advisor_gateway.config import StateKey

if TYPE_CHECKING:
    from spark_advisor_gateway.task.store import TaskStore


def create_health_router() -> APIRouter:
    router = APIRouter(prefix="/health", tags=["health"])

    @router.get("/live")
    def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    async def readiness(request: Request) -> dict[str, object]:
        nc = getattr(request.app.state, StateKey.NC)
        nats_ok: bool = nc is not None and nc.is_connected

        db_ok = False
        store: TaskStore | None = getattr(request.app.state, StateKey.TASK_STORE, None)
        if store is not None:
            with contextlib.suppress(Exception):
                db_ok = await store.check_health()

        all_ok = nats_ok and db_ok
        status = "ok" if all_ok else "degraded"
        return {"status": status, "checks": {"nats": nats_ok, "database": db_ok}}

    return router
