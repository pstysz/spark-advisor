from fastapi import APIRouter, Request

from spark_advisor_gateway.config import StateKey


def create_health_router() -> APIRouter:
    router = APIRouter(prefix="/health", tags=["health"])

    @router.get("/live")
    def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/ready")
    async def readiness(request: Request) -> dict[str, object]:
        nc = getattr(request.app.state, StateKey.NC)
        nats_ok: bool = nc is not None and nc.is_connected
        status = "ok" if nats_ok else "degraded"
        return {"status": status, "checks": {"nats": nats_ok}}

    return router
