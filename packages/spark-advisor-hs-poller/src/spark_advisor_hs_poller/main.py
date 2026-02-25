from fastapi import FastAPI

from spark_advisor_hs_poller.config import PollerSettings
from spark_advisor_hs_poller.lifecycle_manager import LifecycleManager
from spark_advisor_shared.health.fastapi_health import create_health_router


_settings = PollerSettings()

def create_app() -> FastAPI:
    service = LifecycleManager(_settings)
    app = FastAPI(title="spark-advisor-hs-poller", lifespan=service.lifespan)
    health_router = create_health_router(
        readiness_checks={
            "history_server": service.check_history_server,
            "kafka": service.check_kafka,
        }
    )
    app.include_router(health_router)
    return app


def main() -> None:
    import uvicorn

    app = create_app()
    uvicorn.run(app, host=_settings.server_host, port=_settings.server_port)
