from fastapi import FastAPI

from spark_advisor.config import AnalyzerSettings
from spark_advisor.lifecycle_manager import LifecycleManager
from spark_advisor_shared.health.fastapi_health import create_health_router

_settings = AnalyzerSettings()


def create_app() -> FastAPI:
    service = LifecycleManager(_settings)
    app = FastAPI(title="spark-advisor", lifespan=service.lifespan)
    health_router = create_health_router(
        readiness_checks={
            "kafka": service.check_kafka,
        }
    )
    app.include_router(health_router)
    return app


def main() -> None:
    import uvicorn

    app = create_app()
    uvicorn.run(app, host=_settings.server_host, port=_settings.server_port)
