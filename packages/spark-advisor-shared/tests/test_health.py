from fastapi.testclient import TestClient

from spark_advisor_shared.health.fastapi_health import create_health_router


def _make_app():
    from fastapi import FastAPI

    app = FastAPI()
    return app


class TestHealthRouter:
    def test_liveness(self):
        app = _make_app()
        app.include_router(create_health_router())
        client = TestClient(app)
        response = client.get("/health/live")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_readiness_no_checks(self):
        app = _make_app()
        app.include_router(create_health_router())
        client = TestClient(app)
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_readiness_all_pass(self):
        app = _make_app()
        app.include_router(create_health_router({"kafka": lambda: True, "db": lambda: True}))
        client = TestClient(app)
        response = client.get("/health/ready")
        data = response.json()
        assert data["status"] == "ok"
        assert data["checks"]["kafka"] == "ok"
        assert data["checks"]["db"] == "ok"

    def test_readiness_one_fails(self):
        app = _make_app()
        app.include_router(create_health_router({"kafka": lambda: True, "db": lambda: False}))
        client = TestClient(app)
        response = client.get("/health/ready")
        data = response.json()
        assert data["status"] == "degraded"
        assert data["checks"]["kafka"] == "ok"
        assert data["checks"]["db"] == "fail"

    def test_readiness_check_exception(self):
        def _boom() -> bool:
            raise RuntimeError("connection failed")

        app = _make_app()
        app.include_router(create_health_router({"broken": _boom}))
        client = TestClient(app)
        response = client.get("/health/ready")
        data = response.json()
        assert data["status"] == "degraded"
        assert data["checks"]["broken"] == "fail"
