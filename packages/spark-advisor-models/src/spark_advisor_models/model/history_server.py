from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class Attempt(BaseModel):
    model_config = ConfigDict(frozen=True, alias_generator=to_camel, populate_by_name=True)

    attempt_id: str | None = None
    start_time: str = ""
    end_time: str = ""
    last_updated: str = ""
    duration: int = 0
    spark_user: str = ""
    completed: bool = False
    app_spark_version: str = ""
    log_path: str = ""
    start_time_epoch: int = 0
    end_time_epoch: int = 0
    last_updated_epoch: int = 0


class ApplicationSummary(BaseModel):
    model_config = ConfigDict(frozen=True, alias_generator=to_camel, populate_by_name=True)

    id: str
    name: str = ""
    attempts: list[Attempt] = []
    driver_host: str = ""

    @property
    def latest_attempt(self) -> Attempt | None:
        return self.attempts[-1] if self.attempts else None
