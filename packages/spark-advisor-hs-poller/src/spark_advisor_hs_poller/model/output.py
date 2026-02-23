from pydantic import BaseModel, ConfigDict


class Attempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    attemptId: str | None = None
    startTime: str = ""
    endTime: str = ""
    lastUpdated: str = ""
    duration: int = 0
    sparkUser: str = ""
    completed: bool = False
    appSparkVersion: str = ""
    logPath: str = ""
    startTimeEpoch: int = 0
    endTimeEpoch: int = 0
    lastUpdatedEpoch: int = 0


class ApplicationSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str = ""
    attempts: list[Attempt] = []
    driverHost: str = ""
