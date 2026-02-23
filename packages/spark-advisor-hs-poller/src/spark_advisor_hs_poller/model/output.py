from pydantic import BaseModel, ConfigDict


class Attempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    attemptId: str
    startTime: str
    endTime: str
    lastUpdated: str
    duration: int
    sparkUser: str
    completed: bool
    appSparkVersion: str
    logPath: str
    startTimeEpoch: int
    endTimeEpoch: int
    lastUpdatedEpoch: int


class ApplicationSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    attempts: list[Attempt]
    driverHost: str
