from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RecommendationInput(BaseModel):
    model_config = ConfigDict(frozen=True)
    priority: int = Field(description="Priority rank, 1 = highest impact.")
    title: str = Field(description="Short title for the recommendation.")
    parameter: str = Field(
        description="Spark config parameter (e.g. spark.sql.shuffle.partitions). "
        'Use "code_change" for non-config recommendations.'
    )
    current_value: str = Field(description="Current value of the parameter.")
    recommended_value: str = Field(description="Recommended new value.")
    explanation: str = Field(description="Brief explanation of why this change helps (2-3 sentences).")
    estimated_impact: str = Field(description='Quantified estimate, e.g. "~30% reduction in Stage 4 duration".')
    risk: str = Field(description="Potential downsides of this change.")


class AnalysisToolInput(BaseModel):
    model_config = ConfigDict(frozen=True)
    summary: str = Field(description="1-2 sentence overview of the job's health.")
    severity: Literal["critical", "warning", "info"] = Field(description="Overall severity of findings.")
    recommendations: list[RecommendationInput] = Field(description="Prioritized list of recommendations (max 7).")
    causal_chain: str = Field(description="Causal chain between related problems, if any. Empty string if none.")


class FetchJobRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    app_id: str


class ListAppsRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    limit: int = Field(default=20, ge=1, le=500)

class StorageFetchRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    app_id: str
    event_log_uri: str
    spark_conf: dict[str, str] = Field(default_factory=dict)


class K8sFetchRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    namespace: str | None = None
    name: str | None = None
    app_id: str | None = None

    @model_validator(mode="after")
    def validate_identifier(self) -> Self:
        has_k8s_id = self.namespace is not None and self.name is not None
        has_app_id = self.app_id is not None
        if not has_k8s_id and not has_app_id:
            raise ValueError("Either (namespace + name) or app_id must be provided")
        return self


class ListK8sAppsRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    limit: int = Field(default=20, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    namespace: str | None = None
    state: str | None = None
    search: str | None = None
