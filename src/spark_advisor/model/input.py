from typing import Literal

from pydantic import BaseModel, Field


class RecommendationInput(BaseModel):
    priority: int = Field(description="Priority rank, 1 = highest impact.")
    title: str = Field(description="Short title for the recommendation.")
    parameter: str = Field(
        description='Spark config parameter (e.g. spark.sql.shuffle.partitions). '
                    'Use "code_change" for non-config recommendations.'
    )
    current_value: str = Field(description="Current value of the parameter.")
    recommended_value: str = Field(description="Recommended new value.")
    explanation: str = Field(description="Brief explanation of why this change helps (2-3 sentences).")
    estimated_impact: str = Field(description='Quantified estimate, e.g. "~30% reduction in Stage 4 duration".')
    risk: str = Field(description="Potential downsides of this change.")


class AnalysisToolInput(BaseModel):
    summary: str = Field(description="1-2 sentence overview of the job's health.")
    severity: Literal["critical", "warning", "info"] = Field(description="Overall severity of findings.")
    recommendations: list[RecommendationInput] = Field(description="Prioritized list of recommendations (max 7).")
    causal_chain: str = Field(description="Causal chain between related problems, if any. Empty string if none.")
